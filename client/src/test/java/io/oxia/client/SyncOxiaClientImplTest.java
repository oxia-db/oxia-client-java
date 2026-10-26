/*
 * Copyright © 2026 The Oxia Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.oxia.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class SyncOxiaClientImplTest {
    private final AsyncOxiaClient asyncClient = mock(AsyncOxiaClient.class);
    private final SyncOxiaClientImpl client = new SyncOxiaClientImpl(asyncClient);

    @Test
    void throwsDeclaredExceptionsAsIs() {
        var exists = new KeyAlreadyExistsException("k");
        var badVersion = new UnexpectedVersionIdException("k", 1);
        when(asyncClient.put(any(), any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(exists));
        when(asyncClient.delete(any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(badVersion));

        assertThatThrownBy(() -> client.put("k", new byte[0], Set.of())).isSameAs(exists);
        assertThatThrownBy(() -> client.delete("k", Set.of())).isSameAs(badVersion);
    }

    @Test
    void wrapsUndeclaredExceptions() {
        var noSession = new SessionDoesNotExistException();
        var invalid = new InvalidArgumentException("invalid");
        when(asyncClient.get(any(), anySet())).thenReturn(CompletableFuture.failedFuture(invalid));
        when(asyncClient.put(any(), any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(noSession));

        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(invalid);
        // put declares only the outcomes of a conditional put
        assertThatThrownBy(() -> client.put("k", new byte[0], Set.of()))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(noSession);
    }

    @Test
    void throwsUncheckedExceptionsAsIs() {
        var error = new IllegalStateException("closed");
        when(asyncClient.get(any(), anySet())).thenReturn(CompletableFuture.failedFuture(error));

        assertThatThrownBy(() -> client.get("k")).isSameAs(error);
    }

    @Test
    void wrapsOtherCheckedExceptions() {
        var timeout = new TimeoutException();
        when(asyncClient.get(any(), anySet())).thenReturn(CompletableFuture.failedFuture(timeout));

        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(timeout);
    }

    @Test
    void wrapsAnInterruption() {
        when(asyncClient.get(any(), anySet())).thenReturn(new CompletableFuture<>());

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCauseInstanceOf(InterruptedException.class);
        // The interrupt flag is kept
        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    void delegatesToTheAsyncClient() throws Exception {
        when(asyncClient.delete(any(), anySet())).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncClient.deleteRange(any(), any(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(asyncClient.list(any(), any(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of("a")));
        var updates = mock(Closeable.class);
        Consumer<String> listener = k -> {};
        when(asyncClient.getSequenceUpdates("k", listener, Set.of())).thenReturn(updates);
        Consumer<Notification> callback = n -> {};

        assertThat(client.delete("k")).isTrue();
        client.deleteRange("a", "b");
        assertThat(client.list("a", "b")).containsExactly("a");
        client.notifications(callback);
        assertThat(client.getSequenceUpdates("k", listener, Set.of())).isSameAs(updates);
        client.close();

        verify(asyncClient).deleteRange("a", "b", Set.of());
        verify(asyncClient).notifications(callback);
        verify(asyncClient).close();
    }

    @Test
    void wrapsUndeclaredExceptionsOfEveryOperation() {
        var noSession = new SessionDoesNotExistException();
        var failed = CompletableFuture.failedFuture(noSession);
        when(asyncClient.put(any(), any(), anySet())).thenAnswer(i -> failed);
        when(asyncClient.delete(any(), anySet())).thenAnswer(i -> failed);
        when(asyncClient.deleteRange(any(), any(), anySet())).thenAnswer(i -> failed);
        when(asyncClient.list(any(), any(), anySet())).thenAnswer(i -> failed);

        List<ThrowingCallable> calls =
                List.of(
                        () -> client.put("k", new byte[0]),
                        () -> client.delete("k"),
                        () -> client.delete("k", Set.of()),
                        () -> client.deleteRange("a", "b"),
                        () -> client.list("a", "b"));
        for (var call : calls) {
            assertThatThrownBy(call).isInstanceOf(OxiaUncheckedException.class).hasCause(noSession);
        }
    }

    @Test
    void rangeScanIterableClosesItsIterators() {
        var iterable = client.rangeScan("a", "b");
        var first = iterable.iterator();
        var second = iterable.iterator();
        verify(asyncClient).rangeScan("a", "b", (RangeScanConsumer) first, Set.of());
        verify(asyncClient).rangeScan("a", "b", (RangeScanConsumer) second, Set.of());

        iterable.close();
        iterable.close();

        assertThat(first.hasNext()).isFalse();
        assertThat(second.hasNext()).isFalse();
        assertThatThrownBy(iterable::iterator).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @SuppressWarnings("resource")
    void rejectsNullArguments() {
        var value = new byte[0];
        List<ThrowingCallable> calls =
                List.of(
                        () -> client.put(null, value),
                        () -> client.put("k", null),
                        () -> client.put(null, value, Set.of()),
                        () -> client.put("k", null, Set.of()),
                        () -> client.put("k", value, null),
                        () -> client.delete(null),
                        () -> client.delete(null, Set.of()),
                        () -> client.delete("k", null),
                        () -> client.deleteRange(null, "b"),
                        () -> client.deleteRange("a", null),
                        () -> client.deleteRange(null, "b", Set.of()),
                        () -> client.deleteRange("a", null, Set.of()),
                        () -> client.deleteRange("a", "b", null),
                        () -> client.get(null),
                        () -> client.get(null, Set.of()),
                        () -> client.get("k", null),
                        () -> client.list(null, "b"),
                        () -> client.list("a", null),
                        () -> client.list(null, "b", Set.of()),
                        () -> client.list("a", null, Set.of()),
                        () -> client.list("a", "b", null),
                        () -> client.notifications(null),
                        () -> client.getSequenceUpdates(null, k -> {}, Set.of()),
                        () -> client.getSequenceUpdates("k", null, Set.of()),
                        () -> client.getSequenceUpdates("k", k -> {}, null),
                        () -> client.rangeScan(null, "b"),
                        () -> client.rangeScan("a", null),
                        () -> client.rangeScan(null, "b", Set.of()),
                        () -> client.rangeScan("a", null, Set.of()));
        for (var call : calls) {
            assertThatThrownBy(call).isInstanceOf(NullPointerException.class);
        }
    }
}
