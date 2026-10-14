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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class SyncOxiaClientImplTest {
    private final AsyncOxiaClient asyncClient = mock(AsyncOxiaClient.class);
    private final SyncOxiaClientImpl client = new SyncOxiaClientImpl(asyncClient);

    @Test
    void throwsDeclaredExceptionsDirectly() {
        when(asyncClient.put(any(), any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(new KeyAlreadyExistsException("k")));
        when(asyncClient.delete(any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(new UnexpectedVersionIdException("k", 1)));

        assertThatThrownBy(() -> client.put("k", new byte[0], Set.of()))
                .isInstanceOf(KeyAlreadyExistsException.class);
        assertThatThrownBy(() -> client.delete("k", Set.of()))
                .isInstanceOf(UnexpectedVersionIdException.class);
    }

    @Test
    void wrapsUndeclaredOxiaExceptionsInCompletionException() {
        var unavailable = new ResourceUnavailableException("unavailable", null);
        when(asyncClient.get(any(), anySet())).thenReturn(CompletableFuture.failedFuture(unavailable));
        when(asyncClient.delete(any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(new KeyAlreadyExistsException("k")));

        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(CompletionException.class)
                .hasCause(unavailable);
        // delete declares only UnexpectedVersionIdException
        assertThatThrownBy(() -> client.delete("k", Set.of()))
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(KeyAlreadyExistsException.class);
    }

    @Test
    void throwsRuntimeExceptionsDirectly() {
        var closed = new IllegalStateException("Client has been closed");
        when(asyncClient.list(any(), any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(closed));

        assertThatThrownBy(() -> client.list("a", "b")).isSameAs(closed);
    }
}
