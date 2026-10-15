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
import static org.mockito.Mockito.when;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaClientException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
        var unavailable = new ResourceUnavailableException("unavailable", null);
        when(asyncClient.get(any(), anySet())).thenReturn(CompletableFuture.failedFuture(unavailable));
        when(asyncClient.put(any(), any(), anySet()))
                .thenReturn(CompletableFuture.failedFuture(unavailable));

        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(unavailable);
        // put declares only the outcomes of a conditional put
        assertThatThrownBy(() -> client.put("k", new byte[0], Set.of()))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(unavailable);
    }

    @Test
    void wrapsAnInterruption() {
        when(asyncClient.get(any(), anySet())).thenReturn(new CompletableFuture<>());

        Thread.currentThread().interrupt();
        assertThatThrownBy(() -> client.get("k"))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCauseInstanceOf(OxiaClientException.class);
        // The interrupt flag is kept
        assertThat(Thread.interrupted()).isTrue();
    }
}
