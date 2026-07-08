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
package io.oxia.client.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.oxia.client.OxiaClientBuilderImpl;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;

class RpcProviderSharedConnectionTest {

    /**
     * A provider that borrows an externally-owned {@link ConnectionManager} (as the shared-resources
     * pool does) must not close it when the provider itself is closed — only its owner may.
     */
    @Test
    void doesNotCloseExternallyOwnedConnectionManager() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            var config = new OxiaClientBuilderImpl("localhost:6648").getClientConfig();
            ConnectionManager sharedConnections = mock(ConnectionManager.class);

            RpcProvider provider =
                    RpcProvider.create(config, executor, sharedConnections, shardId -> "localhost:6648");
            provider.close();

            verify(sharedConnections, never()).close();
        } finally {
            executor.shutdownNow();
        }
    }
}
