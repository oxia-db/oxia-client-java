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

import io.oxia.client.api.SharedResources;
import org.junit.jupiter.api.Test;

class SharedResourcesImplTest {

    @Test
    void builderValidatesArguments() {
        var builder = SharedResourcesImpl.builder();
        assertThatThrownBy(() -> builder.numWorkerThreads(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchingThreads(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxConnectionPerNode(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void startsWithNoOpenConnections() {
        try (var shared = (SharedResourcesImpl) SharedResourcesImpl.builder().build()) {
            assertThat(shared.getConnectionCount()).isZero();
            assertThat(shared.namespaceCount()).isZero();
        }
    }

    @Test
    void deduplicatesShardManagersByAddressAndNamespace() {
        try (var shared =
                (SharedResourcesImpl) SharedResourcesImpl.builder().numWorkerThreads(1).build()) {
            // Unroutable addresses: the shard-assignment stream never connects, but namespace
            // registration is synchronous, so dedup/keying can be asserted without a real server.
            shared.getOrCreateShardManager(config("localhost:1", "ns-a"));
            assertThat(shared.namespaceCount()).isEqualTo(1);

            // Same (address, namespace) -> reused, no new entry.
            shared.getOrCreateShardManager(config("localhost:1", "ns-a"));
            assertThat(shared.namespaceCount()).isEqualTo(1);

            // Different namespace -> new entry.
            shared.getOrCreateShardManager(config("localhost:1", "ns-b"));
            assertThat(shared.namespaceCount()).isEqualTo(2);

            // Different address -> new entry.
            shared.getOrCreateShardManager(config("localhost:2", "ns-a"));
            assertThat(shared.namespaceCount()).isEqualTo(3);
        }
    }

    @Test
    void closeShutsDownExecutorAndIsIdempotent() {
        var shared = (SharedResourcesImpl) SharedResourcesImpl.builder().numWorkerThreads(1).build();
        assertThat(shared.executor().isShutdown()).isFalse();

        shared.close();
        assertThat(shared.executor().isShutdown()).isTrue();

        // Closing again is a no-op.
        shared.close();
        assertThat(shared.executor().isShutdown()).isTrue();
    }

    @Test
    void getOrCreateShardManagerFailsAfterClose() {
        var shared = (SharedResourcesImpl) SharedResourcesImpl.builder().build();
        shared.close();

        var future = shared.getOrCreateShardManager(config("localhost:1", "ns"));
        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThat(shared.namespaceCount()).isZero();
    }

    @Test
    void rejectsClientTransportSettingsWhenSharedResourcesAttached() {
        try (SharedResources shared = SharedResourcesImpl.builder().build()) {
            assertThatThrownBy(
                            () ->
                                    new OxiaClientBuilderImpl("localhost:6648")
                                            .enableTls(true)
                                            .sharedResources(shared)
                                            .asyncClient())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("enableTls");

            assertThatThrownBy(
                            () ->
                                    new OxiaClientBuilderImpl("localhost:6648")
                                            .maxConnectionPerNode(4)
                                            .sharedResources(shared)
                                            .asyncClient())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxConnectionPerNode");

            // Batching threads are governed by the pool too.
            assertThatThrownBy(
                            () ->
                                    new OxiaClientBuilderImpl("localhost:6648")
                                            .batchingThreads(4)
                                            .sharedResources(shared)
                                            .asyncClient())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchingThreads");
        }
    }

    private static ClientConfig config(String serviceAddress, String namespace) {
        var builder = new OxiaClientBuilderImpl(serviceAddress);
        builder.namespace(namespace);
        return builder.getClientConfig();
    }
}
