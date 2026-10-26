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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.oxia.client.OxiaClientBuilderImpl;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.LongFunction;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class RpcProviderTest {
    private final LongFunction<String> leaders = shardId -> "localhost:6648";

    @Test
    void createsGrpcProvider() throws Exception {
        var executor = Executors.newSingleThreadScheduledExecutor();
        try (var provider =
                RpcProvider.create(
                        new OxiaClientBuilderImpl("localhost:6648").getClientConfig(), executor, leaders)) {
            assertThat(provider).isInstanceOf(GrpcRpcProvider.class);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsNullArguments() {
        var config = new OxiaClientBuilderImpl("localhost:6648").getClientConfig();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var connections = mock(ConnectionManager.class);
        try {
            List<ThrowingCallable> calls =
                    List.of(
                            () -> RpcProvider.create(null, executor, leaders),
                            () -> RpcProvider.create(config, null, leaders),
                            () -> RpcProvider.create(config, executor, null),
                            () -> RpcProvider.create(null, executor, connections, leaders),
                            () -> RpcProvider.create(config, null, connections, leaders),
                            () -> RpcProvider.create(config, executor, null, leaders),
                            () -> RpcProvider.create(config, executor, connections, null));

            calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
        } finally {
            executor.shutdownNow();
        }
    }
}
