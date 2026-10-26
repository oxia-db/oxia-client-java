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
import static org.awaitility.Awaitility.await;

import io.grpc.ServerBuilder;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.services.HealthStatusManager;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class ConnectionManagerTest {

    @Test
    void removesUnhealthyConnection() throws Exception {
        var health = new HealthStatusManager();
        health.setStatus("", ServingStatus.SERVING);
        var server =
                ServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(health.getHealthService())
                        .build()
                        .start();
        var address = "127.0.0.1:" + server.getPort();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(address)
                                        .maxConnectionPerNode(1)
                                        .connectionKeepAliveTime(Duration.ofMillis(10)))
                        .getClientConfig();
        var executor = Executors.newSingleThreadScheduledExecutor();

        try (var connections = new ConnectionManager(config, executor)) {
            var connection = connections.getConnection(address);
            assertThat(connections.getConnection(address)).isSameAs(connection);
            assertThat(connections.getConnectionCount()).isEqualTo(1);

            health.setStatus("", ServingStatus.NOT_SERVING);

            await().untilAsserted(() -> assertThat(connections.getConnectionCount()).isZero());
            health.setStatus("", ServingStatus.SERVING);
            assertThat(connections.getConnection(address)).isNotSameAs(connection);
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }
}
