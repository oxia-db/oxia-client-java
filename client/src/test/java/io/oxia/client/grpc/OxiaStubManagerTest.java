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

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class OxiaStubManagerTest {

    @Test
    void removesStubWhenHealthCheckFails() throws Exception {
        Server server = newHealthServer(HealthCheckResponse.ServingStatus.NOT_SERVING);
        String target = target(server);

        try (OxiaStubManager manager =
                new OxiaStubManager(clientConfig(target, Duration.ofMillis(100)))) {
            OxiaStub stub = manager.getStub(target);

            await()
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertThat(manager.stubs).doesNotContainValue(stub));
        } finally {
            close(server);
        }
    }

    @Test
    void removesStubWhenHealthCheckHangs() throws Exception {
        Server server =
                NettyServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(new BlockingHealthService())
                        .build()
                        .start();
        String target = target(server);

        try (OxiaStubManager manager =
                new OxiaStubManager(clientConfig(target, Duration.ofMillis(100)))) {
            OxiaStub stub = manager.getStub(target);

            await()
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertThat(manager.stubs).doesNotContainValue(stub));
        } finally {
            close(server);
        }
    }

    @Test
    void keepsStubWhenHealthCheckSucceeds() throws Exception {
        Server server = newHealthServer(HealthCheckResponse.ServingStatus.SERVING);
        String target = target(server);

        try (OxiaStubManager manager =
                new OxiaStubManager(clientConfig(target, Duration.ofSeconds(1)))) {
            OxiaStub stub = manager.getStub(target);

            await()
                    .during(Duration.ofMillis(200))
                    .atMost(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(manager.stubs).containsValue(stub));
        } finally {
            close(server);
        }
    }

    @Test
    void keepsStubWhenHealthCheckIsUnsupported() throws Exception {
        Server server = NettyServerBuilder.forPort(0).directExecutor().build().start();
        String target = target(server);

        try (OxiaStubManager manager =
                new OxiaStubManager(clientConfig(target, Duration.ofSeconds(1)))) {
            OxiaStub stub = manager.getStub(target);

            await()
                    .during(Duration.ofMillis(200))
                    .atMost(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(manager.stubs).containsValue(stub));
        } finally {
            close(server);
        }
    }

    private static Server newHealthServer(HealthCheckResponse.ServingStatus status) throws Exception {
        return NettyServerBuilder.forPort(0)
                .directExecutor()
                .addService(new StaticHealthService(status))
                .build()
                .start();
    }

    private static ClientConfig clientConfig(String target, Duration healthCheckTimeout) {
        var builder = new OxiaClientBuilderImpl(target);
        builder.connectionKeepAliveTime(Duration.ofMillis(10));
        builder.connectionKeepAliveTimeout(healthCheckTimeout);
        return builder.getClientConfig();
    }

    private static String target(Server server) {
        return "127.0.0.1:" + server.getPort();
    }

    private static void close(Server server) throws InterruptedException {
        server.shutdownNow();
        server.awaitTermination(1, TimeUnit.SECONDS);
    }

    private static final class StaticHealthService extends HealthGrpc.HealthImplBase {
        private final HealthCheckResponse.ServingStatus status;

        private StaticHealthService(HealthCheckResponse.ServingStatus status) {
            this.status = status;
        }

        @Override
        public void check(
                HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onNext(HealthCheckResponse.newBuilder().setStatus(status).build());
            responseObserver.onCompleted();
        }
    }

    private static final class BlockingHealthService extends HealthGrpc.HealthImplBase {
        @Override
        public void check(
                HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {}
    }
}
