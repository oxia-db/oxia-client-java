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

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class OxiaStubHealthTest {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void removesConnectionAfterHealthTimeout() throws Exception {
        var health = new HealthStatusManager();
        health.setStatus("", ServingStatus.NOT_SERVING);

        var failed = new AtomicBoolean();
        try (var target =
                newTarget(stub -> failed.set(true), Duration.ofMillis(10), health.getHealthService())) {

            await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(failed).isTrue());
        }
    }

    @Test
    void removesConnectionWhenHealthPingHangs() throws Exception {
        var failed = new AtomicBoolean();
        try (var target =
                newTarget(stub -> failed.set(true), Duration.ofMillis(10), new BlockingHealthService())) {

            await().atMost(Duration.ofSeconds(1)).untilAsserted(() -> assertThat(failed).isTrue());
        }
    }

    @Test
    void keepsConnectionWhenHealthIsServing() throws Exception {
        var health = new HealthStatusManager();
        health.setStatus("", ServingStatus.SERVING);

        var failed = new AtomicBoolean();
        try (var target =
                newTarget(stub -> failed.set(true), Duration.ofSeconds(1), health.getHealthService())) {

            await().during(Duration.ofMillis(250)).untilAsserted(() -> assertThat(failed).isFalse());
        }
    }

    @Test
    void keepsConnectionWhenHealthCheckIsUnsupported() throws Exception {
        var failed = new AtomicBoolean();
        try (var target =
                newTarget(
                        stub -> failed.set(true), Duration.ofSeconds(1), new UnimplementedHealthService())) {

            await().during(Duration.ofMillis(100)).untilAsserted(() -> assertThat(failed).isFalse());
        }
    }

    private Target newTarget(
            HealthCheckFailureCallback healthCheckFailureCallback,
            Duration healthPingTimeout,
            BindableService... services)
            throws Exception {
        var serverBuilder = ServerBuilder.forPort(0).directExecutor();
        for (BindableService service : services) {
            serverBuilder.addService(service);
        }
        var server = serverBuilder.build().start();
        var address = "127.0.0.1:" + server.getPort();
        OxiaClientBuilder builder =
                OxiaClientBuilder.create(address)
                        .connectionKeepAliveTime(Duration.ofMillis(10))
                        .connectionKeepAliveTimeout(healthPingTimeout);
        var clientConfig = ((OxiaClientBuilderImpl) builder).getClientConfig();
        return new Target(
                server, new OxiaStub(address, clientConfig, executor, healthCheckFailureCallback));
    }

    private record Target(Server server, OxiaStub stub) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            stub.close();
            server.shutdownNow();
        }
    }

    private static class BlockingHealthService extends HealthGrpc.HealthImplBase {
        @Override
        public void check(
                HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {}
    }

    private static class UnimplementedHealthService extends HealthGrpc.HealthImplBase {
        @Override
        public void check(
                HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
            responseObserver.onError(Status.UNIMPLEMENTED.asRuntimeException());
        }
    }
}
