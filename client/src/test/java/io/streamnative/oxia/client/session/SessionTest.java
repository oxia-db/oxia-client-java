/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client.session;

import static io.streamnative.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.grpc.OxiaStubProvider;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.proto.CloseSessionRequest;
import io.streamnative.oxia.proto.CloseSessionResponse;
import io.streamnative.oxia.proto.KeepAliveResponse;
import io.streamnative.oxia.proto.OxiaClientGrpc;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionTest {

    OxiaStubProvider stubProvider;
    ClientConfig config;
    long shardId = 1L;
    long sessionId = 2L;
    Duration sessionTimeout = Duration.ofSeconds(10);
    String clientId = "client";

    private Server server;
    private OxiaStub stub;
    private TestService service;
    private ScheduledExecutorService executor;

    @BeforeEach
    void setup() throws IOException {
        executor = Executors.newSingleThreadScheduledExecutor();

        config =
                new ClientConfig(
                        "address",
                        Duration.ZERO,
                        Duration.ZERO,
                        1,
                        1024 * 1024,
                        sessionTimeout,
                        clientId,
                        null,
                        DefaultNamespace);

        String serverName = InProcessServerBuilder.generateName();
        service = new TestService();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(service)
                        .build()
                        .start();
        stub = new OxiaStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());

        stubProvider = mock(OxiaStubProvider.class);
        lenient().when(stubProvider.getStubForShard(anyLong())).thenReturn(stub);
    }

    @AfterEach
    public void stopServer() throws Exception {
        server.shutdown();
        stub.close();

        server = null;
        stub = null;
        executor.shutdownNow();
    }

    @Test
    void sessionId() {
        var session =
                new Session(
                        executor,
                        stubProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));
        assertThat(session.getShardId()).isEqualTo(shardId);
        assertThat(session.getSessionId()).isEqualTo(sessionId);
    }

    @Test
    void start() throws Exception {
        var session =
                new Session(
                        executor,
                        stubProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));

        await()
                .untilAsserted(
                        () -> {
                            assertThat(service.signals.size()).isGreaterThan(2);
                            assertThat(service.signals)
                                    .containsOnly(
                                            SessionHeartbeat.newBuilder()
                                                    .setSessionId(sessionId)
                                                    .setShardId(shardId)
                                                    .build());
                        });
        session.close().join();
        assertThat(service.closed).isTrue();
        assertThat(service.signalsAfterClosed).isEmpty();
    }

    static class TestService extends OxiaClientGrpc.OxiaClientImplBase {
        BlockingQueue<SessionHeartbeat> signals = new LinkedBlockingQueue<>();
        BlockingQueue<SessionHeartbeat> signalsAfterClosed = new LinkedBlockingQueue<>();
        AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void keepAlive(
                SessionHeartbeat heartbeat, StreamObserver<KeepAliveResponse> responseObserver) {
            if (!closed.get()) {
                signals.add(heartbeat);
            } else {
                signalsAfterClosed.add(heartbeat);
            }

            responseObserver.onNext(KeepAliveResponse.getDefaultInstance());
        }

        @Override
        public void closeSession(
                CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
            closed.compareAndSet(false, true);
            responseObserver.onNext(CloseSessionResponse.getDefaultInstance());
        }
    }
}
