/*
 * Copyright © 2022-2025 The Oxia Authors
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
package io.oxia.client.session;

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.SessionHeartbeat;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
                        DefaultNamespace,
                        null,
                        false,
                        Duration.ofMillis(100),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(5),
                        1);

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
    public void nonCallbackListener() {
        final OxiaStubProvider mockProvider = mock(OxiaStubProvider.class);
        final SessionNotificationListener listener = spy(SessionNotificationListener.class);
        when(mockProvider.getStubForShard(anyLong()))
                .thenThrow(new IllegalStateException("wrong states"));
        var session =
                new Session(
                        executor, mockProvider, config, shardId, sessionId, InstrumentProvider.NOOP, listener);
        try {
            session.close().join();
            fail("unexpected behaviour");
        } catch (CompletionException ex) {
            Assertions.assertInstanceOf(IllegalStateException.class, ex.getCause());
        }
        verify(listener, times(1)).onSessionClosed(any());
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
                                                    .setShard(shardId)
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
            responseObserver.onCompleted();
        }

        @Override
        public void closeSession(
                CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
            closed.compareAndSet(false, true);
            responseObserver.onNext(CloseSessionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    }
}
