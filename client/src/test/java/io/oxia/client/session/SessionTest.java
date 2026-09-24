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
import static org.mockito.Mockito.*;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.ManagedObservers;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.SessionHeartbeat;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionTest {

    RpcProvider rpcProvider;
    ClientConfig config;
    long shardId = 1L;
    long sessionId = 2L;
    Duration sessionTimeout = Duration.ofSeconds(10);
    String clientId = "client";

    private Server server;
    private ManagedChannel channel;
    private TestService service;
    private ScheduledExecutorService executor;

    @BeforeEach
    void setup() throws IOException {
        executor = Executors.newSingleThreadScheduledExecutor();

        config =
                new ClientConfig(
                        "address",
                        Duration.ZERO,
                        1,
                        1024 * 1024,
                        256L * 1024 * 1024,
                        4,
                        4,
                        1,
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
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        var async = OxiaClientGrpc.newStub(channel);

        rpcProvider = mock(RpcProvider.class);
        lenient()
                .when(rpcProvider.keepAlive(any(SessionHeartbeat.class), any(Duration.class)))
                .thenAnswer(
                        invocation -> {
                            var future = new CompletableFuture<KeepAliveResponse>();
                            async.keepAlive(
                                    invocation.getArgument(0), ManagedObservers.toCompletableFuture(future));
                            return future;
                        });
        lenient()
                .when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenAnswer(
                        invocation -> {
                            var future = new CompletableFuture<CloseSessionResponse>();
                            async.closeSession(
                                    invocation.getArgument(0),
                                    new StreamObserver<>() {
                                        CloseSessionResponse response;

                                        @Override
                                        public void onNext(CloseSessionResponse response) {
                                            this.response = response;
                                        }

                                        @Override
                                        public void onError(Throwable throwable) {
                                            future.completeExceptionally(throwable);
                                        }

                                        @Override
                                        public void onCompleted() {
                                            future.complete(response);
                                        }
                                    });
                            return future;
                        });
    }

    @AfterEach
    public void stopServer() throws Exception {
        server.shutdown();
        channel.shutdownNow();

        server = null;
        channel = null;
        executor.shutdownNow();
    }

    @Test
    void sessionId() {
        var session =
                new Session(
                        executor,
                        rpcProvider,
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
        final RpcProvider mockProvider = mock(RpcProvider.class);
        final SessionNotificationListener listener = spy(SessionNotificationListener.class);
        when(mockProvider.closeSession(any(CloseSessionRequest.class)))
                .thenThrow(new IllegalStateException("wrong states"));
        var session =
                new Session(
                        executor, mockProvider, config, shardId, sessionId, InstrumentProvider.NOOP, listener);
        try {
            Assertions.assertDoesNotThrow(session::close).join();
            fail("unexpected behaviour");
        } catch (CompletionException ex) {
            Assertions.assertInstanceOf(IllegalStateException.class, ex.getCause());
        }
        verifyNoInteractions(listener);
    }

    @Test
    void start() throws Exception {
        var session =
                new Session(
                        executor,
                        rpcProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));

        var expectedHeartbeat = new SessionHeartbeat();
        expectedHeartbeat.setSessionId(sessionId).setShard(shardId);
        var expectedBytes = expectedHeartbeat.toByteArray();
        await()
                .untilAsserted(
                        () -> {
                            assertThat(service.signals.size()).isGreaterThan(2);
                            assertThat(service.signals)
                                    .allSatisfy(hb -> assertThat(hb.toByteArray()).isEqualTo(expectedBytes));
                        });
        session.close().join();
        assertThat(service.closed).isTrue();
        assertThat(service.signalsAfterClosed).isEmpty();
    }

    @Test
    void closeToleratesSessionNotFound() {
        service.failCloseWithSessionNotFound.set(true);
        var session =
                new Session(
                        executor,
                        rpcProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));

        // A session the server no longer knows about is already closed server-side: the
        // goal of the close is achieved, so this succeeds (as in the Go client).
        Assertions.assertDoesNotThrow(() -> session.close().join());
        assertThat(session.isClosed()).isTrue();
    }

    @Test
    void expireThenCloseSendsNoCloseRequest() {
        var session =
                new Session(
                        executor,
                        rpcProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));

        // Local expiry abandons the session: no close RPC, not even on a later close().
        session.expire();
        session.close().join();

        assertThat(session.isClosed()).isTrue();
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void expireStopsHeartbeats() throws Exception {
        var session =
                new Session(
                        executor,
                        rpcProvider,
                        config,
                        shardId,
                        sessionId,
                        InstrumentProvider.NOOP,
                        mock(SessionNotificationListener.class));

        // The heartbeat interval floor is 2s: two signals place us past the first tick.
        await().atMost(Duration.ofSeconds(8)).until(() -> service.signals.size() >= 2);
        session.expire();
        var signalsAtExpiry = service.signals.size();

        // No further keep-alive is scheduled after the expiry verdict.
        Thread.sleep(3000);
        assertThat(service.signals).hasSize(signalsAtExpiry);
        assertThat(service.closed).isFalse();
    }

>>>>>>> efc7e8d (tests: cover expiry without close, bounded retries and close tolerance)
    static class TestService extends OxiaClientGrpc.OxiaClientImplBase {
        BlockingQueue<SessionHeartbeat> signals = new LinkedBlockingQueue<>();
        BlockingQueue<SessionHeartbeat> signalsAfterClosed = new LinkedBlockingQueue<>();
        AtomicBoolean closed = new AtomicBoolean(false);
        AtomicBoolean failCloseWithSessionNotFound = new AtomicBoolean(false);

        @Override
        public void keepAlive(
                SessionHeartbeat heartbeat, StreamObserver<KeepAliveResponse> responseObserver) {
            if (!closed.get()) {
                signals.add(heartbeat);
            } else {
                signalsAfterClosed.add(heartbeat);
            }

            responseObserver.onNext(new KeepAliveResponse());
            responseObserver.onCompleted();
        }

        @Override
        public void closeSession(
                CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
            if (failCloseWithSessionNotFound.get()) {
                var status =
                        com.google.rpc.Status.newBuilder()
                                .setCode(io.grpc.Status.Code.NOT_FOUND.value())
                                .setMessage("oxia: session not found")
                                .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
                return;
            }
            closed.compareAndSet(false, true);
            responseObserver.onNext(new CloseSessionResponse());
            responseObserver.onCompleted();
        }
    }
}
