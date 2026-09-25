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

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    void heartbeatRejectedByStaleLeaderIsRetriedOnNewLeader() throws Exception {
        var leaderHeartbeats = new AtomicInteger();
        var leaderFirstHeartbeatNanos = new AtomicLong();
        Server leaderServer =
                ServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(
                                new OxiaClientGrpc.OxiaClientImplBase() {
                                    @Override
                                    public void keepAlive(
                                            SessionHeartbeat heartbeat,
                                            StreamObserver<KeepAliveResponse> responseObserver) {
                                        leaderFirstHeartbeatNanos.compareAndSet(0, System.nanoTime());
                                        leaderHeartbeats.incrementAndGet();
                                        responseObserver.onNext(new KeepAliveResponse());
                                        responseObserver.onCompleted();
                                    }

                                    @Override
                                    public void closeSession(
                                            CloseSessionRequest request,
                                            StreamObserver<CloseSessionResponse> responseObserver) {
                                        responseObserver.onNext(new CloseSessionResponse());
                                        responseObserver.onCompleted();
                                    }
                                })
                        .build()
                        .start();
        var leaderAddress = "localhost:" + leaderServer.getPort();
        var shardLeader = new AtomicReference<String>();
        var staleLeaderHeartbeats = new AtomicInteger();
        var staleLeaderHeartbeatNanos = new AtomicLong();
        Server staleLeaderServer =
                ServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(
                                new OxiaClientGrpc.OxiaClientImplBase() {
                                    @Override
                                    public void keepAlive(
                                            SessionHeartbeat heartbeat,
                                            StreamObserver<KeepAliveResponse> responseObserver) {
                                        staleLeaderHeartbeatNanos.set(System.nanoTime());
                                        staleLeaderHeartbeats.incrementAndGet();
                                        // The shard map moves to the new leader while the heartbeat is in flight
                                        shardLeader.set(leaderAddress);
                                        responseObserver.onError(nodeIsNotLeaderWithoutLeaderHint());
                                    }
                                })
                        .build()
                        .start();
        shardLeader.set("localhost:" + staleLeaderServer.getPort());
        // Default session timeout (15s): one heartbeat every 2s
        var clientConfig =
                ((OxiaClientBuilderImpl) OxiaClientBuilder.create("localhost:0")).getClientConfig();
        var listener = mock(SessionNotificationListener.class);

        try (var provider = RpcProvider.create(clientConfig, executor, shard -> shardLeader.get())) {
            var session =
                    new Session(
                            executor,
                            provider,
                            clientConfig,
                            shardId,
                            sessionId,
                            InstrumentProvider.NOOP,
                            listener);

            await().atMost(Duration.ofSeconds(10)).until(() -> leaderHeartbeats.get() >= 2);

            assertThat(staleLeaderHeartbeats).hasValue(1);
            // The rejected heartbeat itself was retried on the new leader, before the next one was due
            assertThat(
                            Duration.ofNanos(leaderFirstHeartbeatNanos.get() - staleLeaderHeartbeatNanos.get()))
                    .isLessThan(Duration.ofSeconds(1));
            verifyNoInteractions(listener);
            session.close().get(5, TimeUnit.SECONDS);
        } finally {
            staleLeaderServer.shutdownNow();
            leaderServer.shutdownNow();
        }
    }

    // What a node answers when it is not the shard leader and does not know the current one
    private static Throwable nodeIsNotLeaderWithoutLeaderHint() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value())
                        .setMessage("oxia: node is not leader")
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("oxia.io")
                                                .setReason("NODE_IS_NOT_LEADER")
                                                .build()))
                        .build();
        return StatusProto.toStatusRuntimeException(grpcStatus);
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

            responseObserver.onNext(new KeepAliveResponse());
            responseObserver.onCompleted();
        }

        @Override
        public void closeSession(
                CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
            closed.compareAndSet(false, true);
            responseObserver.onNext(new CloseSessionResponse());
            responseObserver.onCompleted();
        }
    }
}
