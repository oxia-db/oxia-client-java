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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock RpcProvider rpcProvider;
    SessionManager manager;
    ScheduledExecutorService executor;
    ClientConfig config;

    @BeforeEach
    void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        config =
                new ClientConfig(
                        "address",
                        Duration.ofSeconds(1),
                        1,
                        1024,
                        256L * 1024 * 1024,
                        4,
                        4,
                        1,
                        Duration.ofSeconds(10),
                        "client",
                        null,
                        DefaultNamespace,
                        null,
                        false,
                        Duration.ofMillis(10),
                        Duration.ofMillis(100),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(3),
                        1);
        manager = new SessionManager(executor, config, rpcProvider, InstrumentProvider.NOOP);
    }

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    @Test
    void newSession() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));

        var session = manager.getSession(shardId).join();

        assertThat(manager.getSession(shardId).join()).isSameAs(session);
        assertThat(session.getSessionId()).isEqualTo(10L);
        verify(rpcProvider).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void existingSession() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));

        var session1 = manager.getSession(shardId);
        verify(rpcProvider, times(1)).createSession(any(CreateSessionRequest.class));

        var session2 = manager.getSession(shardId);
        assertThat(session2).isSameAs(session1);
        verifyNoMoreInteractions(rpcProvider);
    }

    @Test
    void existingSessionWithFailure() {
        var shardId = 1L;
        // first failed
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")));
        var session1 = manager.getSession(shardId);
        assertThat(session1).isCompletedExceptionally();
        verify(rpcProvider, times(1)).createSession(any(CreateSessionRequest.class));

        // second should be success
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));
        var session2 = manager.getSession(shardId);
        assertThat(session2.join().getSessionId()).isEqualTo(10L);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));

        // third should be cache
        var session3 = manager.getSession(shardId);
        assertThat(session3).isSameAs(session2);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void close() throws Exception {
        var shardId = 5L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));
        when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CloseSessionResponse()));
        manager.getSession(shardId).join();

        manager.close();

        assertThat(manager.getSession(shardId)).isCompletedExceptionally();
        verify(rpcProvider, atLeastOnce()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void closeDoesNotBlockWhenLeaderIsUnreachable() throws Exception {
        var service =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void createSession(
                            CreateSessionRequest request,
                            StreamObserver<CreateSessionResponse> responseObserver) {
                        responseObserver.onNext(createSessionResponse(9L));
                        responseObserver.onCompleted();
                    }
                };
        Server server = ServerBuilder.forPort(0).directExecutor().addService(service).build().start();
        var leader = new AtomicReference<>("localhost:" + server.getPort());

        try (var provider = RpcProvider.create(config, executor, shardId -> leader.get())) {
            var sessionManager = new SessionManager(executor, config, provider, InstrumentProvider.NOOP);
            assertThat(sessionManager.getSession(1).get(5, TimeUnit.SECONDS).getSessionId())
                    .isEqualTo(9L);

            // CloseSession fails with a retryable error until the leader is reachable again
            leader.set(unreachableAddress());

            assertClosesWithinRequestTimeout(sessionManager);
        } finally {
            server.shutdownNow();
        }
    }

    @Test
    void closeDoesNotBlockOnPendingSessionCreation() throws Exception {
        var leader = unreachableAddress();
        var lookups = new AtomicInteger();

        try (var provider =
                RpcProvider.create(
                        config,
                        executor,
                        shardId -> {
                            lookups.incrementAndGet();
                            return leader;
                        })) {
            var sessionManager = new SessionManager(executor, config, provider, InstrumentProvider.NOOP);
            var session = sessionManager.getSession(1);
            assertThat(session).isNotDone();

            assertClosesWithinRequestTimeout(sessionManager);

            assertThat(session)
                    .failsWithin(Duration.ZERO)
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause()
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.TIMEOUT));

            // CreateSession looked up the leader on every attempt, and stopped retrying once it timed
            // out. Only an attempt that was starting as the timeout fired can still do its lookup.
            assertThat(lookups).hasValueGreaterThan(1);
            var lookupsAfterClose = lookups.get();
            await()
                    .during(Duration.ofMillis(500))
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(
                            () -> assertThat(lookups).hasValueLessThanOrEqualTo(lookupsAfterClose + 1));
        }
    }

    @Test
    void accept() throws Exception {
        var shardId1 = 1L;
        var shardId2 = 2L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)),
                        CompletableFuture.completedFuture(createSessionResponse(30L)));
        when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CloseSessionResponse()));

        var session1 = manager.getSession(shardId1).join();
        var session2 = manager.getSession(shardId2).join();

        manager.accept(
                new ShardAssignmentChanges(
                        Set.of(),
                        Set.of(new Shard(shardId1, "leader1", new HashRange(1, 2))),
                        Set.of(new Shard(shardId2, "leader3", new HashRange(3, 4)))));

        assertThat(manager.getSession(shardId1).join()).isNotSameAs(session1);
        // Session here shouldn't have changed after the reassignment
        assertThat(manager.getSession(shardId2).join()).isSameAs(session2);
        verify(rpcProvider, atLeastOnce()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void testSessionExpired() throws Exception {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));
        when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CloseSessionResponse()));

        var session = manager.getSession(shardId).join();

        manager.onSessionExpired(session);
        assertThat(manager.getSession(shardId).join()).isNotSameAs(session);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    private void assertClosesWithinRequestTimeout(SessionManager sessionManager) {
        // Close on a daemon thread, so that a close() that blocks forever can't hang the test JVM
        var closed = new CompletableFuture<Void>();
        var closer =
                new Thread(
                        () -> {
                            try {
                                sessionManager.close();
                                closed.complete(null);
                            } catch (Throwable t) {
                                closed.completeExceptionally(t);
                            }
                        });
        closer.setDaemon(true);
        closer.start();
        assertThat(closed).succeedsWithin(config.requestTimeout().plusSeconds(2));
    }

    private static String unreachableAddress() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return "localhost:" + socket.getLocalPort();
        }
    }

    private static CreateSessionResponse createSessionResponse(long sessionId) {
        return new CreateSessionResponse().setSessionId(sessionId);
    }
}
