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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
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
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.SessionHeartbeat;
import io.oxia.proto.ShardAssignments;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock RpcProvider rpcProvider;
    ShardManager shardManager;
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
        shardManager =
                new ShardManager(executor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);
        manager =
                new SessionManager(executor, config, rpcProvider, shardManager, InstrumentProvider.NOOP);
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

        var session = manager.getSession(() -> shardId).join();

        assertThat(manager.getSession(() -> shardId).join()).isSameAs(session);
        assertThat(session.getSessionId()).isEqualTo(10L);
        verify(rpcProvider).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void existingSession() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));

        var session1 = manager.getSession(() -> shardId);
        verify(rpcProvider, times(1)).createSession(any(CreateSessionRequest.class));

        var session2 = manager.getSession(() -> shardId);
        assertThat(session2).isSameAs(session1);
        verifyNoMoreInteractions(rpcProvider);
    }

    @Test
    void existingSessionWithFailure() {
        var shardId = 1L;
        // first failed
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")));
        var session1 = manager.getSession(() -> shardId);
        assertThat(session1).isCompletedExceptionally();
        verify(rpcProvider, times(1)).createSession(any(CreateSessionRequest.class));

        // second should be success
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));
        var session2 = manager.getSession(() -> shardId);
        assertThat(session2.join().getSessionId()).isEqualTo(10L);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));

        // third should be cache
        var session3 = manager.getSession(() -> shardId);
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
        manager.getSession(() -> shardId).join();

        manager.close();

        assertThat(manager.getSession(() -> shardId)).isCompletedExceptionally();
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
            var sessionManager =
                    new SessionManager(executor, config, provider, shardManager, InstrumentProvider.NOOP);
            assertThat(sessionManager.getSession(() -> 1L).get(5, TimeUnit.SECONDS).getSessionId())
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
            var sessionManager =
                    new SessionManager(executor, config, provider, shardManager, InstrumentProvider.NOOP);
            var session = sessionManager.getSession(() -> 1L);
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
        shardManager.addCallback(manager);
        shardManager.onNext(
                assignments(
                        List.of(
                                new Shard(shardId1, "leader1", new HashRange(0, 99)),
                                new Shard(shardId2, "leader2", new HashRange(100, 199)))));
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        var session1 = manager.getSession(() -> shardId1).join();
        var session2 = manager.getSession(() -> shardId2).join();

        // Shard 1 is split into shards 3 and 4, and the leader of shard 2 changes
        shardManager.onNext(
                assignments(
                        List.of(
                                new Shard(3L, "leader1", new HashRange(0, 49)),
                                new Shard(4L, "leader1", new HashRange(50, 99)),
                                new Shard(shardId2, "leader3", new HashRange(100, 199)))));

        // The session of shard 1 moved to shards 3 and 4, without being closed
        assertThat(manager.getSession(() -> 3L).join().getSessionId()).isEqualTo(10L);
        assertThat(manager.getSession(() -> 4L).join().getSessionId()).isEqualTo(10L);
        assertThat(session1.isClosed()).isTrue();
        // Session here shouldn't have changed after the reassignment
        assertThat(manager.getSession(() -> shardId2).join()).isSameAs(session2);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
    }

    static Stream<Arguments> splits() {
        // The splits of shard 0 and of its children, as {parent, left, right}, the shards that
        // replaced shard 0 in the end, and whether the session of the last of them is looked up
        // before the change of the shard map is notified
        return Stream.of(
                Arguments.of("notified", new long[][] {{0, 1, 2}}, new long[] {1, 2}, false),
                Arguments.of("lookup-first", new long[][] {{0, 1, 2}}, new long[] {1, 2}, true),
                Arguments.of(
                        "split-twice", new long[][] {{0, 1, 2}, {1, 3, 4}}, new long[] {2, 3, 4}, true));
    }

    // The shards that replace a split shard inherit its session, which owns the ephemeral records
    // they get from it. The client must keep the session alive on them, use it for their new
    // ephemeral records, and close it there.
    @ParameterizedTest(name = "{0}")
    @MethodSource("splits")
    void followsSplits(String name, long[][] splits, long[] shards, boolean lookupFirst)
            throws Exception {
        // The id of a session is 10 + the id of the shard it is created on
        var created = new CopyOnWriteArrayList<Long>();
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenAnswer(
                        invocation -> {
                            long shard = invocation.<CreateSessionRequest>getArgument(0).getShard();
                            created.add(shard);
                            return CompletableFuture.completedFuture(createSessionResponse(10 + shard));
                        });
        var heartbeats = new CopyOnWriteArrayList<ShardSession>();
        when(rpcProvider.keepAlive(any(SessionHeartbeat.class), any(Duration.class)))
                .thenAnswer(
                        invocation -> {
                            SessionHeartbeat heartbeat = invocation.getArgument(0);
                            heartbeats.add(new ShardSession(heartbeat.getShard(), heartbeat.getSessionId()));
                            return CompletableFuture.completedFuture(new KeepAliveResponse());
                        });
        var closed = new CopyOnWriteArrayList<ShardSession>();
        when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenAnswer(
                        invocation -> {
                            CloseSessionRequest request = invocation.getArgument(0);
                            closed.add(new ShardSession(request.getShard(), request.getSessionId()));
                            return CompletableFuture.completedFuture(new CloseSessionResponse());
                        });
        if (!lookupFirst) {
            shardManager.addCallback(manager);
        }

        var shardMap = new HashMap<Long, Shard>();
        shardMap.put(0L, new Shard(0, "leader", new HashRange(0, (1L << 32) - 1)));
        shardManager.onNext(assignments(shardMap.values()));
        long sessionId = sessionOn(0);

        for (var split : splits) {
            // At the midpoint, like the coordinator
            var parent = shardMap.remove(split[0]).hashRange();
            long splitPoint = parent.minInclusive() + (parent.maxInclusive() - parent.minInclusive()) / 2;
            shardMap.put(
                    split[1],
                    new Shard(split[1], "leader", new HashRange(parent.minInclusive(), splitPoint)));
            shardMap.put(
                    split[2],
                    new Shard(split[2], "leader", new HashRange(splitPoint + 1, parent.maxInclusive())));
            shardManager.onNext(assignments(shardMap.values()));
        }
        if (lookupFirst) {
            assertThat(sessionOn(shards[shards.length - 1])).isEqualTo(sessionId);
        }

        // The client keeps the session alive on the shards that replaced shard 0, and no longer on
        // shard 0...
        var inherited =
                Arrays.stream(shards).mapToObj(shard -> new ShardSession(shard, sessionId)).toList();
        long heartbeatsOnShard0 = heartbeats.stream().filter(h -> h.shard() == 0).count();
        await().untilAsserted(() -> assertThat(heartbeats).containsAll(inherited));
        assertThat(heartbeats.stream().filter(h -> h.shard() == 0).count())
                .isEqualTo(heartbeatsOnShard0);
        // ...uses it for their new ephemeral records...
        for (long shard : shards) {
            assertThat(sessionOn(shard)).isEqualTo(sessionId);
        }

        // ...and closes it on them
        manager.close();
        assertThat(created).containsExactly(0L);
        assertThat(closed).containsExactlyInAnyOrderElementsOf(inherited);
    }

    @Test
    void testSessionExpired() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        var session = manager.getSession(() -> shardId).join();

        // Both expiry funnels — the local timeout tick and an in-flight SESSION_NOT_FOUND
        // keep-alive response — end here. The expired session is abandoned without any
        // CloseSession RPC: a late close could destroy a new server-side session that
        // happens to reuse the same session id.
        manager.onSessionExpired(session);
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
        assertThat(manager.getSession(() -> shardId).join()).isNotSameAs(session);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void explicitCloseAfterExpirySendsNoRpc() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));

        var session = manager.getSession(() -> shardId).join();

        manager.onSessionExpired(session);
        session.close().join();

        assertThat(session.isClosed()).isTrue();
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void rejectedSessionIsReplacedOnNextGetSession() {
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        var rejected = manager.getSession(() -> 1L).join();
        manager.onSessionRejected(rejected);
        assertThat(rejected.isClosed()).isTrue();

        var replacement = manager.getSession(() -> 1L).join();
        assertThat(replacement.getSessionId()).isEqualTo(20L);

        // Other writes of the rejected session come back rejected after it has been replaced:
        // they must not touch the replacement
        manager.onSessionRejected(rejected);
        assertThat(manager.getSession(() -> 1L).join()).isSameAs(replacement);
        assertThat(replacement.isClosed()).isFalse();
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void lateRejectionDoesNotTouchNewerSessionWithSameId() {
        // A server that lost its state may give a new session the id of an old one
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(14L)),
                        CompletableFuture.completedFuture(createSessionResponse(14L)));

        var expired = manager.getSession(() -> 1L).join();
        manager.onSessionExpired(expired);
        var current = manager.getSession(() -> 1L).join();
        assertThat(current.getSessionId()).isEqualTo(expired.getSessionId());

        // A late rejection of a write of the expired session is about that session only
        manager.onSessionRejected(expired);

        assertThat(manager.getSession(() -> 1L).join()).isSameAs(current);
        assertThat(current.isClosed()).isFalse();
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void lateRejectionDoesNotTouchSessionUnderCreation() {
        var pending = new CompletableFuture<CreateSessionResponse>();
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)), pending);

        var rejected = manager.getSession(() -> 1L).join();
        manager.onSessionRejected(rejected);
        var creating = manager.getSession(() -> 1L);

        // A late rejection racing the creation of the replacement neither waits for it nor
        // disturbs it
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> manager.onSessionRejected(rejected));
        assertThat(creating).isNotDone();

        pending.complete(createSessionResponse(20L));
        assertThat(manager.getSession(() -> 1L).join()).isSameAs(creating.join());
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

    private long sessionOn(long shardId) {
        return manager.getSession(() -> shardId).join().getSessionId();
    }

    private static ShardAssignments assignments(Collection<Shard> shards) {
        var assignments = new ShardAssignments();
        var nsAssignments = assignments.putNamespaces(DefaultNamespace);
        for (var shard : shards) {
            nsAssignments
                    .addAssignment()
                    .setShard(shard.id())
                    .setLeader(shard.leader())
                    .setInt32HashRange()
                    .setMinHashInclusive((int) shard.hashRange().minInclusive())
                    .setMaxHashInclusive((int) shard.hashRange().maxInclusive());
        }
        return assignments;
    }

    private record ShardSession(long shard, long sessionId) {}
}
