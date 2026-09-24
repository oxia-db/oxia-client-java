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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.oxia.client.ClientConfig;
import io.oxia.client.api.SessionEvent;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
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
    void testSessionExpired() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        var session = manager.getSession(shardId).join();

        // Both expiry funnels — the local timeout tick and an in-flight SESSION_NOT_FOUND
        // keep-alive response — end here. The expired session is abandoned without any
        // CloseSession RPC: a late close could destroy a new server-side session that
        // happens to reuse the same session id.
        manager.onSessionExpired(session);
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
        assertThat(manager.getSession(shardId).join()).isNotSameAs(session);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void explicitCloseAfterExpirySendsNoRpc() {
        var shardId = 1L;
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(createSessionResponse(10L)));

        var session = manager.getSession(shardId).join();

        manager.onSessionExpired(session);
        session.close().join();

        assertThat(session.isClosed()).isTrue();
        verify(rpcProvider, never()).closeSession(any(CloseSessionRequest.class));
    }

    @Test
    void sessionListenerEstablishedAndExpiredEvents() {
        var events = new CopyOnWriteArrayList<SessionEvent>();
        var listenerManager = managerWithListener(events::add);
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        var session = listenerManager.getSession(1L).join();

        assertThat(events)
                .containsExactly(new SessionEvent(SessionEvent.Type.ESTABLISHED, 10L, "client", 1L));

        // A duplicate expiry trigger — e.g. the local timeout tick racing an in-flight
        // SESSION_NOT_FOUND response for the same session — must dispatch EXPIRED only once.
        listenerManager.onSessionExpired(session);
        listenerManager.onSessionExpired(session);

        assertThat(events)
                .hasSize(2)
                .element(1)
                .isEqualTo(new SessionEvent(SessionEvent.Type.EXPIRED, 10L, "client", 1L));

        // The expired session is re-created lazily, announcing the new session.
        listenerManager.getSession(1L).join();

        assertThat(events)
                .hasSize(3)
                .element(2)
                .isEqualTo(new SessionEvent(SessionEvent.Type.ESTABLISHED, 20L, "client", 1L));
    }

    @Test
    void sessionListenerExceptionDoesNotAffectSessions() {
        var listenerManager =
                managerWithListener(
                        event -> {
                            throw new IllegalStateException("listener failed");
                        });
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));

        // The ESTABLISHED dispatch must not poison the session future that the
        // ephemeral put triggering the session is waiting on.
        var session = listenerManager.getSession(1L).join();
        assertThat(session.getSessionId()).isEqualTo(10L);

        // An EXPIRED dispatch that throws must not break expiry handling, nor later sessions.
        listenerManager.onSessionExpired(session);
        assertThat(listenerManager.getSession(1L).join().getSessionId()).isEqualTo(20L);
    }

    @Test
    void cleanCloseAndShardRemovalDoNotEmitExpired() throws Exception {
        var events = new CopyOnWriteArrayList<SessionEvent>();
        var listenerManager = managerWithListener(events::add);
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(
                        CompletableFuture.completedFuture(createSessionResponse(10L)),
                        CompletableFuture.completedFuture(createSessionResponse(20L)));
        when(rpcProvider.closeSession(any(CloseSessionRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(new CloseSessionResponse()));

        listenerManager.getSession(1L).join();
        listenerManager.getSession(2L).join();

        // Shard removal is not an expiry.
        listenerManager.accept(
                new ShardAssignmentChanges(
                        Set.of(), Set.of(new Shard(2L, "leader", new HashRange(1, 2))), Set.of()));
        assertThat(events).hasSize(2);

        // Neither is a clean client shutdown.
        listenerManager.close();
        assertThat(events).extracting(SessionEvent::type).containsOnly(SessionEvent.Type.ESTABLISHED);
    }

    private SessionManager managerWithListener(Consumer<SessionEvent> listener) {
        return new SessionManager(
                executor,
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
                        1,
                        null,
                        listener),
                rpcProvider,
                InstrumentProvider.NOOP);
    }

    private static CreateSessionResponse createSessionResponse(long sessionId) {
        return new CreateSessionResponse().setSessionId(sessionId);
    }
}
