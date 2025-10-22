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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock SessionFactory factory;
    @Mock Session session;
    SessionManager manager;
    ScheduledExecutorService executor;

    @BeforeEach
    void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        manager = new SessionManager(factory);
    }

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    @Test
    void newSession() {
        var shardId = 1L;
        when(factory.create(shardId)).thenReturn(CompletableFuture.completedFuture(session));
        assertThat(manager.getSession(shardId).join()).isSameAs(session);
        verify(factory).create(shardId);
    }

    @Test
    void existingSession() {
        var shardId = 1L;
        when(factory.create(shardId)).thenReturn(CompletableFuture.completedFuture(session));
        var session1 = manager.getSession(shardId);
        verify(factory, times(1)).create(shardId);

        var session2 = manager.getSession(shardId);
        assertThat(session2).isSameAs(session1);
        verifyNoMoreInteractions(factory, session);
    }

    @Test
    void existingSessionWithFailure() {
        var shardId = 1L;
        // first failed
        when(factory.create(shardId))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed")));
        var session1 = manager.getSession(shardId);
        assertThat(session1).isCompletedExceptionally();
        verify(factory, times(1)).create(shardId);

        // second should be success
        when(factory.create(shardId)).thenReturn(CompletableFuture.completedFuture(session));
        var session2 = manager.getSession(shardId);
        assertThat(session2).isCompletedWithValue(session);
        verify(factory, times(2)).create(shardId);

        // third should be cache
        var session3 = manager.getSession(shardId);
        assertThat(session3).isSameAs(session2);
        verify(factory, times(2)).create(shardId);
        verifyNoMoreInteractions(factory, session);
    }

    @Test
    void close() throws Exception {
        var shardId = 5L;
        when(session.getShardId()).thenReturn(shardId);
        when(factory.create(shardId)).thenReturn(CompletableFuture.completedFuture(session));
        doAnswer(
                        invocation -> {
                            manager.onSessionClosed(session);
                            return CompletableFuture.completedFuture(null);
                        })
                .when(session)
                .close();
        manager.getSession(shardId);

        assertThat(manager.sessions()).containsEntry(shardId, session);
        manager.close();
        await()
                .untilAsserted(
                        () -> {
                            verify(session).close();
                            assertThat(manager.sessions()).doesNotContainKey(shardId);
                        });

        assertThatThrownBy(() -> manager.getSession(shardId).join())
                .isInstanceOf(CompletionException.class);
    }

    @Test
    void accept() throws Exception {
        Session session21 = mock(Session.class);
        Session session22 = mock(Session.class);
        var shardId1 = 1L;
        var shardId2 = 2L;
        when(factory.create(shardId1)).thenReturn(CompletableFuture.completedFuture(session));
        when(factory.create(shardId2))
                .thenReturn(
                        CompletableFuture.completedFuture(session21),
                        CompletableFuture.completedFuture(session22));

        assertThat(manager.getSession(shardId1).join()).isSameAs(session);
        assertThat(manager.getSession(shardId2).join()).isSameAs(session21);

        manager.accept(
                new ShardAssignmentChanges(
                        Set.of(),
                        Set.of(new Shard(shardId1, "leader1", new HashRange(1, 2))),
                        Set.of(new Shard(shardId2, "leader3", new HashRange(3, 4)))));

        assertThat(manager.sessions()).doesNotContainKey(shardId1);
        // Session here shouldn't have changed after the reassignment
        assertThat(manager.getSession(shardId2).join()).isSameAs(session21);
        verify(session).close();
    }

    @Test
    void testSessionClosed() throws Exception {
        var shardId = 1L;
        when(session.getShardId()).thenReturn(shardId);
        doAnswer(
                        invocation -> {
                            manager.onSessionClosed(session);
                            return null;
                        })
                .when(session)
                .close();
        when(factory.create(shardId)).thenReturn(CompletableFuture.completedFuture(session));
        manager.getSession(shardId);

        assertThat(manager.sessions()).containsEntry(shardId, session);

        session.close();
        assertThat(manager.sessions()).doesNotContainKey(shardId);
    }
}
