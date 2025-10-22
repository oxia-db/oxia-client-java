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

import com.google.common.annotations.VisibleForTesting;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges>, SessionNotificationListener {

    private final ConcurrentMap<Long, CompletableFuture<Session>> sessionsByShardId =
            new ConcurrentHashMap<>();
    private final SessionFactory factory;
    private volatile boolean closed = false;

    public SessionManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        this.factory = new SessionFactory(executor, config, this, stubProvider, instrumentProvider);
    }

    SessionManager(SessionFactory factory) {
        this.factory = factory;
    }

    @NonNull
    public CompletableFuture<Session> getSession(long shardId) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("session manager has been closed"));
        }

        return sessionsByShardId.compute(
                shardId,
                (key, existing) -> {
                    if (existing != null && !existing.isCompletedExceptionally()) {
                        return existing;
                    }
                    return factory.create(shardId);
                });
    }

    @Override
    public void onSessionClosed(Session session) {
        sessionsByShardId.remove(session.getShardId());
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        sessionsByShardId.values().stream().map(this::closeQuietly).forEach(CompletableFuture::join);
    }

    @VisibleForTesting
    Map<Long, Session> sessions() {
        Map<Long, Session> sessions = new HashMap<>(sessionsByShardId.size());
        for (var e : sessionsByShardId.entrySet()) {
            if (e.getValue().isDone() && !e.getValue().isCompletedExceptionally()) {
                sessions.put(e.getKey(), e.getValue().join());
            }
        }
        return sessions;
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (!closed) {
            // Removed shards do not have any sessions to keep alive
            changes.removed().forEach(s -> closeQuietly(sessionsByShardId.remove(s.id())));
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> closeQuietly(CompletableFuture<Session> sessionFuture) {
        return sessionFuture.thenCompose(Session::close).exceptionally(ex -> null);
    }
}
