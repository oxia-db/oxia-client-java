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

import static java.util.concurrent.CompletableFuture.*;

import com.google.common.collect.Maps;
import io.github.merlimat.slog.Logger;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.CreateSessionRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import lombok.NonNull;

public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges>, SessionNotificationListener {

    private final Logger log;
    private final Map<Long, CompletableFuture<Session>> sessions;
    private final ScheduledExecutorService asyncExecutor;
    private final ClientConfig clientConfig;
    private final InstrumentProvider instrumentProvider;
    private final RpcProvider rpcProvider;
    private final ShardManager shardManager;

    // Guards closed, and the creation and the moves of the sessions
    private final Lock lock;
    private boolean closed;

    public SessionManager(
            @NonNull ScheduledExecutorService asyncExecutor,
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this.log =
                Logger.get(SessionManager.class)
                        .with()
                        .attr("clientIdentity", config.clientIdentifier())
                        .build();
        this.sessions = Maps.newConcurrentMap();
        this.asyncExecutor = asyncExecutor;
        this.clientConfig = config;
        this.instrumentProvider = instrumentProvider;
        this.rpcProvider = rpcProvider;
        this.shardManager = shardManager;
        this.lock = new ReentrantLock();
        this.closed = false;
    }

    /**
     * Returns the session on the shard that {@code shardForKey} returns, which is the shard of the
     * returned session.
     */
    @NonNull
    public CompletableFuture<Session> getSession(@NonNull LongSupplier shardForKey) {
        // Lock-free fast path: this runs on every ephemeral put, and the slow path takes the lock even
        // when the session already exists
        final CompletableFuture<Session> existing = sessions.get(shardForKey.getAsLong());
        if (isUsable(existing)) {
            return existing;
        }

        lock.lock();
        try {
            if (closed) {
                return failedFuture(new IllegalStateException("Session manager is closed"));
            }
            // Look up the shard again, under the lock: the one looked up above may be a split shard
            // whose session has moved since
            final long shardId = shardForKey.getAsLong();
            if (!isUsable(sessions.get(shardId))) {
                // The shard may have replaced a split shard, and inherited its session
                followSplits();
            }
            return sessions.compute(
                    shardId, (key, current) -> isUsable(current) ? current : createSession(shardId));
        } finally {
            lock.unlock();
        }
    }

    private static boolean isUsable(CompletableFuture<Session> sessionFuture) {
        if (sessionFuture == null || sessionFuture.isCompletedExceptionally()) {
            return false;
        }
        if (!sessionFuture.isDone()) {
            return true;
        }
        return !sessionFuture.join().isClosed(); // ignore closed sessions
    }

    @Override
    public void onSessionExpired(Session targetSession) {
        sessions.compute(
                targetSession.getShardId(),
                (shard, existFuture) -> {
                    if (existFuture != null
                            && existFuture.isDone()
                            && !existFuture.isCompletedExceptionally()) {
                        final Session existSession = existFuture.join();
                        if (existSession.getSessionId() == targetSession.getSessionId()) {
                            existSession.close();
                            return null;
                        }
                    }
                    return existFuture;
                });
    }

    @Override
    public void close() throws Exception {
        final List<CompletableFuture<Void>> futures;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            // do our best to close all sessions
            futures =
                    sessions.values().stream()
                            .map(sf -> sf.thenCompose(Session::close).exceptionally(ex -> null))
                            .toList();
            sessions.clear();
        } finally {
            lock.unlock();
        }
        allOf(futures.toArray(CompletableFuture[]::new)).join();
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (changes.removed().isEmpty()) {
            return;
        }
        lock.lock();
        try {
            // Right away: the shards that replaced the removed ones started to count the timeout of
            // the sessions they inherited when they were elected, before the client could know them
            if (!closed) {
                followSplits();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Moves the sessions of the shards that were split to the shards that replaced them. Each of
     * those inherited the session, with its ephemeral records in its hash range, and expires it
     * unless it is kept alive there. It must be called with the lock held.
     */
    private void followSplits() {
        for (long shardId : List.copyOf(sessions.keySet())) {
            if (shardManager.allShardIds().contains(shardId)) {
                continue;
            }
            final List<Long> successors = currentSuccessors(shardId);
            final CompletableFuture<Session> sessionFuture = sessions.get(shardId);
            // A session that is still being created is moved by a later call
            if (successors.isEmpty() || !isUsable(sessionFuture) || !sessionFuture.isDone()) {
                continue;
            }

            final Session session = sessionFuture.join();
            log.info()
                    .attr("shard", shardId)
                    .attr("sessionId", session.getSessionId())
                    .attr("shards", successors)
                    .log("Moving the session to the shards that replaced its shard");
            for (long successor : successors) {
                if (!isUsable(sessions.get(successor))) {
                    sessions.put(successor, completedFuture(newSession(successor, session.getSessionId())));
                }
            }
            sessions.remove(shardId);
            session.detach();
        }
    }

    /**
     * The shards of the shard map that replaced a shard removed from it: the ones that replaced it
     * may have been split in turn.
     */
    private List<Long> currentSuccessors(long shardId) {
        final List<Long> current = new ArrayList<>();
        for (long successor : shardManager.getSuccessors(shardId)) {
            if (shardManager.allShardIds().contains(successor)) {
                current.add(successor);
            } else {
                current.addAll(currentSuccessors(successor));
            }
        }
        return current;
    }

    private CompletableFuture<Session> createSession(long shardId) {
        var request = new CreateSessionRequest();
        request
                .setSessionTimeoutMs((int) clientConfig.sessionTimeout().toMillis())
                .setShard(shardId)
                .setClientIdentity(clientConfig.clientIdentifier());
        return rpcProvider
                .createSession(request)
                .thenApply(response -> newSession(shardId, response.getSessionId()));
    }

    private Session newSession(long shardId, long sessionId) {
        return new Session(
                asyncExecutor, rpcProvider, clientConfig, shardId, sessionId, instrumentProvider, this);
    }
}
