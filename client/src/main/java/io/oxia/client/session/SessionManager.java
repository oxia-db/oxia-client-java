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

import com.google.common.collect.Maps;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.CreateSessionRequest;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import lombok.NonNull;

public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges>, SessionNotificationListener {

    private final Map<Long, CompletableFuture<Session>> sessions;
    private final ScheduledExecutorService asyncExecutor;
    private final ClientConfig clientConfig;
    private final InstrumentProvider instrumentProvider;
    private final RpcProvider rpcProvider;

    private final ReadWriteLock closedLock;
    private boolean closed;

    public SessionManager(
            @NonNull ScheduledExecutorService asyncExecutor,
            @NonNull ClientConfig config,
            @NonNull RpcProvider rpcProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        this.sessions = Maps.newConcurrentMap();
        this.asyncExecutor = asyncExecutor;
        this.clientConfig = config;
        this.instrumentProvider = instrumentProvider;
        this.rpcProvider = rpcProvider;
        this.closedLock = new ReentrantReadWriteLock();
        this.closed = false;
    }

    @NonNull
    public CompletableFuture<Session> getSession(long shardId) {
        final Lock rLock = closedLock.readLock();
        try {
            rLock.lock();
            if (closed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Session manager is closed"));
            }
            return sessions.compute(
                    shardId,
                    (key, existing) -> {
                        if (existing != null && !existing.isCompletedExceptionally()) {
                            if (!existing.isDone()) {
                                return existing;
                            }
                            final Session session = existing.join();
                            if (!session.isClosed()) { // ignore closed session
                                return existing;
                            }
                        }
                        return createSession(shardId);
                    });
        } finally {
            rLock.unlock();
        }
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
        final Lock wLock = closedLock.writeLock();
        try {
            wLock.lock();
            if (closed) {
                return;
            }
            closed = true;
            // async close session without wait
            sessions.values().forEach(sf -> sf.thenCompose(Session::close));
            sessions.clear();
        } finally {
            wLock.unlock();
        }
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        changes
                .removed()
                .forEach(
                        s -> {
                            final CompletableFuture<Session> sessionFuture = sessions.remove(s.id());
                            if (sessionFuture != null) {
                                sessionFuture.thenCompose(Session::close);
                            }
                        });
    }

    private CompletableFuture<Session> createSession(long shardId) {
        var request = new CreateSessionRequest();
        request
                .setSessionTimeoutMs((int) clientConfig.sessionTimeout().toMillis())
                .setShard(shardId)
                .setClientIdentity(clientConfig.clientIdentifier());
        return rpcProvider
                .createSession(request)
                .thenApply(
                        response ->
                                new Session(
                                        asyncExecutor,
                                        rpcProvider,
                                        clientConfig,
                                        shardId,
                                        response.getSessionId(),
                                        instrumentProvider,
                                        this));
    }
}
