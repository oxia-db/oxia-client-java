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
import java.util.function.Consumer;
import lombok.NonNull;

public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges>, SessionNotificationListener {

    private final Map<Long, CompletableFuture<Session>> sessions;
    private final ScheduledExecutorService asyncExecutor;
    private final ClientConfig clientConfig;
    private final InstrumentProvider instrumentProvider;
    private final RpcProvider rpcProvider;

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
    }

    @NonNull
    public CompletableFuture<Session> getSession(long shardId) {
        return sessions.compute(
                shardId,
                (key, existing) -> {
                    if (existing != null && !existing.isCompletedExceptionally()) {
                        return existing;
                    }
                    return createSession(shardId);
                });
    }

    @Override
    public void onSessionExpired(Session session) {
        closeSessionQuietly(sessions.remove(session.getShardId()));
    }

    @Override
    public void close() throws Exception {
        sessions.values().stream().map(this::closeSessionQuietly).forEach(CompletableFuture::join);
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        changes.removed().forEach(s -> closeSessionQuietly(sessions.remove(s.id())));
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

    private CompletableFuture<Void> closeSessionQuietly(CompletableFuture<Session> sessionFuture) {
        if (sessionFuture == null) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<Void> future =
                sessionFuture.thenCompose(Session::close).thenApply(__ -> null);
        return future.exceptionally(ex -> null);
    }
}
