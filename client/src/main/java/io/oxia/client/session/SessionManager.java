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
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import io.oxia.proto.CreateSessionRequest;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionManager
        implements AutoCloseable, Consumer<ShardAssignmentChanges> {

    private final ScheduledExecutorService borrowedScheduler;
    private final OxiaStubProvider provider;
    private final ClientConfig clientConfig;
    private final InstrumentProvider instrumentProvider;
    private final Map<Long, CompletableFuture<Session>> sessions;

    public SessionManager(
            @NonNull ScheduledExecutorService scheduler,
            @NonNull ClientConfig config,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull InstrumentProvider instrumentProvider) {
        this.sessions = new ConcurrentHashMap<>();
        this.borrowedScheduler = scheduler;
        this.clientConfig = config;
        this.provider = stubProvider;
        this.instrumentProvider = instrumentProvider;
    }

    @NonNull
    private CompletableFuture<Session> createSession(long shardId) {
        try {
            final var stub = provider.getStubForShard(shardId);
            return stub.createSession(CreateSessionRequest.newBuilder()
                            .setSessionTimeoutMs((int) clientConfig.sessionTimeout().toMillis())
                            .setShard(shardId)
                            .setClientIdentity(clientConfig.clientIdentifier())
                            .build())
                    .thenApply(response -> new Session(
                            borrowedScheduler,
                            provider,
                            clientConfig,
                            shardId,
                            response.getSessionId(),
                            instrumentProvider));
        } catch (Throwable ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    @NonNull
    public CompletableFuture<Session> getSession(long shardId) {
        return sessions.compute(
                shardId,
                (key, existing) -> {
                    if (existing != null) {
                        if (!existing.isDone()) {
                            return existing;
                        }
                        if (!existing.isCompletedExceptionally()) {
                            final Session session = existing.join();
                            if (!session.isFinished()) {
                                return existing;
                            }
                        }
                    }
                    return createSession(shardId);
                });
    }


    @Override
    public void close() throws Exception {
        for (CompletableFuture<Session> session : sessions.values()) {
            session.thenAccept(Session::close).exceptionally(ignore -> null);
        }
    }


    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        for (Shard shard : changes.removed()) {
            final CompletableFuture<Session> sessionFuture = sessions.remove(shard.id());
            if (sessionFuture != null) {
                sessionFuture.thenAccept(Session::close).exceptionally(ignore -> null);
            }
        }
    }


    @VisibleForTesting
    Map<Long, Session> sessions() {
        Map<Long, Session> sessions = new HashMap<>(this.sessions.size());
        for (var e : this.sessions.entrySet()) {
            if (e.getValue().isDone() && !e.getValue().isCompletedExceptionally()) {
                sessions.put(e.getKey(), e.getValue().join());
            }
        }
        return sessions;
    }
}
