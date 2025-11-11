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

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Session implements StreamObserver<KeepAliveResponse> {

    private final @NonNull OxiaStubProvider stubProvider;
    private final @NonNull Duration sessionTimeout;
    private final @NonNull Duration heartbeatInterval;

    @Getter(PACKAGE)
    @VisibleForTesting
    private final long shardId;

    @Getter(PUBLIC)
    private final long sessionId;

    private final String clientIdentifier;

    private final @NonNull SessionHeartbeat heartbeat;

    private final @NonNull SessionNotificationListener listener;

    private volatile boolean closed;

    private Counter sessionsOpened;
    private Counter sessionsExpired;
    private Counter sessionsClosed;

    private final ScheduledFuture<?> heartbeatFuture;

    private volatile Instant lastSuccessfullResponse;

    Session(
            @NonNull ScheduledExecutorService executor,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId,
            InstrumentProvider instrumentProvider,
            SessionNotificationListener listener) {
        this.stubProvider = stubProvider;
        this.sessionTimeout = config.sessionTimeout();
        this.heartbeatInterval =
                Duration.ofMillis(
                        Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis()));
        this.shardId = shardId;
        this.sessionId = sessionId;
        this.clientIdentifier = config.clientIdentifier();
        this.heartbeat =
                SessionHeartbeat.newBuilder().setShard(shardId).setSessionId(sessionId).build();
        this.listener = listener;

        log.info(
                "Session created shard={} sessionId={} clientIdentity={}",
                shardId,
                sessionId,
                config.clientIdentifier());

        this.sessionsOpened =
                instrumentProvider.newCounter(
                        "oxia.client.sessions.opened",
                        Unit.Sessions,
                        "The total number of sessions opened by this client",
                        Attributes.builder().put("oxia.shard", shardId).build());
        this.sessionsExpired =
                instrumentProvider.newCounter(
                        "oxia.client.sessions.expired",
                        Unit.Sessions,
                        "The total number of sessions expired int this client",
                        Attributes.builder().put("oxia.shard", shardId).build());
        this.sessionsClosed =
                instrumentProvider.newCounter(
                        "oxia.client.sessions.closed",
                        Unit.Sessions,
                        "The total number of sessions closed by this client",
                        Attributes.builder().put("oxia.shard", shardId).build());

        sessionsOpened.increment();

        this.lastSuccessfullResponse = Instant.now();
        this.heartbeatFuture =
                executor.scheduleAtFixedRate(
                        () -> {
                            // we should catch exception to avoid the schedule future complete
                            try {
                                sendKeepAlive();
                            } catch (Throwable ex) {
                                log.warn("receive error when send keep-alive request", Throwables.getRootCause(ex));
                            }
                        },
                        heartbeatInterval.toMillis(),
                        heartbeatInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private void sendKeepAlive() {
        Duration diff = Duration.between(lastSuccessfullResponse, Instant.now());

        if (diff.toMillis() > sessionTimeout.toMillis()) {
            handleSessionExpired();
            return;
        }

        stubProvider.getStubForShard(shardId).async().keepAlive(heartbeat, this);
    }

    @Override
    public void onNext(KeepAliveResponse value) {
        lastSuccessfullResponse = Instant.now();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Received keep-alive response shard={} sessionId={} clientIdentity={}",
                    shardId,
                    sessionId,
                    clientIdentifier);
        }
    }

    @Override
    public void onError(Throwable t) {
        log.warn(
                "Error during session keep-alive shard={} sessionId={} clientIdentity={}: {}",
                shardId,
                sessionId,
                clientIdentifier,
                t.getMessage());
    }

    @Override
    public void onCompleted() {
        // Nothing to do
    }

    private void handleSessionExpired() {
        sessionsExpired.increment();
        log.warn(
                "Session expired shard={} sessionId={} clientIdentity={}",
                shardId,
                sessionId,
                clientIdentifier);
        close();
    }

    public CompletableFuture<CloseSessionResponse> close() {
        sessionsClosed.increment();
        heartbeatFuture.cancel(true);
        CompletableFuture<CloseSessionResponse> future;
        try {
            final var stub = stubProvider.getStubForShard(shardId);
            future =
                    stub.closeSession(
                            CloseSessionRequest.newBuilder().setShard(shardId).setSessionId(sessionId).build());
        } catch (Throwable ex) {
            future = CompletableFuture.failedFuture(Throwables.getRootCause(ex));
        }
        return future.whenComplete(
                (__, ignore) -> {
                    listener.onSessionClosed(Session.this);
                    log.info(
                            "Session closed shard={} sessionId={} clientIdentity={}",
                            shardId,
                            sessionId,
                            clientIdentifier);
                });
    }
}
