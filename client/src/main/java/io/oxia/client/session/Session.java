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

import com.google.common.base.Throwables;
import io.github.merlimat.slog.Logger;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;

public class Session {
    private final Logger log;
    @Getter private final long shardId;
    @Getter private final long sessionId;

    private final RpcProvider rpcProvider;
    private final Duration sessionTimeout;

    private final SessionHeartbeat heartbeat;
    private final Duration heartbeatInterval;
    private final SessionNotificationListener listener;

    private final ScheduledFuture<?> heartbeatFuture;
    private volatile Instant lastSuccessfulResponse;

    private final Counter sessionsOpened;
    private final Counter sessionsExpired;
    private final Counter sessionsClosed;

    Session(
            @NonNull ScheduledExecutorService executor,
            @NonNull RpcProvider rpcProvider,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull SessionNotificationListener listener) {
        this.rpcProvider = rpcProvider;
        this.sessionTimeout = config.sessionTimeout();
        this.heartbeatInterval =
                Duration.ofMillis(
                        Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis()));
        this.shardId = shardId;
        this.sessionId = sessionId;
        this.heartbeat = new SessionHeartbeat();
        this.heartbeat.setShard(shardId).setSessionId(sessionId);
        this.listener = listener;
        this.log =
                Logger.get(Session.class)
                        .with()
                        .attr("shard", shardId)
                        .attr("sessionId", sessionId)
                        .attr("clientIdentity", config.clientIdentifier())
                        .build();

        log.info("Session created");

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

        this.lastSuccessfulResponse = Instant.now();
        this.heartbeatFuture =
                executor.scheduleAtFixedRate(
                        this::keepAlive,
                        heartbeatInterval.toMillis(),
                        heartbeatInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private void keepAlive() {
        try {
            Duration diff = Duration.between(lastSuccessfulResponse, Instant.now());
            if (diff.toMillis() > sessionTimeout.toMillis()) {
                sessionsExpired.increment();
                log.warn("Session expired");
                close();
                return;
            }

            rpcProvider
                    .keepAlive(heartbeat, heartbeatInterval)
                    .thenRun(
                            () -> {
                                lastSuccessfulResponse = Instant.now();
                                log.debug("Received keep-alive response");
                            })
                    .exceptionally(
                            error -> {
                                log.warn()
                                        .exceptionMessage(OxiaStatusException.toException(error))
                                        .log("Error during session keep-alive");
                                return null;
                            });
        } catch (Throwable ex) {
            log.warn()
                    .exception(Throwables.getRootCause(ex))
                    .log("receive error when send keep-alive request");
        }
    }

    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future;
        try {
            sessionsClosed.increment();
            heartbeatFuture.cancel(true);
            var closeRequest = new CloseSessionRequest();
            closeRequest.setShard(shardId).setSessionId(sessionId);
            future = rpcProvider.closeSession(closeRequest).thenRun(() -> {});
        } catch (Throwable ex) {
            future = CompletableFuture.failedFuture(Throwables.getRootCause(ex));
        }
        return future.whenComplete(
                (__, ignore) -> {
                    listener.onSessionExpired(Session.this);
                    log.info("Session closed");
                });
    }
}
