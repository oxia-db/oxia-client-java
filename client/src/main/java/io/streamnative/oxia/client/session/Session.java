/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client.session;

import static lombok.AccessLevel.PACKAGE;
import static lombok.AccessLevel.PUBLIC;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.Attributes;
import io.streamnative.oxia.client.ClientConfig;
import io.streamnative.oxia.client.grpc.OxiaStub;
import io.streamnative.oxia.client.metrics.Counter;
import io.streamnative.oxia.client.metrics.InstrumentProvider;
import io.streamnative.oxia.client.metrics.Unit;
import io.streamnative.oxia.client.util.Backoff;
import io.streamnative.oxia.proto.CloseSessionRequest;
import io.streamnative.oxia.proto.CloseSessionResponse;
import io.streamnative.oxia.proto.KeepAliveResponse;
import io.streamnative.oxia.proto.SessionHeartbeat;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Session implements StreamObserver<KeepAliveResponse> {

    private final @NonNull Function<Long, OxiaStub> stubByShardId;
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
    private final Backoff backoff = new Backoff();

    private volatile Instant lastSuccessfullResponse;

    Session(
            @NonNull ScheduledExecutorService executor,
            @NonNull Function<Long, OxiaStub> stubByShardId,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId,
            InstrumentProvider instrumentProvider,
            SessionNotificationListener listener) {
        this.stubByShardId = stubByShardId;
        this.sessionTimeout = config.sessionTimeout();
        this.heartbeatInterval =
                Duration.ofMillis(
                        Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis()));
        this.shardId = shardId;
        this.sessionId = sessionId;
        this.clientIdentifier = config.clientIdentifier();
        this.heartbeat =
                SessionHeartbeat.newBuilder().setShardId(shardId).setSessionId(sessionId).build();
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
                        this::sendKeepAlive,
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

        stubByShardId.apply(shardId).async().keepAlive(heartbeat, this);
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

    public CompletableFuture<Void> close() {
        sessionsClosed.increment();
        heartbeatFuture.cancel(true);
        var stub = stubByShardId.apply(shardId);
        var request =
                CloseSessionRequest.newBuilder().setShardId(shardId).setSessionId(sessionId).build();

        CompletableFuture<Void> result = new CompletableFuture<>();
        stub.async()
                .closeSession(
                        request,
                        new StreamObserver<>() {
                            @Override
                            public void onNext(CloseSessionResponse value) {
                                log.info(
                                        "Session closed shard={} sessionId={} clientIdentity={}",
                                        shardId,
                                        sessionId,
                                        clientIdentifier);
                                listener.onSessionClosed(Session.this);
                                result.complete(null);
                            }

                            @Override
                            public void onError(Throwable t) {
                                // Ignore errors in closing the session, since it might have already expired
                                listener.onSessionClosed(Session.this);
                                result.complete(null);
                            }

                            @Override
                            public void onCompleted() {}
                        });

        return result;
    }
}
