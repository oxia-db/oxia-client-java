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
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.SessionHeartbeat;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Session implements Closeable {
    @Getter(PACKAGE)
    @VisibleForTesting
    private final long shardId;
    @Getter(PUBLIC)
    private final long sessionId;
    private final OxiaStubProvider stubProvider;
    private final Duration sessionTimeout;
    private final String clientIdentifier;
    private final ScheduledFuture<?> heartbeatFuture;
    private final SessionHeartbeat heartbeatRequest;
    private final StreamObserver<KeepAliveResponse> heartbeatHandler;
    // metrics
    private final Counter sessionsExpired;
    private final Counter sessionsClosed;
    // stats
    private volatile Instant lastSucceedTimestamp;
    private final AtomicBoolean finished;


    Session(@NonNull ScheduledExecutorService executor,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull ClientConfig config,
            long shardId,
            long sessionId,
            InstrumentProvider instrumentProvider) {
        this.finished = new AtomicBoolean(false);
        this.stubProvider = stubProvider;
        this.sessionTimeout = config.sessionTimeout();
        final Duration heartbeatInterval = Duration.ofMillis(
                Math.max(config.sessionTimeout().toMillis() / 10, Duration.ofSeconds(2).toMillis()));
        this.shardId = shardId;
        this.sessionId = sessionId;
        this.clientIdentifier = config.clientIdentifier();
        this.heartbeatRequest =
                SessionHeartbeat.newBuilder().setShard(shardId).setSessionId(sessionId).build();

        log.info(
                "Session created shard={} sessionId={} clientIdentity={}",
                shardId,
                sessionId,
                config.clientIdentifier());

        Counter sessionsOpened = instrumentProvider.newCounter(
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


        this.heartbeatHandler = new StreamObserver<>() {

            @Override
            public void onNext(KeepAliveResponse keepAliveResponse) {
                lastSucceedTimestamp = Instant.now();
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Received keep-alive response shard={} sessionId={} clientIdentity={}",
                            shardId,
                            sessionId,
                            clientIdentifier);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn(
                        "Error during session keep-alive shard={} sessionId={} clientIdentity={}: {}",
                        shardId,
                        sessionId,
                        clientIdentifier,
                        throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                // no action
            }
        };
        sessionsOpened.increment();
        this.lastSucceedTimestamp = Instant.now();
        this.heartbeatFuture =
                executor.scheduleAtFixedRate(
                        heartbeat(),
                        heartbeatInterval.toMillis(),
                        heartbeatInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    public boolean isFinished() {
        return finished.getAcquire();
    }

    private Runnable heartbeat() {
        return () -> {
            try {
                final Duration diff = Duration.between(lastSucceedTimestamp, Instant.now());
                if (diff.toMillis() > sessionTimeout.toMillis()) {
                    sessionsExpired.increment();
                    log.warn(
                            "Session expired shard={} sessionId={} clientIdentity={}",
                            shardId,
                            sessionId,
                            clientIdentifier);
                    close();
                    return;
                }
                stubProvider.getStubForShard(shardId).async().keepAlive(heartbeatRequest, heartbeatHandler);
            } catch (Throwable ex) {
                log.warn("receive error when send keep-alive request", Throwables.getRootCause(ex));
            }
        };
    }

    public void close() {
        finished.setRelease(true);
        sessionsClosed.increment();
        heartbeatFuture.cancel(true);
        try {
            final var stub = stubProvider.getStubForShard(shardId);
            // this is try-our-best operation, we don't care the result
            stub.closeSession(
                    CloseSessionRequest.newBuilder()
                            .setShard(shardId)
                            .setSessionId(sessionId)
                            .build());
        } catch (Throwable ex) {
            log.warn("Unexpected error when notify oxia node the session has closed. shard={} sessionId={}",
                    shardId, sessionId, Throwables.getRootCause(ex));
        }
        log.info(
                "Session closed shard={} sessionId={} clientIdentity={}",
                shardId,
                sessionId,
                clientIdentifier);
    }
}
