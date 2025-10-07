/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.oxia.client.notify;

import static io.oxia.client.api.Notification.KeyModified;
import static lombok.AccessLevel.PACKAGE;

import io.grpc.stub.StreamObserver;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.util.Backoff;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import java.io.Closeable;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardNotificationReceiver implements Closeable, StreamObserver<NotificationBatch> {

    private final OxiaStub stub;
    private final NotificationManager notificationManager;

    @Getter(PACKAGE)
    private final long shardId;

    private final @NonNull Consumer<Notification> callback;

    @Getter private volatile @NonNull OptionalLong offset;

    private volatile boolean closed = false;

    private final Backoff backoff = new Backoff();

    ShardNotificationReceiver(
            @NonNull OxiaStub stub,
            long shardId,
            @NonNull Consumer<Notification> callback,
            NotificationManager notificationManager,
            @NonNull OptionalLong offset) {
        this.stub = stub;
        this.notificationManager = notificationManager;
        this.shardId = shardId;
        this.callback = callback;
        this.offset = offset;

        start();
    }

    void start() {
        var request = NotificationsRequest.newBuilder().setShard(shardId);
        offset.ifPresent(request::setStartOffsetExclusive);
        stub.async().getNotifications(request.build(), this);
    }

    @Override
    public void onNext(NotificationBatch batch) {
        if (offset.isPresent() && offset.getAsLong() >= batch.getOffset()) {
            // Ignore repeated notifications
            return;
        }

        offset = OptionalLong.of(batch.getOffset());

        notificationManager.getCounterNotificationsBatchesReceived().increment();
        notificationManager.getCounterNotificationsReceived().add(batch.getNotificationsCount());

        batch
                .getNotificationsMap()
                .forEach(
                        (key, notification) -> {
                            if (log.isDebugEnabled()) {
                                log.debug("--- Got notification: {} - {}", key, notification.getType());
                            }

                            var n =
                                    switch (notification.getType()) {
                                        case KEY_CREATED -> new KeyCreated(key, notification.getVersionId());
                                        case KEY_MODIFIED -> new KeyModified(key, notification.getVersionId());
                                        case KEY_DELETED -> new KeyDeleted(key);
                                        case KEY_RANGE_DELETED ->
                                                new Notification.KeyRangeDelete(key, notification.getKeyRangeLast());
                                        case UNRECOGNIZED -> null;
                                    };

                            if (n != null) {
                                callback.accept(n);
                            }
                        });
    }

    @Override
    public void onError(Throwable t) {
        if (closed) {
            return;
        }

        long retryDelayMillis = backoff.nextDelayMillis();
        log.warn(
                "Error while receiving notifications for shard={}: {} - Retrying in {} seconds",
                shardId,
                t.getMessage(),
                retryDelayMillis / 1000.0);
        notificationManager
                .getExecutor()
                .schedule(
                        () -> {
                            if (!closed) {
                                log.info("Retrying getting notifications for shard={}", shardId);
                                start();
                            }
                        },
                        retryDelayMillis,
                        TimeUnit.MILLISECONDS);
    }

    @Override
    public void onCompleted() {
        if (!closed) {
            start();
        }
    }

    @RequiredArgsConstructor(access = PACKAGE)
    static class Factory {
        private final @NonNull OxiaStubManager stubManager;

        @Getter
        private final @NonNull CompositeConsumer<Notification> callback = new CompositeConsumer<>();

        @NonNull
        ShardNotificationReceiver newReceiver(
                long shardId,
                @NonNull String leader,
                @NonNull NotificationManager notificationManager,
                @NonNull OptionalLong offset) {
            return new ShardNotificationReceiver(
                    stubManager.getStub(leader), shardId, callback, notificationManager, offset);
        }
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
