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
package io.oxia.client.notify;

import io.opentelemetry.api.common.Attributes;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NotificationManager implements AutoCloseable, Consumer<ShardAssignmentChanges> {
    private final ConcurrentMap<Long, ShardNotificationReceiver> shardReceivers =
            new ConcurrentHashMap<>();
    private final @NonNull ShardNotificationReceiver.Factory receiverFactory;
    private final @NonNull ShardManager shardManager;
    private final CompositeConsumer<Notification> compositeCallback;

    @Getter private final ScheduledExecutorService executor;
    private volatile boolean started = false;
    private volatile boolean closed = false;

    @Getter private final Counter counterNotificationsReceived;
    @Getter private final Counter counterNotificationsBatchesReceived;

    public NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull OxiaStubManager stubManager,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this(
                executor,
                new ShardNotificationReceiver.Factory(stubManager),
                shardManager,
                instrumentProvider);
    }

    public NotificationManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull ShardNotificationReceiver.Factory receiverFactory,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider) {
        this.receiverFactory = receiverFactory;
        this.compositeCallback = receiverFactory.getCallback();
        this.shardManager = shardManager;
        this.executor = executor;

        this.counterNotificationsReceived =
                instrumentProvider.newCounter(
                        "oxia.client.notifications.received",
                        Unit.Events,
                        "The total number of notification events",
                        Attributes.empty());
        this.counterNotificationsBatchesReceived =
                instrumentProvider.newCounter(
                        "oxia.client.notifications.batches.received",
                        Unit.Events,
                        "The total number of notification batches received",
                        Attributes.empty());
    }

    @Override
    public void accept(@NonNull ShardAssignmentChanges changes) {
        if (!started || closed) {
            return;
        }
        connectNotificationReceivers(changes);
    }

    public void registerCallback(@NonNull Consumer<Notification> callback) {
        if (closed) {
            throw new IllegalStateException("Notification manager has been closed");
        }
        compositeCallback.add(callback);
        if (!started) {
            synchronized (this) {
                if (!started) {
                    bootstrap();
                    started = true;
                }
            }
        }
    }

    private void bootstrap() {
        connectNotificationReceivers(
                new ShardAssignmentChanges(
                        Set.copyOf(shardManager.allShards()), Collections.emptySet(), Collections.emptySet()));
    }

    private void connectNotificationReceivers(@NonNull ShardAssignmentChanges changes) {
        changes.removed().forEach(shard -> shardReceivers.remove(shard.id()).close());
        changes
                .added()
                .forEach(
                        s ->
                                shardReceivers.computeIfAbsent(
                                        s.id(),
                                        id ->
                                                receiverFactory.newReceiver(
                                                        s.id(), s.leader(), this, OptionalLong.empty())));
        changes
                .reassigned()
                .forEach(
                        s -> {
                            var receiver = Optional.ofNullable(shardReceivers.remove(s.id()));
                            receiver.ifPresent(ShardNotificationReceiver::close);
                            var offset =
                                    receiver.map(ShardNotificationReceiver::getOffset).orElse(OptionalLong.empty());
                            shardReceivers.computeIfAbsent(
                                    s.id(), id -> receiverFactory.newReceiver(s.id(), s.leader(), this, offset));
                        });
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        shardReceivers.values().parallelStream().forEach(ShardNotificationReceiver::close);
    }
}
