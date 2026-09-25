/*
 * Copyright © 2026 The Oxia Authors
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

import static io.oxia.proto.NotificationType.KEY_CREATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.Counter;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ShardNotificationReceiverGrpcTest {
    private static final long SHARD_ID = 1L;
    private static final long COMMIT_OFFSET = 10L;
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(200);
    // Each subscription is renewed at a random age in [1.5s, 3s)
    private static final Duration SUBSCRIPTION_MAX_AGE = Duration.ofSeconds(3);

    @Test
    void resumedSubscriptionOnIdleShardStaysLive() throws Exception {
        // Start offset of each subscription, -1 for a new one
        var subscriptions = new CopyOnWriteArrayList<Long>();
        Set<ServerCallStreamObserver<NotificationBatch>> openCalls = ConcurrentHashMap.newKeySet();
        var maxOpenCalls = new AtomicInteger();
        Server server =
                ServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(
                                new OxiaClientGrpc.OxiaClientImplBase() {
                                    @Override
                                    public void getNotifications(
                                            NotificationsRequest request,
                                            StreamObserver<NotificationBatch> responseObserver) {
                                        var call = (ServerCallStreamObserver<NotificationBatch>) responseObserver;
                                        call.setOnCancelHandler(() -> openCalls.remove(call));
                                        openCalls.add(call);
                                        maxOpenCalls.accumulateAndGet(openCalls.size(), Math::max);
                                        // Like the Oxia leader, only a new subscription gets a first "dummy"
                                        // batch: a resumed one gets nothing until a new notification is written
                                        if (request.hasStartOffsetExclusive()) {
                                            subscriptions.add(request.getStartOffsetExclusive());
                                        } else {
                                            subscriptions.add(-1L);
                                            call.onNext(
                                                    new NotificationBatch().setShard(SHARD_ID).setOffset(COMMIT_OFFSET));
                                        }
                                    }
                                })
                        .build()
                        .start();
        var address = "localhost:" + server.getPort();
        var config =
                ((OxiaClientBuilderImpl)
                                OxiaClientBuilder.create(address)
                                        .requestTimeout(REQUEST_TIMEOUT)
                                        .subscriptionMaxAge(SUBSCRIPTION_MAX_AGE))
                        .getClientConfig();
        var executor = Executors.newSingleThreadScheduledExecutor();
        var notificationManager = mock(NotificationManager.class);
        when(notificationManager.getExecutor()).thenReturn(executor);
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));
        var notifications = new CopyOnWriteArrayList<Notification>();

        try (var rpcProvider = RpcProvider.create(config, executor, shardId -> address);
                var receiver =
                        new ShardNotificationReceiver(
                                rpcProvider,
                                SHARD_ID,
                                notifications::add,
                                notificationManager,
                                OptionalLong.empty())) {
            // At its maximum age, the first subscription is resumed from the dummy batch offset
            await().until(() -> subscriptions.contains(COMMIT_OFFSET));
            var subscriptionCount = subscriptions.size();

            // The resumed subscription is not timed out and re-established on the idle shard, and
            // no abandoned call is left open on the server
            Thread.sleep(3 * REQUEST_TIMEOUT.toMillis());
            assertThat(subscriptions).hasSize(subscriptionCount);
            assertThat(openCalls).hasSize(1);
            assertThat(maxOpenCalls).hasValue(1);

            // A notification written later is delivered right away on the live subscription
            var batch = new NotificationBatch().setShard(SHARD_ID).setOffset(COMMIT_OFFSET + 1);
            batch.putNotifications("key").setType(KEY_CREATED).setVersionId(1);
            openCalls.forEach(call -> call.onNext(batch));
            await()
                    .atMost(Duration.ofSeconds(1))
                    .untilAsserted(() -> assertThat(notifications).containsExactly(new KeyCreated("key", 1)));
            assertThat(subscriptions).hasSize(subscriptionCount);
        } finally {
            executor.shutdownNow();
            server.shutdownNow();
        }
    }
}
