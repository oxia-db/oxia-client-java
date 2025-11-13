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

import static io.oxia.proto.NotificationType.KEY_CREATED;
import static io.oxia.proto.NotificationType.KEY_DELETED;
import static io.oxia.proto.NotificationType.KEY_MODIFIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.metrics.Counter;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ShardNotificationReceiverTest {

    record NotificationWrapper(NotificationBatch notifications, Exception ex, boolean endOfStream) {}

    BlockingQueue<NotificationWrapper> responses = new ArrayBlockingQueue<>(10);

    OxiaClientGrpc.OxiaClientImplBase serviceImpl =
            new OxiaClientGrpc.OxiaClientImplBase() {
                @Override
                public void getNotifications(
                        NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
                    requests.incrementAndGet();
                    NotificationWrapper nw = responses.poll();
                    if (nw != null) {
                        if (nw.ex != null) {
                            responseObserver.onError(nw.ex);
                        } else {
                            responseObserver.onNext(nw.notifications);

                            if (nw.endOfStream) {
                                responseObserver.onCompleted();
                            }
                        }
                    }
                }
            };
    AtomicInteger requests = new AtomicInteger();

    String serverName = InProcessServerBuilder.generateName();
    Server server;

    long shardId = 1L;
    String leader = "address";
    @Mock OxiaStub stub;
    @Mock OxiaStubManager stubManager;
    @Mock Consumer<Notification> notificationCallback;
    @Mock NotificationManager notificationManager;

    @BeforeEach
    void beforeEach() throws Exception {
        requests.set(0);
        responses.clear();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();
        stub = new OxiaStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());
        stubManager = mock(OxiaStubManager.class);
        when(stubManager.getStub(any())).thenReturn(stub);
    }

    @AfterEach
    void afterEach() throws Exception {
        stub.close();
        server.shutdownNow();
    }

    @Test
    void start() throws Exception {
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));

        var notifications =
                NotificationBatch.newBuilder()
                        .putNotifications("key1", created(1L))
                        .putNotifications("key2", deleted(2L))
                        .putNotifications("key3", modified(3L))
                        .build();
        responses.put(new NotificationWrapper(notifications, null, false));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        stubManager,
                        leader,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                verify(notificationCallback).accept(new KeyDeleted("key2"));
                                verify(notificationCallback).accept(new KeyModified("key3", 3L));
                            });
        }
    }

    @Test
    void neverStarts() {
        //        responses.offer(Flux.never());
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        stubManager,
                        leader,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback, never()).accept(any());
                            });
        }
    }

    @Test
    public void recoveryFromError() {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        when(notificationManager.getExecutor()).thenReturn(executorService);
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));

        responses.offer(new NotificationWrapper(null, Status.UNAVAILABLE.asException(), false));
        var notifications =
                NotificationBatch.newBuilder().putNotifications("key1", created(1L)).build();
        responses.offer(new NotificationWrapper(notifications, null, false));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        stubManager,
                        leader,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                            });
        }
        assertThat(requests).hasValue(2);
    }

    @Test
    public void recoveryFromEndOfStream() throws Exception {
        when(notificationManager.getCounterNotificationsReceived()).thenReturn(mock(Counter.class));
        when(notificationManager.getCounterNotificationsBatchesReceived())
                .thenReturn(mock(Counter.class));
        var notifications =
                NotificationBatch.newBuilder().putNotifications("key1", created(1L)).build();
        responses.put(new NotificationWrapper(notifications, null, true));
        try (var notificationReceiver =
                new ShardNotificationReceiver(
                        stubManager,
                        leader,
                        shardId,
                        notificationCallback,
                        notificationManager,
                        OptionalLong.empty())) {
            await()
                    .untilAsserted(
                            () -> {
                                verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                            });
        }
        assertThat(requests).hasValue(2);
    }

    static io.oxia.proto.Notification created(long version) {
        return io.oxia.proto.Notification.newBuilder()
                .setType(KEY_CREATED)
                .setVersionId(version)
                .build();
    }

    static io.oxia.proto.Notification deleted(long version) {
        return io.oxia.proto.Notification.newBuilder()
                .setType(KEY_DELETED)
                .setVersionId(version)
                .build();
    }

    static io.oxia.proto.Notification modified(long version) {
        return io.oxia.proto.Notification.newBuilder()
                .setType(KEY_MODIFIED)
                .setVersionId(version)
                .build();
    }
}
