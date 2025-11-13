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
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.Notification.KeyDeleted;
import io.oxia.client.api.Notification.KeyModified;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.HashRange;
import io.oxia.client.shard.Shard;
import io.oxia.client.shard.ShardAssignmentsContainer;
import io.oxia.client.shard.ShardManager;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NotificationManagerTest {

    @Nested
    @DisplayName("Simple lifecycle tests")
    class SimpleLifecycleTest {

        @Mock ShardManager shardManager;
        @Mock ShardAssignmentsContainer assignments;
        ShardNotificationReceiver.Factory receiverFactory;
        @Mock ShardNotificationReceiver receiver1;
        @Mock ShardNotificationReceiver receiver2;
        @Mock ShardNotificationReceiver receiver3;
        NotificationManager manager;
        CompositeConsumer<Notification> callback = new CompositeConsumer<>();
        HashRange r = new HashRange(1L, 2L);

        ScheduledExecutorService executor;

        @BeforeEach
        void setup() {
            executor = Executors.newSingleThreadScheduledExecutor();
            receiverFactory = mock(ShardNotificationReceiver.Factory.class);
            when(receiverFactory.getCallback()).thenReturn(callback);
            manager =
                    new NotificationManager(executor, receiverFactory, shardManager, InstrumentProvider.NOOP);
        }

        @AfterEach
        void teardown() throws Exception {
            executor.shutdownNow();
        }

        @Test
        void startOnRegisterCallback() {
            when(shardManager.allShards())
                    .thenReturn(
                            Set.of(
                                    new Shard(1L, "leader1", new HashRange(1, 2)),
                                    new Shard(2L, "leader2", new HashRange(2, 3))));
            lenient().when(shardManager.leader(1L)).thenReturn("leader1");
            lenient().when(shardManager.leader(2L)).thenReturn("leader2");

            manager.registerCallback(n -> {});

            verify(receiverFactory).newReceiver(1L, "leader1", manager, OptionalLong.empty());
            verify(receiverFactory).newReceiver(2L, "leader2", manager, OptionalLong.empty());
        }

        @Test
        void acceptNotStarted() {
            var changes =
                    new ShardAssignmentChanges(
                            Set.of(new Shard(1L, "leader1", r), new Shard(2L, "leader2", r)), Set.of(), Set.of());
            manager.accept(changes);

            verifyNoInteractions(receiver1, receiver2, receiver3);
        }

        @Test
        void acceptRemoveShard() {
            when(receiverFactory.newReceiver(1L, "leader1", manager, OptionalLong.empty()))
                    .thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", manager, OptionalLong.empty()))
                    .thenReturn(receiver2);
            when(shardManager.allShards())
                    .thenReturn(Set.of(new Shard(1L, "leader1", r), new Shard(2L, "leader2", r)));
            lenient().when(shardManager.leader(1L)).thenReturn("leader1");
            lenient().when(shardManager.leader(2L)).thenReturn("leader2");

            manager.registerCallback(n -> {});

            var changes =
                    new ShardAssignmentChanges(Set.of(), Set.of(new Shard(2L, "leader2", r)), Set.of());
            manager.accept(changes);

            verify(receiver2).close();
        }

        @Test
        void acceptAddShard() {
            lenient()
                    .when(receiverFactory.newReceiver(1L, "leader1", manager, OptionalLong.empty()))
                    .thenReturn(receiver1);
            lenient()
                    .when(receiverFactory.newReceiver(2L, "leader2", manager, OptionalLong.empty()))
                    .thenReturn(receiver2);
            lenient().when(shardManager.allShardIds()).thenReturn(Set.of(1L, 2L));
            lenient().when(shardManager.leader(1L)).thenReturn("leader1");
            lenient().when(shardManager.leader(2L)).thenReturn("leader2");

            manager.registerCallback(n -> {});

            when(receiverFactory.newReceiver(3L, "leader3", manager, OptionalLong.empty()))
                    .thenReturn(receiver3);

            var changes =
                    new ShardAssignmentChanges(Set.of(new Shard(3L, "leader3", r)), Set.of(), Set.of());
            manager.accept(changes);
        }

        @Test
        void acceptReassignShard() {
            lenient()
                    .when(receiverFactory.newReceiver(2, "leader2", manager, OptionalLong.empty()))
                    .thenReturn(receiver2);
            when(shardManager.allShards())
                    .thenReturn(
                            Set.of(
                                    new Shard(1, "leader1", new HashRange(1, 2)),
                                    new Shard(2, "leader2", new HashRange(2, 3))));
            lenient().when(shardManager.leader(1L)).thenReturn("leader1");
            lenient().when(shardManager.leader(2L)).thenReturn("leader2");

            manager.registerCallback(n -> {});

            var shard2offset = 1000L;
            when(receiverFactory.newReceiver(2L, "leader3", manager, OptionalLong.of(shard2offset)))
                    .thenReturn(receiver3);
            when(receiver2.getOffset()).thenReturn(OptionalLong.of(shard2offset));

            var changes =
                    new ShardAssignmentChanges(Set.of(), Set.of(), Set.of(new Shard(2L, "leader3", r)));
            manager.accept(changes);

            verify(receiver2).close();
        }

        @Test
        void closeNotStarted() {
            assertThatNoException().isThrownBy(() -> manager.close());
        }

        @Test
        void close() throws Exception {
            when(receiverFactory.newReceiver(1L, "leader1", manager, OptionalLong.empty()))
                    .thenReturn(receiver1);
            when(receiverFactory.newReceiver(2L, "leader2", manager, OptionalLong.empty()))
                    .thenReturn(receiver2);
            when(shardManager.allShards())
                    .thenReturn(
                            Set.of(
                                    new Shard(1, "leader1", new HashRange(1, 2)),
                                    new Shard(2, "leader2", new HashRange(2, 3))));
            lenient().when(shardManager.leader(1L)).thenReturn("leader1");
            lenient().when(shardManager.leader(2L)).thenReturn("leader2");

            manager.registerCallback(n -> {});

            manager.close();

            verify(receiver1).close();
            verify(receiver2).close();

            assertThatThrownBy(() -> manager.registerCallback(callback))
                    .isInstanceOf(IllegalStateException.class);

            var changes1 =
                    new ShardAssignmentChanges(
                            Set.of(new Shard(1L, "leader1", r), new Shard(2L, "leader2", r)), Set.of(), Set.of());
            manager.accept(changes1);

            verifyNoMoreInteractions(receiverFactory, receiver1, receiver2);
        }
    }

    @Nested
    @DisplayName("GRPC tests")
    class GrpcTest {

        BlockingQueue<NotificationBatch> responses1 = new ArrayBlockingQueue<>(10);
        BlockingQueue<NotificationBatch> responses2 = new ArrayBlockingQueue<>(10);

        OxiaClientGrpc.OxiaClientImplBase serviceImpl1 =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getNotifications(
                            NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
                        NotificationBatch assignments = responses1.poll();
                        if (assignments != null) {
                            responseObserver.onNext(assignments);
                        }
                    }
                };

        OxiaClientGrpc.OxiaClientImplBase serviceImpl2 =
                new OxiaClientGrpc.OxiaClientImplBase() {
                    @Override
                    public void getNotifications(
                            NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
                        NotificationBatch assignments = responses2.poll();
                        if (assignments != null) {
                            responseObserver.onNext(assignments);
                        }
                    }
                };

        String serverName1 = InProcessServerBuilder.generateName();
        String serverName2 = InProcessServerBuilder.generateName();
        Server server1;
        Server server2;
        ManagedChannel channel1;
        ManagedChannel channel2;

        long shardId1 = 1L;
        long shardId2 = 2L;
        OxiaStub stub1;
        OxiaStub stub2;
        @Mock OxiaStubManager stubManager;
        @Mock ShardManager shardManager;
        @Mock ShardAssignmentsContainer assignments;
        @Mock Consumer<Notification> notificationCallback;

        @BeforeEach
        void beforeEach() throws Exception {
            responses1.clear();
            responses2.clear();
            server1 =
                    InProcessServerBuilder.forName(serverName1)
                            .directExecutor()
                            .addService(serviceImpl1)
                            .build()
                            .start();
            server2 =
                    InProcessServerBuilder.forName(serverName2)
                            .directExecutor()
                            .addService(serviceImpl2)
                            .build()
                            .start();
            channel1 = InProcessChannelBuilder.forName(serverName1).directExecutor().build();
            channel2 = InProcessChannelBuilder.forName(serverName2).directExecutor().build();
            stub1 = new OxiaStub(channel1);
            stub2 = new OxiaStub(channel2);
            when(stubManager.getStub("leader1")).thenReturn(stub1);
            when(stubManager.getStub("leader2")).thenThrow(new IllegalStateException("illegal state"));
        }

        @Test
        void notificationsFromMultipleShards() throws Exception {
            var notifications1 =
                    NotificationBatch.newBuilder()
                            .putNotifications("key1", created(1L))
                            .putNotifications("key3", modified(3L))
                            .build();

            var notifications2 =
                    NotificationBatch.newBuilder().putNotifications("key2", deleted()).build();

            responses1.offer(notifications1);
            responses2.offer(notifications2);

            HashRange r = new HashRange(1, 2);

            @Cleanup("shutdownNow")
            var executor = Executors.newSingleThreadScheduledExecutor();
            try (var manager =
                    new NotificationManager(executor, stubManager, shardManager, InstrumentProvider.NOOP)) {
                manager.registerCallback(notificationCallback);
                var changes =
                        new ShardAssignmentChanges(
                                Set.of(new Shard(1L, "leader1", r), new Shard(2L, "leader2", r)),
                                Set.of(),
                                Set.of());
                manager.accept(changes);
                doReturn(stub2).when(stubManager).getStub("leader2");
                await()
                        .untilAsserted(
                                () -> {
                                    verify(notificationCallback).accept(new KeyCreated("key1", 1L));
                                    verify(notificationCallback).accept(new KeyDeleted("key2"));
                                    verify(notificationCallback).accept(new KeyModified("key3", 3L));
                                });
            }
        }

        @AfterEach
        void afterEach() {
            channel1.shutdownNow();
            channel2.shutdownNow();
            server1.shutdownNow();
            server2.shutdownNow();
        }

        static io.oxia.proto.Notification created(long version) {
            return io.oxia.proto.Notification.newBuilder()
                    .setType(KEY_CREATED)
                    .setVersionId(version)
                    .build();
        }

        static io.oxia.proto.Notification deleted() {
            return io.oxia.proto.Notification.newBuilder().setType(KEY_DELETED).build();
        }

        static io.oxia.proto.Notification modified(long version) {
            return io.oxia.proto.Notification.newBuilder()
                    .setType(KEY_MODIFIED)
                    .setVersionId(version)
                    .build();
        }
    }
}
