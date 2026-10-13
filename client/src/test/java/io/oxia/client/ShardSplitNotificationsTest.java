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
package io.oxia.client;

import static io.oxia.proto.NotificationType.KEY_CREATED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import io.oxia.client.api.Notification.KeyCreated;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A subscription to the notifications follows the splits of the shards: the shards that replace a
 * split shard continue its offsets, and each of them serves the notifications of the split shard
 * that the subscription hasn't read, on its side of the split, before its own. The fake server
 * splits a shard like Oxia does: the shard map replaces it with its children, and the stream of the
 * split shard stays open.
 */
class ShardSplitNotificationsTest {

    private record ShardRange(long id, long min, long max) {}

    // Shard 0 is split into shards 1 and 2, and shard 2 into shards 3 and 4
    private static final ShardRange SHARD_0 = new ShardRange(0, 0, (1L << 32) - 1);
    private static final ShardRange SHARD_1 = new ShardRange(1, 0, (1L << 31) - 1);
    private static final ShardRange SHARD_2 = new ShardRange(2, 1L << 31, (1L << 32) - 1);
    private static final ShardRange SHARD_3 = new ShardRange(3, 1L << 31, (3L << 30) - 1);
    private static final ShardRange SHARD_4 = new ShardRange(4, 3L << 30, (1L << 32) - 1);

    // The offset that the fake server confirms a new subscription at
    private static final long COMMIT_OFFSET = 10;

    private FakeOxia oxia;
    private Server server;
    private AsyncOxiaClient client;
    private final BlockingQueue<Notification> notifications = new LinkedBlockingQueue<>();

    @BeforeEach
    void setup() throws Exception {
        oxia = new FakeOxia();
        server = ServerBuilder.forPort(0).directExecutor().addService(oxia).build().start();
        oxia.leader = "localhost:" + server.getPort();
        oxia.setShards(SHARD_0);
        client =
                OxiaClientBuilder.create(oxia.leader)
                        .requestTimeout(Duration.ofSeconds(10))
                        .asyncClient()
                        .get(10, SECONDS);
    }

    @AfterEach
    void cleanup() throws Exception {
        client.close();
        server.shutdownNow();
    }

    @Test
    void resumesOnTheShardsThatReplacedASplitShard() throws Exception {
        client.notifications(notifications::add);
        var parent = oxia.nextSubscriptions(1).get(SHARD_0.id());
        assertThat(parent.startOffset()).isEmpty();
        parent.send(11, "a");
        assertThat(nextNotification()).isEqualTo(new KeyCreated("a", 11));

        // The stream of the split shard stays open: the client cancels it, and resumes on the shards
        // that replaced it after the last batch it received from it
        oxia.setShards(SHARD_1, SHARD_2);
        parent.cancelled().get(10, SECONDS);
        var children = oxia.nextSubscriptions(2);
        assertThat(children).containsOnlyKeys(SHARD_1.id(), SHARD_2.id());
        assertThat(children.values()).allSatisfy(child -> assertThat(child.startOffset()).hasValue(11));

        children.get(SHARD_1.id()).send(12, "b");
        children.get(SHARD_2.id()).send(13, "c");
        assertThat(List.of(nextNotification(), nextNotification()))
                .containsExactlyInAnyOrder(new KeyCreated("b", 12), new KeyCreated("c", 13));

        // A child split in turn hands its subscription over the same way
        oxia.setShards(SHARD_1, SHARD_3, SHARD_4);
        children.get(SHARD_2.id()).cancelled().get(10, SECONDS);
        var grandchildren = oxia.nextSubscriptions(2);
        assertThat(grandchildren).containsOnlyKeys(SHARD_3.id(), SHARD_4.id());
        assertThat(grandchildren.values())
                .allSatisfy(grandchild -> assertThat(grandchild.startOffset()).hasValue(13));
        assertThat(children.get(SHARD_1.id()).cancelled()).isNotDone();
    }

    @Test
    void resumesOnTheCurrentShardsWhenAChildWasSplitBeforeTheFirstSplitWasSeen() throws Exception {
        client.notifications(notifications::add);
        var parent = oxia.nextSubscriptions(1).get(SHARD_0.id());
        parent.send(11, "a");
        assertThat(nextNotification()).isEqualTo(new KeyCreated("a", 11));

        // Shard 0 was split into shards 1 and 2, and shard 2 into shards 3 and 4, before the client
        // got the shard map with shards 1 and 2
        oxia.setShards(SHARD_1, SHARD_3, SHARD_4);
        parent.cancelled().get(10, SECONDS);
        var successors = oxia.nextSubscriptions(3);
        assertThat(successors).containsOnlyKeys(SHARD_1.id(), SHARD_3.id(), SHARD_4.id());
        assertThat(successors.values())
                .allSatisfy(successor -> assertThat(successor.startOffset()).hasValue(11));
    }

    @Test
    void subscribesToTheShardsThatReplacedAShardSplitBeforeItsSubscriptionWasConfirmed()
            throws Exception {
        oxia.unconfirmed.add(SHARD_0.id());
        client.notifications(notifications::add);
        var parent = oxia.nextSubscriptions(1).get(SHARD_0.id());

        // No batch of the split shard to resume after: the subscriptions to the shards that
        // replaced it start where they are
        oxia.setShards(SHARD_1, SHARD_2);
        parent.cancelled().get(10, SECONDS);
        var children = oxia.nextSubscriptions(2);
        assertThat(children).containsOnlyKeys(SHARD_1.id(), SHARD_2.id());
        assertThat(children.values()).allSatisfy(child -> assertThat(child.startOffset()).isEmpty());

        children.get(SHARD_1.id()).send(COMMIT_OFFSET + 1, "a");
        assertThat(nextNotification()).isEqualTo(new KeyCreated("a", COMMIT_OFFSET + 1));
    }

    private Notification nextNotification() throws InterruptedException {
        var notification = notifications.poll(10, SECONDS);
        assertThat(notification).as("no notification").isNotNull();
        return notification;
    }

    /** A subscription of the client to the notifications of a shard, on the fake server. */
    private record Subscription(
            long shard,
            OptionalLong startOffset,
            ServerCallStreamObserver<NotificationBatch> stream,
            CompletableFuture<Void> cancelled) {

        // Sends a batch with the creation of the keys
        void send(long offset, String... keys) {
            var batch = new NotificationBatch().setShard(shard).setOffset(offset);
            for (var key : keys) {
                batch.putNotifications(key).setType(KEY_CREATED).setVersionId(offset);
            }
            stream.onNext(batch);
        }
    }

    private static final class FakeOxia extends OxiaClientGrpc.OxiaClientImplBase {
        private volatile String leader;
        private final List<StreamObserver<ShardAssignments>> assignmentObservers = new ArrayList<>();
        private ShardAssignments assignments;

        // The shards whose subscriptions the server doesn't confirm
        private final Set<Long> unconfirmed = ConcurrentHashMap.newKeySet();
        private final BlockingQueue<Subscription> subscriptions = new LinkedBlockingQueue<>();

        synchronized void setShards(ShardRange... shards) {
            assignments = new ShardAssignments();
            var nsAssignments = assignments.putNamespaces(OxiaClientBuilderImpl.DefaultNamespace);
            for (var shard : shards) {
                var assignment = nsAssignments.addAssignment().setShard(shard.id()).setLeader(leader);
                assignment
                        .setInt32HashRange()
                        .setMinHashInclusive((int) shard.min())
                        .setMaxHashInclusive((int) shard.max());
            }
            assignmentObservers.forEach(observer -> observer.onNext(assignments));
        }

        // Returns the next subscriptions, by shard
        Map<Long, Subscription> nextSubscriptions(int count) throws InterruptedException {
            var next = new HashMap<Long, Subscription>();
            for (int i = 0; i < count; i++) {
                var subscription = subscriptions.poll(10, SECONDS);
                assertThat(subscription).as("no subscription").isNotNull();
                next.put(subscription.shard(), subscription);
            }
            return next;
        }

        @Override
        public synchronized void getShardAssignments(
                ShardAssignmentsRequest request, StreamObserver<ShardAssignments> responseObserver) {
            assignmentObservers.add(responseObserver);
            responseObserver.onNext(assignments);
        }

        @Override
        public void getNotifications(
                NotificationsRequest request, StreamObserver<NotificationBatch> responseObserver) {
            var stream = (ServerCallStreamObserver<NotificationBatch>) responseObserver;
            var cancelled = new CompletableFuture<Void>();
            stream.setOnCancelHandler(() -> cancelled.complete(null));
            var subscription =
                    new Subscription(
                            request.getShard(),
                            request.hasStartOffsetExclusive()
                                    ? OptionalLong.of(request.getStartOffsetExclusive())
                                    : OptionalLong.empty(),
                            stream,
                            cancelled);
            // Like the Oxia leader, it confirms a subscription with an empty batch at the offset where
            // it starts
            if (!unconfirmed.contains(subscription.shard())) {
                subscription.send(subscription.startOffset().orElse(COMMIT_OFFSET));
            }
            subscriptions.add(subscription);
        }
    }
}
