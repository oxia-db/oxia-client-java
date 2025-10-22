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
package io.oxia.client.shard;

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.proto.Int32HashRange;
import io.oxia.proto.NamespaceShardsAssignment;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ShardManagerGrpcTest {
    record AssignmentWrapper(ShardAssignments assignments, Throwable ex, boolean closed) {}

    BlockingQueue<AssignmentWrapper> responses = new ArrayBlockingQueue<>(10);

    OxiaClientGrpc.OxiaClientImplBase serviceImpl =
            new OxiaClientGrpc.OxiaClientImplBase() {
                @Override
                public void getShardAssignments(
                        ShardAssignmentsRequest request, StreamObserver<ShardAssignments> responseObserver) {
                    requests.incrementAndGet();

                    while (true) {
                        var aw = responses.poll();
                        if (aw == null) {
                            return;
                        }

                        if (aw.ex != null) {
                            responseObserver.onError(aw.ex);
                            return;
                        } else {
                            if (aw.assignments != null) {
                                responseObserver.onNext(aw.assignments);
                            }

                            if (aw.closed) {
                                responseObserver.onCompleted();
                                return;
                            }
                        }
                    }
                }
            };
    AtomicInteger requests = new AtomicInteger();

    String serverName = InProcessServerBuilder.generateName();
    Server server;

    @Mock OxiaStub stub;

    ScheduledExecutorService executor;

    @BeforeEach
    void beforeEach() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        requests.set(0);
        responses.clear();
        server =
                InProcessServerBuilder.forName(serverName)
                        .directExecutor()
                        .addService(serviceImpl)
                        .build()
                        .start();

        stub = new OxiaStub(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @AfterEach
    void afterEach() throws Exception {
        stub.close();
        server.shutdownNow();
        executor.shutdownNow();
    }

    @Test
    void start() {
        var assignments =
                ShardAssignments.newBuilder()
                        .putNamespaces(
                                DefaultNamespace,
                                NamespaceShardsAssignment.newBuilder().addAssignments(assignment(0, 0, 3)).build())
                        .build();
        responses.offer(new AssignmentWrapper(assignments, null, false));
        try (var shardManager =
                new ShardManager(executor, stub, InstrumentProvider.NOOP, DefaultNamespace)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.allShardIds()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
    }

    @Test
    void neverStarts() {
        try (var shardManager =
                new ShardManager(executor, stub, InstrumentProvider.NOOP, DefaultNamespace)) {
            assertThatThrownBy(() -> shardManager.start().get(1, SECONDS))
                    .isInstanceOf(TimeoutException.class);
            assertThat(shardManager.allShardIds()).isEmpty();
        }
    }

    @Test
    void update() {
        var assignments0 =
                ShardAssignments.newBuilder()
                        .putNamespaces(
                                DefaultNamespace,
                                NamespaceShardsAssignment.newBuilder().addAssignments(assignment(0, 0, 3)).build())
                        .build();
        var assignments1 =
                ShardAssignments.newBuilder()
                        .putNamespaces(
                                DefaultNamespace,
                                NamespaceShardsAssignment.newBuilder()
                                        .addAssignments(assignment(1, 0, 1))
                                        .addAssignments(assignment(2, 2, 3))
                                        .build())
                        .build();
        responses.offer(new AssignmentWrapper(assignments0, null, false));
        responses.offer(new AssignmentWrapper(assignments1, null, false));
        try (var shardManager =
                new ShardManager(executor, stub, InstrumentProvider.NOOP, DefaultNamespace)) {
            shardManager.start().join();
            await()
                    .untilAsserted(
                            () -> {
                                assertThat(shardManager.allShardIds()).containsExactlyInAnyOrder(1L, 2L);
                                assertThat(shardManager.leader(1)).isEqualTo("leader1");
                                assertThat(shardManager.leader(2)).isEqualTo("leader2");
                            });
        }
    }

    @Test
    public void recoveryFromError() {
        responses.offer(new AssignmentWrapper(null, Status.UNAVAILABLE.asException(), false));
        var assignments =
                ShardAssignments.newBuilder()
                        .putNamespaces(
                                DefaultNamespace,
                                NamespaceShardsAssignment.newBuilder().addAssignments(assignment(0, 0, 3)).build())
                        .build();
        responses.offer(new AssignmentWrapper(assignments, null, false));
        try (var shardManager =
                new ShardManager(executor, stub, InstrumentProvider.NOOP, DefaultNamespace)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.allShardIds()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
        assertThat(requests).hasValue(2);
    }

    @Test
    public void recoveryFromEndOfStream() {
        responses.offer(new AssignmentWrapper(null, null, true));
        var assignments =
                ShardAssignments.newBuilder()
                        .putNamespaces(
                                DefaultNamespace,
                                NamespaceShardsAssignment.newBuilder().addAssignments(assignment(0, 0, 3)).build())
                        .build();
        responses.offer(new AssignmentWrapper(assignments, null, false));
        try (var shardManager =
                new ShardManager(executor, stub, InstrumentProvider.NOOP, DefaultNamespace)) {
            assertThat(shardManager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(shardManager.allShardIds()).containsExactlyInAnyOrder(0L);
            assertThat(shardManager.leader(0)).isEqualTo("leader0");
        }
        assertThat(requests).hasValue(2);
    }

    ShardAssignment assignment(int shardId, int min, int max) {
        var hashRange =
                Int32HashRange.newBuilder().setMinHashInclusive(min).setMaxHashInclusive(max).build();
        return ShardAssignment.newBuilder()
                .setShard(shardId)
                .setLeader("leader" + shardId)
                .setInt32HashRange(hashRange)
                .build();
    }
}
