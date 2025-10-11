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
import static io.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.CompositeConsumer;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.proto.NamespaceShardsAssignment;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ShardManagerTest {

    @Nested
    @DisplayName("Tests of assignments data structure")
    class AssignmentsTests {

        @Test
        void recomputeShardHashBoundaries() {
            var existing =
                    Map.of(
                            1L, new Shard(1L, "leader 1", new HashRange(1, 3)),
                            2L, new Shard(2L, "leader 1", new HashRange(7, 9)),
                            3L, new Shard(3L, "leader 2", new HashRange(4, 6)),
                            4L, new Shard(4L, "leader 2", new HashRange(10, 11)),
                            5L, new Shard(5L, "leader 3", new HashRange(11, 12)),
                            6L, new Shard(6L, "leader 4", new HashRange(13, 13)));
            var updates =
                    Set.of(
                            new Shard(1L, "leader 4", new HashRange(1, 3)), // Leader change
                            new Shard(2L, "leader 4", new HashRange(7, 9)), //
                            new Shard(3L, "leader 2", new HashRange(4, 5)), // Split
                            new Shard(7L, "leader 3", new HashRange(6, 6)), //
                            new Shard(4L, "leader 2", new HashRange(10, 12)) // Merge
                            );
            var assignments = ShardManager.recomputeShardHashBoundaries(existing, updates);
            assertThat(assignments)
                    .satisfies(
                            a -> {
                                assertThat(a)
                                        .containsOnly(
                                                entry(1L, new Shard(1L, "leader 4", new HashRange(1, 3))),
                                                entry(2L, new Shard(2L, "leader 4", new HashRange(7, 9))),
                                                entry(3L, new Shard(3L, "leader 2", new HashRange(4, 5))),
                                                entry(7L, new Shard(7L, "leader 3", new HashRange(6, 6))),
                                                entry(4L, new Shard(4L, "leader 2", new HashRange(10, 12))),
                                                entry(6L, new Shard(6L, "leader 4", new HashRange(13, 13))));
                                assertThat(a).isUnmodifiable();
                            });
        }

        @Test
        void recomputeShardHashBoundariesWithSameValue() {
            var existing =
                    Map.of(
                            1L, new Shard(1L, "leader 1", new HashRange(1, 3)),
                            2L, new Shard(2L, "leader 1", new HashRange(7, 9)),
                            3L, new Shard(3L, "leader 2", new HashRange(4, 6)),
                            4L, new Shard(4L, "leader 2", new HashRange(10, 11)),
                            5L, new Shard(5L, "leader 3", new HashRange(11, 12)),
                            6L, new Shard(6L, "leader 4", new HashRange(13, 13)));
            var assignments =
                    ShardManager.recomputeShardHashBoundaries(
                            existing,
                            // same values
                            new HashSet<>(existing.values()));
            assertThat(assignments)
                    .satisfies(
                            a -> {
                                assertThat(a)
                                        .containsOnly(
                                                entry(1L, new Shard(1L, "leader 1", new HashRange(1, 3))),
                                                entry(2L, new Shard(2L, "leader 1", new HashRange(7, 9))),
                                                entry(3L, new Shard(3L, "leader 2", new HashRange(4, 6))),
                                                entry(4L, new Shard(4L, "leader 2", new HashRange(10, 11))),
                                                entry(5L, new Shard(5L, "leader 3", new HashRange(11, 12))),
                                                entry(6L, new Shard(6L, "leader 4", new HashRange(13, 13))));
                                assertThat(a).isUnmodifiable();
                            });
        }

        @Test
        void recomputeAssignmentsWithSameBoundariesAndDiffLeader() {
            final var existing = Map.of(1L, new Shard(1L, "leader 1", new HashRange(0, 4294967295L)));
            final var updates = Set.of(new Shard(1L, "leader 2", new HashRange(0, 4294967295L)));
            final var updatedMap = ShardManager.recomputeShardHashBoundaries(existing, updates);
            final var changes = ShardManager.computeShardLeaderChanges(existing, updatedMap);
            assertThat(changes.reassigned()).isEqualTo(updates);
        }
    }

    @Nested
    @DisplayName("Manager delegation")
    class ManagerTests {
        private final String namespace = "default";

        @Spy
        ShardAssignmentsContainer assignments =
                new ShardAssignmentsContainer(Xxh332HashRangeShardStrategy, DefaultNamespace);

        @Mock OxiaClientGrpc.OxiaClientStub async;

        @Mock OxiaStub stub;
        ShardManager manager;
        ScheduledExecutorService executor;

        @BeforeEach
        void mocking() {
            executor = Executors.newSingleThreadScheduledExecutor();
            stub = mock(OxiaStub.class);
            async = mock(OxiaClientGrpc.OxiaClientStub.class);

            manager =
                    new ShardManager(
                            executor, stub, assignments, new CompositeConsumer<>(), InstrumentProvider.NOOP);
        }

        @AfterEach
        void cleanup() {
            executor.shutdownNow();
        }

        @Test
        void start() {
            var assignment = ShardAssignment.newBuilder().setShard(0).setLeader("leader0").build();
            var nsAssignment = NamespaceShardsAssignment.newBuilder().addAssignments(assignment).build();
            when(stub.async()).thenReturn(async);

            doAnswer(
                            invocation -> {
                                manager.onNext(
                                        ShardAssignments.newBuilder().putNamespaces(namespace, nsAssignment).build());
                                return null;
                            })
                    .when(async)
                    .getShardAssignments(
                            ShardAssignmentsRequest.newBuilder().setNamespace(namespace).build(), manager);

            var future = manager.start();
            assertThat(future).succeedsWithin(Duration.ofSeconds(1));

            assertThat(manager.leader(0)).isEqualTo("leader0");
        }

        @Test
        void get() {
            assertThatThrownBy(() -> manager.getShardForKey("a"))
                    .isInstanceOf(NoShardAvailableException.class);
        }

        @Test
        void getAll() {
            manager.allShardIds();
            verify(assignments).allShardIds();
        }

        @Test
        void leader() {
            assertThatThrownBy(() -> manager.leader(1)).isInstanceOf(NoShardAvailableException.class);
        }
    }
}
