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
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import io.grpc.Status;
import io.oxia.client.KeyOrder;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
import io.oxia.proto.KeySorting;
import io.oxia.proto.NamespaceShardsAssignment;
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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

        @Mock RpcProvider rpcProvider;
        ShardManager manager;
        ScheduledExecutorService asyncExecutor;

        @BeforeEach
        void mocking() {
            asyncExecutor = Executors.newSingleThreadScheduledExecutor();

            manager =
                    new ShardManager(asyncExecutor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);
        }

        @AfterEach
        void cleanup() {
            asyncExecutor.shutdownNow();
        }

        @Test
        void start() {
            var assignment = new ShardAssignment();
            assignment.setShard(0).setLeader("leader0");
            assignment.setInt32HashRange().setMinHashInclusive(0).setMaxHashInclusive(Integer.MAX_VALUE);

            doAnswer(
                            invocation -> {
                                var sa = new ShardAssignments();
                                sa.putNamespaces(namespace).addAssignment().copyFrom(assignment);
                                manager.onNext(sa);
                                return null;
                            })
                    .when(rpcProvider)
                    .getShardAssignments(any(ShardAssignmentsRequest.class), eq(manager));

            var future = manager.start();
            assertThat(future).succeedsWithin(Duration.ofSeconds(1));

            assertThat(manager.leader(0)).isEqualTo("leader0");
        }

        @Test
        void oldServerAssignmentsKeepLegacyKeyOrder() {
            assertThat(manager.getKeyComparator().compare("b/x", "a/y/z")).isPositive();

            var sa = new ShardAssignments();
            sa.putNamespaces(namespace);
            manager.onNext(sa);
            assertThat(manager.getKeyComparator()).isSameAs(KeyOrder.comparator(null));

            sa.getNamespaces(namespace).setKeySorting(KeySorting.KEY_SORTING_UNKNOWN);
            manager.onNext(sa);
            assertThat(manager.getKeyComparator()).isSameAs(KeyOrder.comparator(null));
        }

        @Test
        void advertisedSortingIsVisibleBeforeCallbacksAndInitializationCompletes() {
            var sa = new ShardAssignments();
            var ns = sa.putNamespaces(namespace);
            ns.setKeySorting(KeySorting.KEY_SORTING_HIERARCHICAL);
            var assignment = ns.addAssignment().setShard(0).setLeader("leader0");
            assignment.setInt32HashRange().setMinHashInclusive(0).setMaxHashInclusive(Integer.MAX_VALUE);
            manager.addCallback(
                    changes -> {
                        assertThat(manager.getKeyComparator().compare("b/x", "a/y/z")).isNegative();
                        assertThat(manager.leader(0)).isEqualTo("leader0");
                    });

            doAnswer(
                            invocation -> {
                                manager.onNext(sa);
                                return null;
                            })
                    .when(rpcProvider)
                    .getShardAssignments(any(ShardAssignmentsRequest.class), eq(manager));

            assertThat(manager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(manager.getKeyComparator())
                    .isSameAs(KeyOrder.comparator(KeySorting.KEY_SORTING_HIERARCHICAL));
        }

        @Test
        void comparatorUpdatesEvenWhenShardAssignmentsDoNotChange() {
            var callbackCount = new AtomicInteger();
            manager.addCallback(changes -> callbackCount.incrementAndGet());
            var sa = new ShardAssignments();
            var ns = sa.putNamespaces(namespace);
            ns.setKeySorting(KeySorting.KEY_SORTING_NATURAL);
            var assignment = ns.addAssignment().setShard(0).setLeader("leader0");
            assignment.setInt32HashRange().setMinHashInclusive(0).setMaxHashInclusive(Integer.MAX_VALUE);

            manager.onNext(sa);
            var naturalSnapshot = manager.getKeyComparator();
            assertThat(naturalSnapshot.compare("da//z", "da1")).isNegative();
            assertThat(manager.leader(0)).isEqualTo("leader0");
            assertThat(callbackCount).hasValue(1);

            // Only namespace metadata changes; the shard id, leader and hash range stay the same.
            ns.setKeySorting(KeySorting.KEY_SORTING_HIERARCHICAL);
            manager.onNext(sa);
            assertThat(manager.getKeyComparator().compare("da//z", "da1")).isPositive();
            assertThat(naturalSnapshot.compare("da//z", "da1")).isNegative();
            assertThat(manager.leader(0)).isEqualTo("leader0");
            assertThat(callbackCount).hasValue(1);

            ns.clearKeySorting();
            manager.onNext(sa);
            assertThat(manager.getKeyComparator()).isSameAs(KeyOrder.comparator(null));
            assertThat(manager.leader(0)).isEqualTo("leader0");
            assertThat(callbackCount).hasValue(1);
        }

        @Test
        void sortingFieldUsesServerWireNumbersAndUnknownValuesFallBack() {
            var ns = new NamespaceShardsAssignment();
            ns.parseFrom(new byte[] {0x18, 0x01});
            assertThat(ns.getKeySorting()).isEqualTo(KeySorting.KEY_SORTING_NATURAL);
            assertThat(ns.toByteArray()).containsExactly((byte) 0x18, (byte) 0x01);

            ns.clear().parseFrom(new byte[] {0x18, 0x02});
            assertThat(ns.getKeySorting()).isEqualTo(KeySorting.KEY_SORTING_HIERARCHICAL);

            var sa = new ShardAssignments();
            sa.putNamespaces(namespace).parseFrom(new byte[] {0x18, 0x7f});
            manager.onNext(sa);
            assertThat(manager.getKeyComparator()).isSameAs(KeyOrder.comparator(null));
        }

        @Test
        void refetchesStubWhenRetryingShardAssignmentStream() {
            var stubCalls = new AtomicInteger();
            manager =
                    new ShardManager(asyncExecutor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);

            var assignment = new ShardAssignment();
            assignment.setShard(0).setLeader("leader0");
            assignment.setInt32HashRange().setMinHashInclusive(0).setMaxHashInclusive(Integer.MAX_VALUE);

            doAnswer(
                            invocation -> {
                                if (stubCalls.getAndIncrement() == 0) {
                                    manager.onError(Status.UNAVAILABLE.asException());
                                } else {
                                    var sa = new ShardAssignments();
                                    sa.putNamespaces(namespace).addAssignment().copyFrom(assignment);
                                    manager.onNext(sa);
                                }
                                return null;
                            })
                    .when(rpcProvider)
                    .getShardAssignments(any(ShardAssignmentsRequest.class), eq(manager));

            assertThat(manager.start()).succeedsWithin(Duration.ofSeconds(1));

            verify(rpcProvider, org.mockito.Mockito.times(2))
                    .getShardAssignments(any(ShardAssignmentsRequest.class), eq(manager));
            assertThat(stubCalls).hasValue(2);
        }

        @Test
        void callbacksOnlyReceiveChangedAssignments() {
            var received = new ArrayList<ShardAssignmentChanges>();
            manager.addCallback(received::add);
            Function<String, ShardAssignments> assignments =
                    leader -> {
                        var sa = new ShardAssignments();
                        var assignment = sa.putNamespaces(namespace).addAssignment();
                        assignment.setShard(0).setLeader(leader);
                        assignment
                                .setInt32HashRange()
                                .setMinHashInclusive(0)
                                .setMaxHashInclusive(Integer.MAX_VALUE);
                        return sa;
                    };

            manager.onNext(assignments.apply("leader0"));
            manager.onNext(assignments.apply("leader0"));
            manager.onNext(assignments.apply("leader1"));

            assertThat(received).hasSize(2);
            assertThat(received.get(0).added()).extracting(Shard::id).containsExactly(0L);
            assertThat(received.get(1).reassigned()).extracting(Shard::leader).containsExactly("leader1");
            assertThat(manager.leader(0)).isEqualTo("leader1");
        }

        @Test
        void get() {
            assertThatThrownBy(() -> manager.getShardForKey("a"))
                    .isInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error;
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("key", "a");
                            });
        }

        @Test
        void getAll() {
            assertThat(manager.allShardIds()).isEmpty();
        }

        @Test
        void leader() {
            assertThatThrownBy(() -> manager.leader(1))
                    .isInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error;
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", "1");
                            });
        }

        @Test
        void leaderUnavailableWhenAssignmentLeaderIsEmpty() {
            var assignment = new ShardAssignment();
            assignment.setShard(0).setLeader("");
            assignment.setInt32HashRange().setMinHashInclusive(0).setMaxHashInclusive(Integer.MAX_VALUE);

            doAnswer(
                            invocation -> {
                                var sa = new ShardAssignments();
                                sa.putNamespaces(namespace).addAssignment().copyFrom(assignment);
                                manager.onNext(sa);
                                return null;
                            })
                    .when(rpcProvider)
                    .getShardAssignments(any(ShardAssignmentsRequest.class), eq(manager));

            assertThat(manager.start()).succeedsWithin(Duration.ofSeconds(1));
            assertThat(manager.allShardIds()).containsExactly(0L);
            assertThatThrownBy(() -> manager.leader(0))
                    .isInstanceOf(OxiaStatusException.class)
                    .satisfies(
                            error -> {
                                var oxiaError = (OxiaStatusException) error;
                                assertThat(oxiaError.getStatusCode())
                                        .isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", "0");
                                assertThat(oxiaError.isRetryable()).isTrue();
                            });
        }
    }
}
