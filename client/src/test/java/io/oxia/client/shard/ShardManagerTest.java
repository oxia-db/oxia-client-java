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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.grpc.Status;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.shard.ShardManager.ShardAssignmentChanges;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
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
        void currentSuccessorsOfShardsSplitInTurn() {
            long max = (1L << 32) - 1;
            manager.onNext(assignments(new Shard(0, "leader0", new HashRange(0, max))));
            // Shard 0 is split into shards 1 and 2, then shard 2 into shards 3 and 4
            var shard1 = new Shard(1, "leader0", new HashRange(0, (1L << 31) - 1));
            manager.onNext(assignments(shard1, new Shard(2, "leader0", new HashRange(1L << 31, max))));
            manager.onNext(
                    assignments(
                            shard1,
                            new Shard(3, "leader0", new HashRange(1L << 31, (3L << 30) - 1)),
                            new Shard(4, "leader0", new HashRange(3L << 30, max))));

            assertThat(manager.getSuccessors(0)).containsExactlyInAnyOrder(1L, 2L);
            assertThat(manager.currentSuccessors(0)).containsExactlyInAnyOrder(1L, 3L, 4L);
            assertThat(manager.currentSuccessors(2)).containsExactlyInAnyOrder(3L, 4L);
            assertThat(manager.currentSuccessors(1)).isEmpty();
        }

        private ShardAssignments assignments(Shard... shards) {
            var sa = new ShardAssignments();
            var nsAssignments = sa.putNamespaces(namespace);
            for (var shard : shards) {
                var assignment =
                        nsAssignments.addAssignment().setShard(shard.id()).setLeader(shard.leader());
                assignment
                        .setInt32HashRange()
                        .setMinHashInclusive((int) shard.hashRange().minInclusive())
                        .setMaxHashInclusive((int) shard.hashRange().maxInclusive());
            }
            return sa;
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

        @Test
        void rejectsInvalidArguments() {
            assertThatThrownBy(
                            () -> new ShardManager(null, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(
                            () ->
                                    new ShardManager(asyncExecutor, null, InstrumentProvider.NOOP, DefaultNamespace))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new ShardManager(asyncExecutor, rpcProvider, null, DefaultNamespace))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(
                            () -> new ShardManager(asyncExecutor, rpcProvider, InstrumentProvider.NOOP, null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(
                            () -> new ShardManager(asyncExecutor, rpcProvider, InstrumentProvider.NOOP, ""))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> manager.addCallback(null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> manager.removeCallback(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void routesKeysToTheAssignedShards() {
            var shard = new Shard(0, "leader0", new HashRange(0, (1L << 32) - 1));
            manager.onNext(assignments(shard));

            assertThat(manager.getShardForKey("a")).isZero();
            assertThat(manager.allShards()).containsExactly(shard);
        }

        @Test
        void failsAssignmentsWithoutTheNamespace() {
            var sa = new ShardAssignments();
            sa.putNamespaces("other");

            assertThatThrownBy(() -> manager.onNext(sa))
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.NAMESPACE_NOT_FOUND));
        }

        @Test
        void notifiesShardsRemovedByAWiderShard() {
            var received = new ArrayList<ShardAssignmentChanges>();
            manager.onNext(
                    assignments(
                            new Shard(0, "leader0", new HashRange(0, 10)),
                            new Shard(1, "leader0", new HashRange(11, 20))));
            manager.addCallback(received::add);

            manager.onNext(assignments(new Shard(0, "leader0", new HashRange(0, 20))));

            assertThat(received).hasSize(1);
            assertThat(received.get(0).added()).isEmpty();
            assertThat(received.get(0).reassigned()).isEmpty();
            assertThat(received.get(0).removed()).extracting(Shard::id).containsExactly(1L);
        }

        @Test
        void removedCallbackIsNoLongerNotified() {
            var received = new ArrayList<ShardAssignmentChanges>();
            Consumer<ShardAssignmentChanges> callback = received::add;
            manager.addCallback(callback);
            manager.removeCallback(callback);

            manager.onNext(assignments(new Shard(0, "leader0", new HashRange(0, 20))));

            assertThat(received).isEmpty();
        }

        @ParameterizedTest
        @EnumSource(
                value = Status.Code.class,
                names = {"CANCELLED", "DEADLINE_EXCEEDED"})
        void renewsRightAwayOnlyOnceAssigned(Status.Code code) {
            var executor = mock(ScheduledExecutorService.class);
            var shardManager =
                    new ShardManager(executor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);

            shardManager.onError(code.toStatus().asException());
            assertThat(scheduledRestartDelay(executor)).isBetween(100L, 120L);

            shardManager.onNext(assignments(new Shard(0, "leader0", new HashRange(0, 20))));
            shardManager.onError(code.toStatus().asException());
            assertThat(scheduledRestartDelay(executor)).isZero();
        }

        @Test
        void retriesNamespaceNotFoundOnceAssigned() {
            var executor = mock(ScheduledExecutorService.class);
            var shardManager =
                    new ShardManager(executor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);
            var started = shardManager.start();
            shardManager.onNext(assignments(new Shard(0, "leader0", new HashRange(0, 20))));

            shardManager.onError(OxiaStatusException.namespaceNotFound(DefaultNamespace));

            assertThat(started).isCompleted();
            assertThat(scheduledRestartDelay(executor)).isBetween(100L, 120L);
        }

        @Test
        void closedManagerDoesNotRestart() {
            var executor = mock(ScheduledExecutorService.class);
            var shardManager =
                    new ShardManager(executor, rpcProvider, InstrumentProvider.NOOP, DefaultNamespace);
            shardManager.onCompleted();
            var restart = ArgumentCaptor.forClass(Runnable.class);
            verify(executor).schedule(restart.capture(), any(Long.class), eq(TimeUnit.MILLISECONDS));

            shardManager.close();
            restart.getValue().run();
            shardManager.onError(Status.UNAVAILABLE.asException());
            shardManager.onCompleted();

            verifyNoInteractions(rpcProvider);
            verify(executor).schedule(any(Runnable.class), any(Long.class), any(TimeUnit.class));
        }

        private long scheduledRestartDelay(ScheduledExecutorService executor) {
            var delay = ArgumentCaptor.forClass(Long.class);
            verify(executor, atLeastOnce())
                    .schedule(any(Runnable.class), delay.capture(), eq(TimeUnit.MILLISECONDS));
            return delay.getValue();
        }
    }
}
