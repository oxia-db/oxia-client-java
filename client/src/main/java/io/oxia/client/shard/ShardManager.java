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

import static com.google.common.base.Throwables.getRootCause;
import static io.oxia.client.shard.HashRangeShardStrategy.Xxh332HashRangeShardStrategy;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.CompositeConsumer;
import io.oxia.client.grpc.CustomStatusCode;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.util.Backoff;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardManager implements AutoCloseable, StreamObserver<ShardAssignments> {
    private final ScheduledExecutorService executor;
    private final OxiaStub stub;
    private final @NonNull ShardAssignmentsContainer assignments;
    private final @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks;

    private final Counter shardAssignmentsEvents;

    private final Backoff backoff = new Backoff();
    private volatile boolean closed;

    private final CompletableFuture<Void> initialAssignmentsFuture = new CompletableFuture<>();

    @VisibleForTesting
    ShardManager(
            @NonNull ScheduledExecutorService executor,
            @NonNull OxiaStub stub,
            @NonNull ShardAssignmentsContainer assignments,
            @NonNull CompositeConsumer<ShardAssignmentChanges> callbacks,
            @NonNull InstrumentProvider instrumentProvider) {
        this.stub = stub;
        this.executor = executor;
        this.assignments = assignments;
        this.callbacks = callbacks;

        this.shardAssignmentsEvents =
                instrumentProvider.newCounter(
                        "oxia.client.shard.assignments.count",
                        Unit.None,
                        "The total count of received shard assignment events",
                        Attributes.empty());
    }

    public ShardManager(
            ScheduledExecutorService executor,
            @NonNull OxiaStub stub,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull String namespace) {
        this(
                executor,
                stub,
                new ShardAssignmentsContainer(Xxh332HashRangeShardStrategy, namespace),
                new CompositeConsumer<>(),
                instrumentProvider);
    }

    @Override
    public void close() {
        closed = true;
    }

    public CompletableFuture<Void> start() {
        stub.watchShardAssignments(
                ShardAssignmentsRequest.newBuilder()
                        .setNamespace(assignments.getNamespace())
                        .build(), this);
        return initialAssignmentsFuture;
    }

    @Override
    public void onNext(ShardAssignments assignments) {
        shardAssignmentsEvents.increment();
        updateAssignments(assignments);
        backoff.reset();
        if (!initialAssignmentsFuture.isDone()) {
            initialAssignmentsFuture.complete(null);
        }
    }

    @Override
    public void onError(Throwable error) {
        if (closed) {
            return;
        }

        if (error instanceof StatusRuntimeException statusError) {
            var status = statusError.getStatus();
            if (status.getCode() == Status.Code.UNKNOWN) {
                // Suppress unknown errors
                final var description = status.getDescription();
                if (description != null) {
                    var customStatusCode = CustomStatusCode.fromDescription(description);
                    if (customStatusCode == CustomStatusCode.ErrorNamespaceNotFound) {
                        log.error("Namespace not found: {}", assignments.getNamespace());
                        if (!initialAssignmentsFuture.isDone()) {
                            if (initialAssignmentsFuture.completeExceptionally(
                                    new NamespaceNotFoundException(assignments.getNamespace()))) {
                                close();
                            }
                        }
                    }
                }
            }
        }
        log.warn("Failed receiving shard assignments.", getRootCause(error));
        executor.schedule(
                () -> {
                    if (!closed) {
                        log.info(
                                "Retry creating stream for shard assignments namespace={}",
                                assignments.getNamespace());
                        start();
                    }
                },
                backoff.nextDelayMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void onCompleted() {
        if (closed) {
            return;
        }

        log.warn("Stream closed while receiving shard assignments");
        executor.schedule(
                () -> {
                    if (!closed) {
                        log.info(
                                "Retry creating stream for shard assignments after stream closed namespace={}",
                                assignments.getNamespace());
                        start();
                    }
                },
                backoff.nextDelayMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void updateAssignments(io.oxia.proto.ShardAssignments shardAssignments) {
        var nsSharedAssignments = shardAssignments.getNamespacesMap().get(assignments.getNamespace());
        if (nsSharedAssignments == null) {
            /*
            It shouldn't happen, but we have to do some defensive programming to avoid server nodes
            Allowing namespaces to be deleted in the future.

            Retries are available so that the client does not panic until the namespace is recreated.
            */
            throw new NamespaceNotFoundException(assignments.getNamespace(), true);
        }
        var updates =
                nsSharedAssignments.getAssignmentsList().stream().map(Shard::fromProto).collect(toSet());
        var updatedMap = recomputeShardHashBoundaries(assignments.allShards(), updates);
        var changes = computeShardLeaderChanges(assignments.allShards(), updatedMap);
        assignments.update(changes);
        callbacks.accept(changes);
    }

    @VisibleForTesting
    static Map<Long, Shard> recomputeShardHashBoundaries(
            Map<Long, Shard> assignments, Set<Shard> updates) {
        var toDelete = new ArrayList<>();
        updates.forEach(
                update ->
                        update
                                .findOverlapping(assignments.values())
                                .forEach(
                                        existing -> {
                                            log.info("Deleting shard {} as it overlaps with {}", existing, update);
                                            toDelete.add(existing.id());
                                        }));

        return unmodifiableMap(
                Stream.concat(
                                assignments.entrySet().stream()
                                        .filter(e -> !toDelete.contains(e.getKey()))
                                        .map(Map.Entry::getValue),
                                updates.stream())
                        .collect(
                                toMap(
                                        Shard::id,
                                        identity(),
                                        // merge function to avoid throw any exception when receive unchanged events
                                        (existing, newValue) -> newValue)));
    }

    @VisibleForTesting
    static ShardAssignmentChanges computeShardLeaderChanges(
            Map<Long, Shard> oldAssignments, Map<Long, Shard> newAssignments) {
        Set<Shard> removed =
                oldAssignments.values().stream()
                        .filter(shard -> !newAssignments.containsKey(shard.id()))
                        .collect(toSet());
        Set<Shard> added =
                newAssignments.values().stream()
                        .filter(shard -> !oldAssignments.containsKey(shard.id()))
                        .collect(toSet());
        Set<Shard> changed =
                oldAssignments.values().stream()
                        .filter(s -> newAssignments.containsKey(s.id()))
                        .filter(s -> !newAssignments.get(s.id()).leader().equals(s.leader()))
                        .map(s -> newAssignments.get(s.id())) // return reassigned leader
                        .collect(toSet());
        return new ShardAssignmentChanges(
                unmodifiableSet(added), unmodifiableSet(removed), unmodifiableSet(changed));
    }

    public record ShardAssignmentChanges(
            Set<Shard> added, Set<Shard> removed, Set<Shard> reassigned) {
    }

    public long getShardForKey(String key) {
        return assignments.getShardForKey(key);
    }

    public Collection<Shard> allShards() {
        return assignments.allShards().values();
    }

    public Set<Long> allShardIds() {
        return assignments.allShardIds();
    }

    public String leader(long shardId) {
        return assignments.leader(shardId);
    }

    public void addCallback(@NonNull Consumer<ShardAssignmentChanges> callback) {
        callbacks.add(callback);
    }
}
