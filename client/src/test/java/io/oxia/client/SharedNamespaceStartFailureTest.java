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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SharedResources;
import io.oxia.client.shard.NamespaceNotFoundException;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A shared namespace whose start fails, e.g. because it doesn't exist, must not stay in the pool:
 * the clients waiting for it get the failure, and the next client tries it again.
 */
class SharedNamespaceStartFailureTest {

    private static final String NAMESPACE = "my-ns";

    // The shard-assignment streams opened by the clients, which the test answers
    private final BlockingQueue<StreamObserver<ShardAssignments>> assignmentStreams =
            new LinkedBlockingQueue<>();
    private Server server;
    private SharedResourcesImpl shared;

    @BeforeEach
    void setup() throws Exception {
        server =
                ServerBuilder.forPort(0)
                        .directExecutor()
                        .addService(
                                new OxiaClientGrpc.OxiaClientImplBase() {
                                    @Override
                                    public void getShardAssignments(
                                            ShardAssignmentsRequest request,
                                            StreamObserver<ShardAssignments> responseObserver) {
                                        assignmentStreams.add(responseObserver);
                                    }
                                })
                        .build()
                        .start();
        shared = (SharedResourcesImpl) SharedResources.builder().numWorkerThreads(1).build();
    }

    @AfterEach
    void cleanup() {
        shared.close();
        server.shutdownNow();
    }

    @Test
    void dropsTheNamespaceWhenItIsNotFound() throws Exception {
        // Two clients wait for the namespace, which doesn't exist
        var client1 = newClient();
        var client2 = newClient();
        assertThat(shared.namespaceCount()).isOne();
        nextAssignmentStream()
                .onError(
                        Status.NOT_FOUND.withDescription("oxia: namespace not found").asRuntimeException());

        // Both get the failure...
        for (var client : List.of(client1, client2)) {
            assertThatThrownBy(client::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(NamespaceNotFoundException.class);
        }
        // ...and the pool dropped the namespace and closed its RpcProvider: only the health checks of
        // the shared connections are still scheduled
        assertThat(shared.namespaceCount()).isZero();
        assertThat(scheduledTasks()).isEqualTo(shared.getConnectionCount());

        // Once the namespace is created, the next client gets it from the server
        var client3 = newClient();
        nextAssignmentStream().onNext(assignments());
        client3.get(10, SECONDS).close();
        assertThat(shared.namespaceCount()).isOne();
    }

    private CompletableFuture<AsyncOxiaClient> newClient() {
        return OxiaClientBuilder.create("localhost:" + server.getPort())
                .sharedResources(shared)
                .namespace(NAMESPACE)
                .asyncClient();
    }

    private StreamObserver<ShardAssignments> nextAssignmentStream() throws InterruptedException {
        var stream = assignmentStreams.poll(10, SECONDS);
        assertThat(stream).as("shard-assignment stream").isNotNull();
        return stream;
    }

    // A single shard, on the fake server
    private ShardAssignments assignments() {
        var assignments = new ShardAssignments();
        assignments
                .putNamespaces(NAMESPACE)
                .addAssignment()
                .setShard(0)
                .setLeader("localhost:" + server.getPort())
                .setInt32HashRange()
                .setMinHashInclusive(0)
                .setMaxHashInclusive(0xFFFFFFFF);
        return assignments;
    }

    // The tasks scheduled on the shared executor, leaving out the cancelled ones that it only drops
    // once they are due
    private long scheduledTasks() {
        return ((ScheduledThreadPoolExecutor) shared.executor())
                .getQueue().stream().filter(task -> !((Future<?>) task).isCancelled()).count();
    }
}
