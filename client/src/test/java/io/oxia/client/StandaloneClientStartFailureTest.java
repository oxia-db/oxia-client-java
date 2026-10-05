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

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.grpc.Attributes;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerTransportFilter;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.shard.NamespaceNotFoundException;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A standalone client whose start fails is never handed to the caller, who therefore can't close
 * it: it must release its connection and threads by itself.
 */
class StandaloneClientStartFailureTest {

    private final AtomicInteger openedConnections = new AtomicInteger();
    private final AtomicInteger closedConnections = new AtomicInteger();
    private final List<Thread> clientThreads = new CopyOnWriteArrayList<>();
    private volatile Set<Thread> threadsBefore;
    private Server server;

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
                                        // The client is fully built by now: record its threads
                                        clientThreads.addAll(oxiaThreadsStartedSince(threadsBefore));
                                        responseObserver.onError(
                                                Status.NOT_FOUND
                                                        .withDescription("oxia: namespace not found")
                                                        .asRuntimeException());
                                    }
                                })
                        .addTransportFilter(
                                new ServerTransportFilter() {
                                    @Override
                                    public Attributes transportReady(Attributes attributes) {
                                        openedConnections.incrementAndGet();
                                        return attributes;
                                    }

                                    @Override
                                    public void transportTerminated(Attributes attributes) {
                                        closedConnections.incrementAndGet();
                                    }
                                })
                        .build()
                        .start();
        threadsBefore = Thread.getAllStackTraces().keySet();
    }

    @AfterEach
    void cleanup() {
        server.shutdownNow();
    }

    @Test
    void releasesItsResourcesWhenTheNamespaceIsNotFound() {
        assertThatThrownBy(
                        () ->
                                OxiaClientBuilder.create("localhost:" + server.getPort())
                                        .namespace("my-ns-does-not-exist")
                                        .asyncClient()
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(NamespaceNotFoundException.class);

        // The client closed its connection...
        assertThat(openedConnections).hasPositiveValue();
        await().untilAsserted(() -> assertThat(closedConnections).hasValue(openedConnections.get()));
        // ...and stopped its threads: its executor's and its batchers'
        assertThat(clientThreads).isNotEmpty();
        await().untilAsserted(() -> assertThat(clientThreads).filteredOn(Thread::isAlive).isEmpty());
    }

    // The threads of the pools started since the snapshot, leaving out those of the pools that other
    // tests left running, which can still grow. Netty's DefaultThreadFactory names a thread
    // "<pool name>-<pool id>-<thread id>".
    private static List<Thread> oxiaThreadsStartedSince(Set<Thread> threadsBefore) {
        Set<String> poolsBefore = threadsBefore.stream().map(t -> pool(t.getName())).collect(toSet());
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("oxia-") && !poolsBefore.contains(pool(t.getName())))
                .toList();
    }

    private static String pool(String threadName) {
        return threadName.substring(0, Math.max(threadName.lastIndexOf('-'), 0));
    }
}
