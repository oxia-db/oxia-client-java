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

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SharedResources;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The retries of the shard assignments wait at most 1 s, whatever the connection backoff: a client
 * still starts when the minimum of its connection backoff is at or above that.
 */
class ConnectionBackoffStartTest {

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
                                        responseObserver.onNext(assignments());
                                    }
                                })
                        .build()
                        .start();
    }

    @AfterEach
    void cleanup() {
        server.shutdownNow();
    }

    @ParameterizedTest
    @CsvSource({"1, 30", "2, 60"})
    void standaloneClientStarts(long minDelaySeconds, long maxDelaySeconds) throws Exception {
        OxiaClientBuilder.create(address())
                .connectionBackoff(ofSeconds(minDelaySeconds), ofSeconds(maxDelaySeconds))
                .asyncClient()
                .get(10, SECONDS)
                .close();
    }

    @ParameterizedTest
    @CsvSource({"1, 30", "2, 60"})
    void clientOfSharedResourcesStarts(long minDelaySeconds, long maxDelaySeconds) throws Exception {
        // The shard assignments of a namespace of the pool are retried with the connection backoff of
        // the client that created it: the pool's own applies to its connections
        try (var shared = SharedResources.builder().build()) {
            OxiaClientBuilder.create(address())
                    .sharedResources(shared)
                    .connectionBackoff(ofSeconds(minDelaySeconds), ofSeconds(maxDelaySeconds))
                    .asyncClient()
                    .get(10, SECONDS)
                    .close();
        }
    }

    private String address() {
        return "localhost:" + server.getPort();
    }

    // A single shard, on the fake server
    private ShardAssignments assignments() {
        var assignments = new ShardAssignments();
        assignments
                .putNamespaces(OxiaClientBuilderImpl.DefaultNamespace)
                .addAssignment()
                .setShard(0)
                .setLeader(address())
                .setInt32HashRange()
                .setMinHashInclusive(0)
                .setMaxHashInclusive(0xFFFFFFFF);
        return assignments;
    }
}
