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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.SessionHeartbeat;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shards that replace a split shard inherit its sessions, with the ephemeral records of each
 * session in their hash range, and expire a session unless it is kept alive on them. The fake
 * server splits a shard like Oxia does: the shard map replaces it with its children.
 */
class ShardSplitSessionTest {

    private record ShardRange(long id, long min, long max) {}

    private record ShardSession(long shard, long sessionId) {}

    // Shard 0 is split into shards 1 and 2
    private static final ShardRange SHARD_0 = new ShardRange(0, 0, (1L << 32) - 1);
    private static final ShardRange SHARD_1 = new ShardRange(1, 0, (1L << 31) - 1);
    private static final ShardRange SHARD_2 = new ShardRange(2, 1L << 31, (1L << 32) - 1);

    private static final byte[] VALUE = "value".getBytes(UTF_8);

    private FakeOxia oxia;
    private Server server;
    private AsyncOxiaClient client;

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
    void keepsTheSessionOfASplitShardOnTheShardsThatReplacedIt() throws Exception {
        long sessionId = putEphemeral(keyIn(SHARD_1, "before"));
        assertThat(putEphemeral(keyIn(SHARD_2, "before"))).isEqualTo(sessionId);
        assertThat(oxia.createdSessions).containsExactly(SHARD_0.id());

        // Shard 0 is split into shards 1 and 2, which inherit its session
        oxia.setShards(SHARD_1, SHARD_2);

        // The client keeps the session alive on them...
        await()
                .untilAsserted(
                        () ->
                                assertThat(oxia.heartbeats)
                                        .contains(
                                                new ShardSession(SHARD_1.id(), sessionId),
                                                new ShardSession(SHARD_2.id(), sessionId)));
        // ...uses it for their new ephemeral records...
        assertThat(putEphemeral(keyIn(SHARD_1, "after"))).isEqualTo(sessionId);
        assertThat(putEphemeral(keyIn(SHARD_2, "after"))).isEqualTo(sessionId);
        assertThat(oxia.createdSessions).containsExactly(SHARD_0.id());

        // ...and closes it on them
        client.close();
        assertThat(oxia.closedSessions)
                .containsExactlyInAnyOrder(
                        new ShardSession(SHARD_1.id(), sessionId), new ShardSession(SHARD_2.id(), sessionId));
    }

    // Returns the id of the session of the record
    private long putEphemeral(String key) throws Exception {
        return client
                .put(key, VALUE, Set.of(PutOption.AsEphemeralRecord))
                .get(10, SECONDS)
                .version()
                .sessionId()
                .orElseThrow();
    }

    private static String keyIn(ShardRange shard, String prefix) {
        for (int i = 0; ; i++) {
            var key = prefix + "-" + i;
            long hash = LongHashFunction.xx3().hashBytes(key.getBytes(UTF_8)) & 0xFFFFFFFFL;
            if (shard.min() <= hash && hash <= shard.max()) {
                return key;
            }
        }
    }

    private static final class FakeOxia extends OxiaClientGrpc.OxiaClientImplBase {
        private volatile String leader;
        private final List<StreamObserver<ShardAssignments>> assignmentObservers = new ArrayList<>();
        private ShardAssignments assignments;

        // The shards the sessions were created on, and the sessions kept alive and closed
        private final List<Long> createdSessions = new CopyOnWriteArrayList<>();
        private final Set<ShardSession> heartbeats = ConcurrentHashMap.newKeySet();
        private final List<ShardSession> closedSessions = new CopyOnWriteArrayList<>();

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

        @Override
        public synchronized void getShardAssignments(
                ShardAssignmentsRequest request, StreamObserver<ShardAssignments> responseObserver) {
            assignmentObservers.add(responseObserver);
            responseObserver.onNext(assignments);
        }

        @Override
        public void createSession(
                CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
            createdSessions.add(request.getShard());
            // The id of a session is 10 + the id of the shard it is created on
            responseObserver.onNext(new CreateSessionResponse().setSessionId(10 + request.getShard()));
            responseObserver.onCompleted();
        }

        @Override
        public void keepAlive(
                SessionHeartbeat request, StreamObserver<KeepAliveResponse> responseObserver) {
            heartbeats.add(new ShardSession(request.getShard(), request.getSessionId()));
            responseObserver.onNext(new KeepAliveResponse());
            responseObserver.onCompleted();
        }

        @Override
        public void closeSession(
                CloseSessionRequest request, StreamObserver<CloseSessionResponse> responseObserver) {
            closedSessions.add(new ShardSession(request.getShard(), request.getSessionId()));
            responseObserver.onNext(new CloseSessionResponse());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<WriteRequest> writeStream(
                StreamObserver<WriteResponse> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(WriteRequest request) {
                    var response = new WriteResponse();
                    for (int i = 0; i < request.getPutsCount(); i++) {
                        var put = request.getPutAt(i);
                        var version = response.addPut().setStatus(io.oxia.proto.Status.OK).setVersion();
                        if (put.hasSessionId()) {
                            version.setSessionId(put.getSessionId()).setClientIdentity(put.getClientIdentity());
                        }
                    }
                    responseObserver.onNext(response);
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {}
            };
        }
    }
}
