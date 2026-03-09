/*
 * Copyright © 2022-2026 The Oxia Authors
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
package io.oxia.client.batch;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.OxiaStatus;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.grpc.WriteStreamWrapper;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.GetResponse;
import io.oxia.proto.KeyComparisonType;
import io.oxia.proto.LeaderHint;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.PutResponse;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests that verify retry behavior with leader hint extraction, simulating the scenario where the
 * server returns NODE_IS_NOT_LEADER with a LeaderHint pointing to the correct leader.
 */
@ExtendWith(MockitoExtension.class)
class RetryWithLeaderHintTest {

    private static final Metadata.Key<com.google.rpc.Status> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    static ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofSeconds(5),
                    Duration.ofMillis(1000),
                    10,
                    1024 * 1024,
                    Duration.ofMillis(1000),
                    "client_id",
                    null,
                    OxiaClientBuilderImpl.DefaultNamespace,
                    null,
                    false,
                    Duration.ofMillis(100),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(5),
                    1);

    long shardId = 1L;

    @Nested
    @DisplayName("Write retry with leader hint")
    class WriteRetryTests {

        @Mock SessionManager sessionManager;

        private Server leaderServer;
        private Server followerServer;
        private OxiaStub leaderStub;
        private OxiaStub followerStub;
        private AtomicInteger followerCallCount = new AtomicInteger(0);
        private AtomicInteger leaderCallCount = new AtomicInteger(0);

        @BeforeEach
        void setup() throws Exception {
            String leaderName = InProcessServerBuilder.generateName();
            String followerName = InProcessServerBuilder.generateName();

            // Leader server: responds successfully
            leaderServer =
                    InProcessServerBuilder.forName(leaderName)
                            .directExecutor()
                            .addService(
                                    new OxiaClientGrpc.OxiaClientImplBase() {
                                        @Override
                                        public StreamObserver<WriteRequest> writeStream(
                                                StreamObserver<WriteResponse> responseObserver) {
                                            return new StreamObserver<>() {
                                                @Override
                                                public void onNext(WriteRequest request) {
                                                    leaderCallCount.incrementAndGet();
                                                    responseObserver.onNext(
                                                            WriteResponse.newBuilder()
                                                                    .addPuts(
                                                                            PutResponse.newBuilder()
                                                                                    .setStatus(io.oxia.proto.Status.OK)
                                                                                    .build())
                                                                    .build());
                                                }

                                                @Override
                                                public void onError(Throwable t) {}

                                                @Override
                                                public void onCompleted() {
                                                    responseObserver.onCompleted();
                                                }
                                            };
                                        }
                                    })
                            .build()
                            .start();

            // Follower server: returns NODE_IS_NOT_LEADER with leader hint
            followerServer =
                    InProcessServerBuilder.forName(followerName)
                            .directExecutor()
                            .addService(
                                    new OxiaClientGrpc.OxiaClientImplBase() {
                                        @Override
                                        public StreamObserver<WriteRequest> writeStream(
                                                StreamObserver<WriteResponse> responseObserver) {
                                            return new StreamObserver<>() {
                                                @Override
                                                public void onNext(WriteRequest request) {
                                                    followerCallCount.incrementAndGet();
                                                    var hint =
                                                            LeaderHint.newBuilder()
                                                                    .setShard(shardId)
                                                                    .setLeaderAddress(leaderName)
                                                                    .build();
                                                    var rpcStatus =
                                                            com.google.rpc.Status.newBuilder()
                                                                    .setCode(OxiaStatus.NODE_IS_NOT_LEADER.code())
                                                                    .setMessage("node is not leader for shard " + shardId)
                                                                    .addDetails(Any.pack(hint))
                                                                    .build();
                                                    Metadata trailers = new Metadata();
                                                    trailers.put(STATUS_DETAILS_KEY, rpcStatus);
                                                    responseObserver.onError(
                                                            Status.UNKNOWN
                                                                    .withDescription("node is not leader for shard " + shardId)
                                                                    .asRuntimeException(trailers));
                                                }

                                                @Override
                                                public void onError(Throwable t) {}

                                                @Override
                                                public void onCompleted() {
                                                    responseObserver.onCompleted();
                                                }
                                            };
                                        }
                                    })
                            .build()
                            .start();

            leaderStub = new OxiaStub(InProcessChannelBuilder.forName(leaderName).build());
            followerStub = new OxiaStub(InProcessChannelBuilder.forName(followerName).build());
        }

        @AfterEach
        void tearDown() throws Exception {
            leaderServer.shutdownNow();
            followerServer.shutdownNow();
            leaderStub.close();
            followerStub.close();
        }

        @Test
        @DisplayName("Write retries to leader after receiving NOT_LEADER with hint")
        void writeRetriesWithLeaderHint() {
            // StubProvider that initially returns follower, then leader when hint is provided
            OxiaStubProvider stubProvider =
                    new OxiaStubProvider("default", null, null) {
                        @Override
                        public WriteStreamWrapper getWriteStreamForShard(long shardId, LeaderHint leaderHint) {
                            if (leaderHint != null && !leaderHint.getLeaderAddress().isEmpty()) {
                                return new WriteStreamWrapper(leaderStub.async());
                            }
                            return new WriteStreamWrapper(followerStub.async());
                        }
                    };

            var factory =
                    new WriteBatchFactory(stubProvider, sessionManager, config, InstrumentProvider.NOOP);
            var batch =
                    new WriteBatch(
                            factory, stubProvider, sessionManager, shardId, 1024 * 1024, config.requestTimeout());

            CompletableFuture<PutResult> putFuture = new CompletableFuture<>();
            var put =
                    new PutOperation(
                            putFuture,
                            "key1",
                            Optional.empty(),
                            Optional.empty(),
                            "value".getBytes(),
                            OptionalLong.empty(),
                            OptionalLong.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());

            batch.add(put);
            batch.send();

            assertThat(putFuture).isCompleted();
            assertThat(followerCallCount.get()).isEqualTo(1);
            assertThat(leaderCallCount.get()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Read retry with leader hint")
    class ReadRetryTests {

        private Server leaderServer;
        private Server followerServer;
        private OxiaStub leaderStub;
        private OxiaStub followerStub;
        private AtomicInteger followerCallCount = new AtomicInteger(0);
        private AtomicInteger leaderCallCount = new AtomicInteger(0);

        @BeforeEach
        void setup() throws Exception {
            String leaderName = InProcessServerBuilder.generateName();
            String followerName = InProcessServerBuilder.generateName();

            // Leader server: responds successfully
            leaderServer =
                    InProcessServerBuilder.forName(leaderName)
                            .directExecutor()
                            .addService(
                                    new OxiaClientGrpc.OxiaClientImplBase() {
                                        @Override
                                        public void read(
                                                ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                                            leaderCallCount.incrementAndGet();
                                            responseObserver.onNext(
                                                    ReadResponse.newBuilder()
                                                            .addGets(
                                                                    GetResponse.newBuilder()
                                                                            .setStatus(io.oxia.proto.Status.OK)
                                                                            .build())
                                                            .build());
                                            responseObserver.onCompleted();
                                        }
                                    })
                            .build()
                            .start();

            // Follower server: returns NODE_IS_NOT_LEADER with leader hint
            followerServer =
                    InProcessServerBuilder.forName(followerName)
                            .directExecutor()
                            .addService(
                                    new OxiaClientGrpc.OxiaClientImplBase() {
                                        @Override
                                        public void read(
                                                ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                                            followerCallCount.incrementAndGet();
                                            var hint =
                                                    LeaderHint.newBuilder()
                                                            .setShard(shardId)
                                                            .setLeaderAddress(leaderName)
                                                            .build();
                                            var rpcStatus =
                                                    com.google.rpc.Status.newBuilder()
                                                            .setCode(OxiaStatus.NODE_IS_NOT_LEADER.code())
                                                            .setMessage("node is not leader for shard " + shardId)
                                                            .addDetails(Any.pack(hint))
                                                            .build();
                                            Metadata trailers = new Metadata();
                                            trailers.put(STATUS_DETAILS_KEY, rpcStatus);
                                            responseObserver.onError(
                                                    Status.UNKNOWN
                                                            .withDescription("node is not leader for shard " + shardId)
                                                            .asRuntimeException(trailers));
                                        }
                                    })
                            .build()
                            .start();

            leaderStub = new OxiaStub(InProcessChannelBuilder.forName(leaderName).build());
            followerStub = new OxiaStub(InProcessChannelBuilder.forName(followerName).build());
        }

        @AfterEach
        void tearDown() throws Exception {
            leaderServer.shutdownNow();
            followerServer.shutdownNow();
            leaderStub.close();
            followerStub.close();
        }

        @Test
        @DisplayName("Read retries to leader after receiving NOT_LEADER with hint")
        void readRetriesWithLeaderHint() {
            // StubProvider that initially returns follower, then leader when hint is provided
            OxiaStubProvider stubProvider =
                    new OxiaStubProvider("default", null, null) {
                        @Override
                        public OxiaStub getStubForShard(long shardId, LeaderHint leaderHint) {
                            if (leaderHint != null && !leaderHint.getLeaderAddress().isEmpty()) {
                                return leaderStub;
                            }
                            return followerStub;
                        }
                    };

            var factory = new ReadBatchFactory(stubProvider, config, InstrumentProvider.NOOP);
            var batch = new ReadBatch(factory, stubProvider, shardId, config.requestTimeout());

            CompletableFuture<GetResult> getFuture = new CompletableFuture<>();
            var get =
                    new GetOperation(
                            getFuture, "key1", new GetOptions(null, true, KeyComparisonType.EQUAL, null));

            batch.add(get);
            batch.send();

            assertThat(getFuture).isCompleted();
            assertThat(followerCallCount.get()).isEqualTo(1);
            assertThat(leaderCallCount.get()).isEqualTo(1);
        }
    }
}
