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
package io.oxia.client.batch;

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static io.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import static io.oxia.proto.Status.KEY_NOT_FOUND;
import static io.oxia.proto.Status.OK;
import static io.oxia.proto.Status.UNEXPECTED_VERSION_ID;
import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.*;
import io.oxia.client.grpc.OxiaBackoffProvider;
import io.oxia.client.grpc.OxiaStub;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.grpc.WriteStreamWrapper;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.Session;
import io.oxia.client.session.SessionManager;
import io.oxia.client.shard.NoShardAvailableException;
import io.oxia.proto.DeleteRangeResponse;
import io.oxia.proto.DeleteResponse;
import io.oxia.proto.GetResponse;
import io.oxia.proto.KeyComparisonType;
import io.oxia.proto.PutResponse;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BatchTest {
    OxiaStubProvider clientByShardId;
    @Mock SessionManager sessionManager;
    @Mock Session session;
    long shardId = 1L;
    long sessionId = 1L;
    protected static volatile Authentication authentication;
    protected static volatile ServerInterceptor serverInterceptor;

    static ClientConfig config =
            new ClientConfig(
                    "address",
                    Duration.ofMillis(100),
                    Duration.ofMillis(1000),
                    10,
                    1024 * 1024,
                    Duration.ofMillis(1000),
                    "client_id",
                    null,
                    OxiaClientBuilderImpl.DefaultNamespace,
                    authentication,
                    authentication != null,
                    Duration.ofMillis(100),
                    Duration.ofSeconds(30),
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(5),
                    1);

    private final OxiaClientImplBase serviceImpl =
            mock(
                    OxiaClientImplBase.class,
                    delegatesTo(
                            new OxiaClientImplBase() {

                                @Override
                                public StreamObserver<WriteRequest> writeStream(
                                        StreamObserver<WriteResponse> responseObserver) {
                                    ForkJoinPool.commonPool()
                                            .submit(
                                                    () -> {
                                                        try {
                                                            Thread.sleep(1000);
                                                        } catch (InterruptedException e) {
                                                            throw new RuntimeException(e);
                                                        }
                                                        writeResponses.forEach(wr -> wr.accept(responseObserver));
                                                    });

                                    return new StreamObserver<WriteRequest>() {
                                        @Override
                                        public void onNext(WriteRequest value) {}

                                        @Override
                                        public void onError(Throwable t) {}

                                        @Override
                                        public void onCompleted() {}
                                    };
                                }

                                @Override
                                public void write(
                                        WriteRequest request, StreamObserver<WriteResponse> responseObserver) {
                                    writeResponses.forEach(c -> c.accept(responseObserver));
                                }

                                @Override
                                public void read(
                                        ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                                    readResponses.forEach(c -> c.accept(responseObserver));
                                }
                            }));

    private Server server;
    private OxiaStub stub;
    private final List<Consumer<StreamObserver<WriteResponse>>> writeResponses = new ArrayList<>();
    private final List<Consumer<StreamObserver<ReadResponse>>> readResponses = new ArrayList<>();

    @BeforeEach
    public void setUp() throws Exception {
        writeResponses.clear();
        String serverName = InProcessServerBuilder.generateName();
        InProcessServerBuilder serverBuilder =
                InProcessServerBuilder.forName(serverName).directExecutor().addService(serviceImpl);
        if (serverInterceptor != null) {
            serverBuilder.intercept(serverInterceptor);
        }
        server = serverBuilder.build().start();
        stub =
                new OxiaStub(
                        InProcessChannelBuilder.forName(serverName).directExecutor().build(),
                        authentication,
                        OxiaBackoffProvider.DEFAULT);
        final WriteStreamWrapper writeStreamWrapper = new WriteStreamWrapper(stub.async());
        clientByShardId = mock(OxiaStubProvider.class);
        lenient().when(clientByShardId.getStubForShard(anyLong())).thenReturn(stub);
        lenient()
                .when(clientByShardId.getWriteStreamForShard(anyLong()))
                .thenReturn(writeStreamWrapper);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdownNow();
        stub.close();
    }

    @Nested
    @DisplayName("Tests of write batch")
    class WriteBatchTests {
        WriteBatch batch;
        CompletableFuture<PutResult> putCallable = new CompletableFuture<>();
        CompletableFuture<PutResult> putEphemeralCallable = new CompletableFuture<>();
        CompletableFuture<Boolean> deleteCallable = new CompletableFuture<>();
        CompletableFuture<Void> deleteRangeCallable = new CompletableFuture<>();

        PutOperation put =
                new PutOperation(
                        putCallable,
                        "",
                        Optional.empty(),
                        Optional.empty(),
                        new byte[0],
                        OptionalLong.of(1),
                        OptionalLong.empty(),
                        Optional.empty(),
                        Collections.emptyList());
        PutOperation putEphemeral =
                new PutOperation(
                        putEphemeralCallable,
                        "",
                        Optional.empty(),
                        Optional.empty(),
                        new byte[0],
                        OptionalLong.of(1),
                        OptionalLong.of(1),
                        Optional.of("client-id"),
                        Collections.emptyList());
        DeleteOperation delete = new DeleteOperation(deleteCallable, "", OptionalLong.of(1));
        DeleteRangeOperation deleteRange = new DeleteRangeOperation(deleteRangeCallable, "a", "b");

        @BeforeEach
        void setup() {

            var factory =
                    new WriteBatchFactory(
                            mock(OxiaStubProvider.class),
                            mock(SessionManager.class),
                            config,
                            InstrumentProvider.NOOP);
            batch = new WriteBatch(factory, clientByShardId, sessionManager, shardId, 1024 * 1024);
        }

        @Test
        public void size() {
            batch.add(put);
            assertThat(batch.size()).isEqualTo(1);
            batch.add(delete);
            assertThat(batch.size()).isEqualTo(2);
            batch.add(deleteRange);
            assertThat(batch.size()).isEqualTo(3);
        }

        @Test
        public void add() {
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);
            assertThat(batch.puts).containsOnly(put);
            assertThat(batch.deletes).containsOnly(delete);
            assertThat(batch.deleteRanges).containsOnly(deleteRange);
        }

        @Test
        public void toProto() {
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);
            var request = batch.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getPutsList()).containsOnly(put.toProto());
                                assertThat(r.getDeletesList()).containsOnly(delete.toProto());
                                assertThat(r.getDeleteRangesList()).containsOnly(deleteRange.toProto());
                            });
        }

        @Test
        public void sendOk() {
            writeResponses.add(
                    o ->
                            o.onNext(
                                    WriteResponse.newBuilder()
                                            .addPuts(PutResponse.newBuilder().setStatus(UNEXPECTED_VERSION_ID).build())
                                            .addPuts(PutResponse.newBuilder().setStatus(OK).build())
                                            .addDeletes(DeleteResponse.newBuilder().setStatus(KEY_NOT_FOUND).build())
                                            .addDeleteRanges(DeleteRangeResponse.newBuilder().setStatus(OK).build())
                                            .build()));
            writeResponses.add(StreamObserver::onCompleted);

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await()
                    .untilAsserted(
                            () -> {
                                assertThat(putCallable).isCompletedExceptionally();
                            });

            assertThat(putEphemeralCallable).isCompleted();
            assertThatThrownBy(putCallable::get)
                    .hasCauseExactlyInstanceOf(UnexpectedVersionIdException.class);
            assertThat(deleteCallable).isCompletedWithValueMatching(r -> !r);
            assertThat(deleteRangeCallable).isCompleted();
        }

        @Test
        public void sendFail() {
            var batchError = new RuntimeException();
            writeResponses.add(o -> o.onError(batchError));

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await()
                    .untilAsserted(
                            () -> {
                                assertThat(putCallable).isCompletedExceptionally();
                            });

            assertThatThrownBy(putCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(putEphemeralCallable).isCompletedExceptionally();
            assertThatThrownBy(putEphemeralCallable::get)
                    .hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteRangeCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
        }

        @Test
        public void sendFailNoClient() {
            var stubProvider = mock(OxiaStubProvider.class);
            when(stubProvider.getWriteStreamForShard(anyLong()))
                    .thenThrow(new NoShardAvailableException(1));

            batch =
                    new WriteBatch(
                            new WriteBatchFactory(
                                    mock(OxiaStubProvider.class),
                                    mock(SessionManager.class),
                                    config,
                                    InstrumentProvider.NOOP),
                            stubProvider,
                            sessionManager,
                            shardId,
                            1024 * 1024);
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await()
                    .untilAsserted(
                            () -> {
                                assertThat(putCallable).isCompletedExceptionally();
                            });
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
        }

        @Test
        public void shardId() {
            assertThat(batch.getShardId()).isEqualTo(shardId);
        }
    }

    @Nested
    @DisplayName("Tests of read batch")
    class ReadBatchTests {
        ReadBatch batch;
        CompletableFuture<GetResult> getCallable = new CompletableFuture<>();
        GetOperation get =
                new GetOperation(
                        getCallable, "", new GetOptions(null, true, KeyComparisonType.EQUAL, null));

        @BeforeEach
        void setup() {
            var factory =
                    new ReadBatchFactory(mock(OxiaStubProvider.class), config, InstrumentProvider.NOOP);
            batch = new ReadBatch(factory, clientByShardId, shardId);
        }

        @Test
        public void size() {
            batch.add(get);
            assertThat(batch.size()).isEqualTo(1);
        }

        @Test
        public void add() {
            batch.add(get);
            assertThat(batch.gets).containsOnly(get);
        }

        @Test
        public void toProto() {
            batch.add(get);
            var request = batch.toProto();
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getGetsList()).containsOnly(get.toProto());
                            });
        }

        @Test
        public void sendOk() {
            var getResponse = GetResponse.newBuilder().setStatus(KEY_NOT_FOUND).build();
            readResponses.add(o -> o.onNext(ReadResponse.newBuilder().addGets(getResponse).build()));
            readResponses.add(StreamObserver::onCompleted);

            batch.add(get);
            batch.send();

            assertThat(getCallable).isCompletedWithValueMatching(Objects::isNull);
        }

        @Test
        public void sendFail() {
            var batchError = new RuntimeException();
            readResponses.add(o -> o.onError(batchError));

            batch.add(get);
            batch.send();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get).hasCauseInstanceOf(StatusRuntimeException.class);
        }

        @Test
        public void sendFailNoClient() {
            var stubProvider = mock(OxiaStubProvider.class);
            when(stubProvider.getStubForShard(anyLong())).thenThrow(new NoShardAvailableException(1));
            batch =
                    new ReadBatch(
                            new ReadBatchFactory(mock(OxiaStubProvider.class), config, InstrumentProvider.NOOP),
                            stubProvider,
                            shardId);

            batch.add(get);
            batch.send();

            Awaitility.await()
                    .untilAsserted(
                            () -> {
                                assertThat(getCallable).isCompletedExceptionally();
                            });
            assertThatThrownBy(getCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(NoShardAvailableException.class);
                                assertThat(((NoShardAvailableException) e.getCause()).getShardId())
                                        .isEqualTo(shardId);
                            });
        }

        @Test
        public void shardId() {
            assertThat(batch.getShardId()).isEqualTo(shardId);
        }
    }

    @Nested
    @DisplayName("Tests of write batch factory")
    class FactoryTests {
        ClientConfig config =
                new ClientConfig(
                        "address",
                        ZERO,
                        ZERO,
                        1,
                        1024 * 1024,
                        ZERO,
                        "client_id",
                        null,
                        DefaultNamespace,
                        null,
                        false,
                        Duration.ofMillis(100),
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(5),
                        1);

        @Nested
        @DisplayName("Tests of write batch factory")
        class WriteBatchFactoryTests {
            @Test
            void apply() {
                var batch =
                        new WriteBatchFactory(clientByShardId, sessionManager, config, InstrumentProvider.NOOP)
                                .getBatch(shardId);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }

        @Nested
        @DisplayName("Tests of read batch factory")
        class ReadBatchFactoryTests {
            @Test
            void apply() {
                var batch =
                        new ReadBatchFactory(clientByShardId, config, InstrumentProvider.NOOP)
                                .getBatch(shardId);
                assertThat(batch.getShardId()).isEqualTo(shardId);
            }
        }
    }
}
