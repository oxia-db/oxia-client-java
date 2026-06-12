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

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static io.oxia.proto.OxiaClientGrpc.OxiaClientImplBase;
import static io.oxia.proto.Status.KEY_NOT_FOUND;
import static io.oxia.proto.Status.OK;
import static io.oxia.proto.Status.UNEXPECTED_VERSION_ID;
import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
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
import io.oxia.client.grpc.ManagedWriteStream;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.Session;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.GetResponse;
import io.oxia.proto.KeyComparisonType;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import io.oxia.proto.WriteResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
    RpcProvider clientByShardId;
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
                    10,
                    1024 * 1024,
                    256L * 1024 * 1024,
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
                                public void read(
                                        ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
                                    readResponses.forEach(c -> c.accept(responseObserver));
                                }
                            }));

    private Server server;
    private ManagedChannel channel;
    private final List<Consumer<StreamObserver<ReadResponse>>> readResponses = new ArrayList<>();

    @BeforeEach
    public void setUp() throws Exception {
        readResponses.clear();
        String serverName = InProcessServerBuilder.generateName();
        InProcessServerBuilder serverBuilder =
                InProcessServerBuilder.forName(serverName).directExecutor().addService(serviceImpl);
        if (serverInterceptor != null) {
            serverBuilder.intercept(serverInterceptor);
        }
        server = serverBuilder.build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        clientByShardId = mock(RpcProvider.class);
        lenient()
                .doAnswer(
                        invocation -> {
                            StreamObserver<ReadResponse> observer = invocation.getArgument(1);
                            stub()
                                    .read(
                                            invocation.getArgument(0),
                                            new StreamObserver<>() {
                                                @Override
                                                public void onNext(ReadResponse response) {
                                                    observer.onNext(response);
                                                }

                                                @Override
                                                public void onError(Throwable error) {
                                                    observer.onError(OxiaStatusException.from(error));
                                                }

                                                @Override
                                                public void onCompleted() {
                                                    observer.onCompleted();
                                                }
                                            });
                            return null;
                        })
                .when(clientByShardId)
                .read(any(ReadRequest.class), any(StreamObserver.class));
    }

    private OxiaClientGrpc.OxiaClientStub stub() {
        var stub = OxiaClientGrpc.newStub(channel);
        if (authentication == null) {
            return stub;
        }
        return stub.withCallCredentials(
                new CallCredentials() {
                    @Override
                    public void applyRequestMetadata(
                            RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
                        Metadata metadata = new Metadata();
                        authentication
                                .generateCredentials()
                                .forEach(
                                        (key, value) ->
                                                metadata.put(
                                                        Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value));
                        applier.apply(metadata);
                    }
                });
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Nested
    @DisplayName("Tests of write batch")
    class WriteBatchTests {
        WriteBatch batch;
        ManagedWriteStream writeStream;
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
                        Collections.emptyList(),
                        OptionalLong.empty(),
                        OptionalLong.empty());
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
                        Collections.emptyList(),
                        OptionalLong.empty(),
                        OptionalLong.empty());
        DeleteOperation delete = new DeleteOperation(deleteCallable, "", OptionalLong.of(1));
        DeleteRangeOperation deleteRange = new DeleteRangeOperation(deleteRangeCallable, "a", "b");

        @BeforeEach
        void setup() {
            writeStream = mock(ManagedWriteStream.class);
            lenient().when(clientByShardId.getWriteStream(shardId)).thenReturn(writeStream);

            var factory =
                    new WriteBatchFactory(
                            mock(RpcProvider.class), mock(SessionManager.class), config, InstrumentProvider.NOOP);
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
        public void sizeOfMatchesEncodedLength() {
            // One, two, three and four byte UTF-8 sequences
            String key = "a-é-世-🚀";
            int keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
            byte[] value = new byte[10];

            var sizedPut =
                    new PutOperation(
                            new CompletableFuture<>(),
                            key,
                            Optional.empty(),
                            Optional.empty(),
                            value,
                            OptionalLong.empty(),
                            OptionalLong.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            assertThat(batch.sizeOf(sizedPut)).isEqualTo(keyBytes + value.length);

            var sizedDelete = new DeleteOperation(new CompletableFuture<>(), key, OptionalLong.empty());
            assertThat(batch.sizeOf(sizedDelete)).isEqualTo(keyBytes);

            var sizedDeleteRange = new DeleteRangeOperation(new CompletableFuture<>(), key, key);
            assertThat(batch.sizeOf(sizedDeleteRange)).isEqualTo(2 * keyBytes);
        }

        @Test
        public void toProto() {
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);
            var request = batch.toProto();
            var expectedPut = new io.oxia.proto.PutRequest();
            put.toProto(expectedPut);
            var expectedDelete = new io.oxia.proto.DeleteRequest();
            delete.toProto(expectedDelete);
            var expectedDeleteRange = new io.oxia.proto.DeleteRangeRequest();
            deleteRange.toProto(expectedDeleteRange);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getPutsCount()).isEqualTo(1);
                                assertThat(r.getPutAt(0).toByteArray()).isEqualTo(expectedPut.toByteArray());
                                assertThat(r.getDeletesCount()).isEqualTo(1);
                                assertThat(r.getDeleteAt(0).toByteArray()).isEqualTo(expectedDelete.toByteArray());
                                assertThat(r.getDeleteRangesCount()).isEqualTo(1);
                                assertThat(r.getDeleteRangeAt(0).toByteArray())
                                        .isEqualTo(expectedDeleteRange.toByteArray());
                            });
        }

        @Test
        public void sendOk() {
            var resp = new WriteResponse();
            resp.addPut().setStatus(UNEXPECTED_VERSION_ID);
            resp.addPut().setStatus(OK).setVersion();
            resp.addDelete().setStatus(KEY_NOT_FOUND);
            resp.addDeleteRange().setStatus(OK);
            when(writeStream.send(any())).thenReturn(CompletableFuture.completedFuture(resp));

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await().untilAsserted(() -> assertThat(putCallable).isCompletedExceptionally());
            assertThat(putEphemeralCallable).isCompleted();
            assertThatThrownBy(putCallable::get)
                    .hasCauseExactlyInstanceOf(UnexpectedVersionIdException.class);
            assertThat(deleteCallable).isCompletedWithValueMatching(r -> !r);
            assertThat(deleteRangeCallable).isCompleted();
        }

        @Test
        public void sendFail() {
            var batchError = Status.UNAVAILABLE.asRuntimeException();
            when(writeStream.send(any())).thenReturn(CompletableFuture.failedFuture(batchError));

            batch.add(put);
            batch.add(putEphemeral);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await().untilAsserted(() -> assertThat(putCallable).isCompletedExceptionally());
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
            var rpcProvider = mock(RpcProvider.class);
            when(rpcProvider.getWriteStream(shardId))
                    .thenThrow(OxiaStatusException.shardNotFound(shardId));

            batch =
                    new WriteBatch(
                            new WriteBatchFactory(
                                    mock(RpcProvider.class),
                                    mock(SessionManager.class),
                                    config,
                                    InstrumentProvider.NOOP),
                            rpcProvider,
                            sessionManager,
                            shardId,
                            1024 * 1024);
            batch.add(put);
            batch.add(delete);
            batch.add(deleteRange);

            batch.send();

            Awaitility.await().untilAsserted(() -> assertThat(putCallable).isCompletedExceptionally());
            assertThatThrownBy(putCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(OxiaStatusException.class);
                                var oxiaError = (OxiaStatusException) e.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", Long.toString(shardId));
                            });
            assertThat(deleteCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(OxiaStatusException.class);
                                var oxiaError = (OxiaStatusException) e.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", Long.toString(shardId));
                            });
            assertThat(deleteRangeCallable).isCompletedExceptionally();
            assertThatThrownBy(deleteRangeCallable::get)
                    .satisfies(
                            e -> {
                                assertThat(e).hasCauseExactlyInstanceOf(OxiaStatusException.class);
                                var oxiaError = (OxiaStatusException) e.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", Long.toString(shardId));
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
            var factory = new ReadBatchFactory(mock(RpcProvider.class), config, InstrumentProvider.NOOP);
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
                                assertThat(r.getGetsCount()).isEqualTo(1);
                                assertThat(r.getGetAt(0).toByteArray()).isEqualTo(get.toProto().toByteArray());
                            });
        }

        @Test
        public void sendOk() {
            var getResponse = new GetResponse();
            getResponse.setStatus(KEY_NOT_FOUND);
            readResponses.add(
                    o -> {
                        var resp = new ReadResponse();
                        resp.addGet().copyFrom(getResponse);
                        o.onNext(resp);
                    });
            readResponses.add(StreamObserver::onCompleted);

            batch.add(get);
            batch.send();

            assertThat(getCallable).isCompletedWithValueMatching(Objects::isNull);
        }

        @Test
        public void sendFail() {
            var batchError = Status.UNAVAILABLE.asRuntimeException();
            readResponses.add(o -> o.onError(batchError));

            batch.add(get);
            batch.send();

            assertThat(getCallable).isCompletedExceptionally();
            assertThatThrownBy(getCallable::get).hasCauseInstanceOf(OxiaStatusException.class);
        }

        @Test
        public void sendFailNoClient() {
            var rpcProvider = mock(RpcProvider.class);
            doThrow(OxiaStatusException.shardNotFound(1))
                    .when(rpcProvider)
                    .read(any(ReadRequest.class), any(StreamObserver.class));
            batch =
                    new ReadBatch(
                            new ReadBatchFactory(mock(RpcProvider.class), config, InstrumentProvider.NOOP),
                            rpcProvider,
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
                                assertThat(e).hasCauseExactlyInstanceOf(OxiaStatusException.class);
                                var oxiaError = (OxiaStatusException) e.getCause();
                                assertThat(oxiaError.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
                                assertThat(oxiaError.getMetadata()).containsEntry("shard", Long.toString(shardId));
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
                        1,
                        1024 * 1024,
                        256L * 1024 * 1024,
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
