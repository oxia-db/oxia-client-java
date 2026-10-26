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

import static io.oxia.client.api.options.defs.OptionVersionId.KEY_NOT_EXISTS;
import static io.oxia.proto.Status.INVALID_ARGUMENT;
import static io.oxia.proto.Status.KEY_NOT_FOUND;
import static io.oxia.proto.Status.OK;
import static io.oxia.proto.Status.SESSION_DOES_NOT_EXIST;
import static io.oxia.proto.Status.UNEXPECTED_VERSION_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.exceptions.UnknownException;
import io.oxia.client.api.options.defs.OptionSecondaryIndex;
import io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import io.oxia.client.batch.Operation.WriteOperation.PutOperation;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.Session;
import io.oxia.proto.DeleteRangeRequest;
import io.oxia.proto.DeleteRangeResponse;
import io.oxia.proto.DeleteRequest;
import io.oxia.proto.DeleteResponse;
import io.oxia.proto.GetRequest;
import io.oxia.proto.GetResponse;
import io.oxia.proto.KeyComparisonType;
import io.oxia.proto.PutRequest;
import io.oxia.proto.PutResponse;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OperationTest {

    // The batch manager the operations are submitted to: these tests don't route them
    private static final BatchManager batchManager = mock(BatchManager.class);

    // A response with status 99, a number the client's Status enum doesn't define: field 1, a varint
    private static final byte[] UNKNOWN_STATUS = {0x08, 99};

    private static void assertFailedWithUnknown(CompletableFuture<?> callback, String message) {
        assertThat(callback).isCompletedExceptionally();
        assertThatThrownBy(callback::get)
                .satisfies(
                        e -> {
                            assertThat(e).isInstanceOf(ExecutionException.class);
                            assertThat(e.getCause()).isInstanceOf(UnknownException.class).hasMessage(message);
                        });
    }

    @Nested
    @DisplayName("Tests of get operation")
    class GetOperationTests {

        CompletableFuture<GetResult> callback = new CompletableFuture<>();
        GetOperation op =
                new GetOperation(
                        batchManager,
                        1L,
                        callback,
                        "key",
                        new GetOptions(null, true, KeyComparisonType.EQUAL, null));

        @Test
        void toProto() {
            var request = new GetRequest();
            op.toProto(request);
            assertThat(request.getKey()).isEqualTo(op.key());
        }

        @Test
        void completeKeyNotFound() {
            var response = new GetResponse();
            response.setStatus(KEY_NOT_FOUND);
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(Objects::isNull);
        }

        @Test
        void completeOk() {
            var payload = "hello".getBytes(UTF_8);
            var response = new GetResponse();
            response.setKey("my-key").setStatus(OK).setValue(payload);
            response
                    .setVersion()
                    .setVersionId(1L)
                    .setCreatedTimestamp(2L)
                    .setModifiedTimestamp(3L)
                    .setModificationsCount(4L);
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValue(
                            new GetResult(
                                    "my-key",
                                    payload,
                                    new io.oxia.client.api.Version(
                                            1L, 2L, 3L, 4L, Optional.empty(), Optional.empty())));
        }

        @Test
        void completeOkWithSecondaryIndexKey() {
            var response = new GetResponse();
            response.setKey("my-key").setSecondaryIndexKey("my-secondary-key").setStatus(OK);
            response.setVersion().setVersionId(1L);
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValueMatching(
                            r ->
                                    r.key().equals("my-key")
                                            && r.secondaryIndexKey().equals(Optional.of("my-secondary-key")));
        }

        @Test
        void completeOkEphemeral() {
            var payload = "hello".getBytes(UTF_8);
            var response = new GetResponse();
            response.setKey("my-key").setStatus(OK).setValue(payload);
            response
                    .setVersion()
                    .setVersionId(1L)
                    .setCreatedTimestamp(2L)
                    .setModifiedTimestamp(3L)
                    .setModificationsCount(4L)
                    .setSessionId(5L)
                    .setClientIdentity("client-id");
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValue(
                            new GetResult(
                                    "my-key",
                                    payload,
                                    new io.oxia.client.api.Version(
                                            1L, 2L, 3L, 4L, Optional.of(5L), Optional.of("client-id"))));
        }

        @Test
        void completeUnexpectedStatus() {
            var response = new GetResponse();
            response.setStatus(SESSION_DOES_NOT_EXIST);
            op.complete(response);
            assertFailedWithUnknown(callback, "GRPC.Status: SESSION_DOES_NOT_EXIST");
        }

        @Test
        void completeUnknownStatus() {
            var response = new GetResponse();
            response.parseFrom(UNKNOWN_STATUS);
            op.complete(response);
            assertFailedWithUnknown(callback, "GRPC.Status: UNRECOGNIZED(99)");
        }
    }

    @Nested
    @DisplayName("Tests of put operation")
    class PutOperationTests {
        CompletableFuture<PutResult> callback = new CompletableFuture<>();
        byte[] payload = "hello".getBytes(UTF_8);
        PutOperation op =
                new PutOperation(
                        batchManager,
                        1L,
                        callback,
                        "key",
                        Optional.empty(),
                        Optional.empty(),
                        payload,
                        OptionalLong.of(10),
                        Optional.empty(),
                        Optional.empty(),
                        Collections.emptyList(),
                        OptionalLong.empty(),
                        OptionalLong.empty());
        long sessionId = 0L;

        @Test
        void constructInvalidExpectedVersionId() {
            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    new PutOperation(
                                            batchManager,
                                            1L,
                                            callback,
                                            "key",
                                            Optional.empty(),
                                            Optional.empty(),
                                            payload,
                                            OptionalLong.of(KEY_NOT_EXISTS),
                                            Optional.empty(),
                                            Optional.empty(),
                                            Collections.emptyList(),
                                            OptionalLong.empty(),
                                            OptionalLong.empty()));
            assertThatNoException()
                    .isThrownBy(
                            () ->
                                    new PutOperation(
                                            batchManager,
                                            1L,
                                            callback,
                                            "key",
                                            Optional.empty(),
                                            Optional.empty(),
                                            payload,
                                            OptionalLong.of(0L),
                                            Optional.empty(),
                                            Optional.empty(),
                                            Collections.emptyList(),
                                            OptionalLong.empty(),
                                            OptionalLong.empty()));
            assertThatThrownBy(
                            () ->
                                    new PutOperation(
                                            batchManager,
                                            1L,
                                            callback,
                                            "key",
                                            Optional.empty(),
                                            Optional.empty(),
                                            payload,
                                            OptionalLong.of(-2L),
                                            Optional.empty(),
                                            Optional.empty(),
                                            Collections.emptyList(),
                                            OptionalLong.empty(),
                                            OptionalLong.empty()))
                    .isInstanceOf(OxiaStatusException.class)
                    .extracting("statusCode")
                    .isEqualTo(OxiaStatusCode.INVALID_ARGUMENT);
        }

        @Test
        void toProtoNoExpectedVersion() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue()).isEqualTo(op.value());
                                assertThat(r.hasExpectedVersionId()).isFalse();
                                assertThat(r.hasSessionId()).isFalse();
                                assertThat(r.hasClientIdentity()).isFalse();
                            });
        }

        @Test
        void toProtoExpectedVersion() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.of(1L),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue()).isEqualTo(op.value());
                                assertThat(r.getExpectedVersionId()).isEqualTo(1L);
                                assertThat(r.hasSessionId()).isFalse();
                                assertThat(r.hasClientIdentity()).isFalse();
                            });
        }

        @Test
        void toProtoPartitionKey() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.of("my-partition-key"),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getPartitionKey()).isEqualTo(op.partitionKey().get());
                                assertThat(r.getValue()).isEqualTo(op.value());
                                assertThat(r.hasSessionId()).isFalse();
                                assertThat(r.hasClientIdentity()).isFalse();
                            });
        }

        @Test
        void toProtoNoExistingVersion() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.of(KEY_NOT_EXISTS),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue()).isEqualTo(op.value());
                                assertThat(r.getExpectedVersionId()).isEqualTo(KEY_NOT_EXISTS);
                                assertThat(r.hasSessionId()).isFalse();
                                assertThat(r.hasClientIdentity()).isFalse();
                            });
        }

        @Test
        void toProtoEphemeral() {
            var session = mock(Session.class);
            when(session.getSessionId()).thenReturn(sessionId);
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.of(session),
                            Optional.of("client-id"),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getValue()).isEqualTo(op.value());
                                assertThat(r.hasExpectedVersionId()).isFalse();
                                assertThat(r.getSessionId()).isEqualTo(sessionId);
                                assertThat(r.getClientIdentity()).isEqualTo("client-id");
                            });
        }

        @Test
        void toProtoOverrideVersionMetadata() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.of(42L),
                            OptionalLong.of(7L));
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.hasOverrideVersionId()).isTrue();
                                assertThat(r.getOverrideVersionId()).isEqualTo(42L);
                                assertThat(r.hasOverrideModificationsCount()).isTrue();
                                assertThat(r.getOverrideModificationsCount()).isEqualTo(7L);
                            });
        }

        @Test
        void toProtoNoOverrideVersionMetadata() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.hasOverrideVersionId()).isFalse();
                                assertThat(r.hasOverrideModificationsCount()).isFalse();
                            });
        }

        @Test
        void completeUnexpectedVersion() {
            var response = new PutResponse();
            response.setStatus(UNEXPECTED_VERSION_ID);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(UnexpectedVersionIdException.class)
                                        .hasMessage("key 'key' has unexpected versionId (expected 10)");
                                assertThat(e.getCause().getStackTrace()).isEmpty();
                            });
            var stage = callback.whenComplete((r, t) -> {});
            assertThatThrownBy(stage::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(UnexpectedVersionIdException.class)
                    .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
        }

        @Test
        void completeKeyAlreadyExists() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.of(KEY_NOT_EXISTS),
                            Optional.empty(),
                            Optional.empty(),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var response = new PutResponse();
            response.setStatus(UNEXPECTED_VERSION_ID);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(KeyAlreadyExistsException.class)
                                        .hasMessage("key already exists: key");
                                assertThat(e.getCause().getStackTrace()).isEmpty();
                            });
            var stage = callback.whenComplete((r, t) -> {});
            assertThatThrownBy(stage::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(KeyAlreadyExistsException.class)
                    .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
        }

        @Test
        void completeSessionDoesNotExist() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "key",
                            Optional.empty(),
                            Optional.empty(),
                            payload,
                            OptionalLong.empty(),
                            Optional.of(mock(Session.class)),
                            Optional.of("client-id"),
                            Collections.emptyList(),
                            OptionalLong.empty(),
                            OptionalLong.empty());
            var response = new PutResponse();
            response.setStatus(SESSION_DOES_NOT_EXIST);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(SessionDoesNotExistException.class)
                                        .hasMessage("session does not exist");
                            });
        }

        @Test
        void completeInvalidArgument() {
            var response = new PutResponse();
            response.setStatus(INVALID_ARGUMENT);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(InvalidArgumentException.class)
                                        .hasMessage("put of key 'key' rejected for an invalid argument");
                            });
        }

        @Test
        void completeUnknownStatus() {
            var response = new PutResponse();
            response.parseFrom(UNKNOWN_STATUS);
            op.complete(response);
            assertFailedWithUnknown(callback, "GRPC.Status: UNRECOGNIZED(99)");
        }

        @Test
        void completeOk() {
            var response = new PutResponse();
            response.setStatus(OK).setKey("my-key");
            response
                    .setVersion()
                    .setVersionId(1L)
                    .setCreatedTimestamp(2L)
                    .setModifiedTimestamp(3L)
                    .setModificationsCount(4L);
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValue(
                            new PutResult(
                                    "my-key",
                                    new io.oxia.client.api.Version(
                                            1L, 2L, 3L, 4L, Optional.empty(), Optional.empty())));
        }

        @Test
        void completeEphemeral() {
            var response = new PutResponse();
            response.setStatus(OK).setKey("my-key");
            response
                    .setVersion()
                    .setVersionId(1L)
                    .setCreatedTimestamp(2L)
                    .setModifiedTimestamp(3L)
                    .setModificationsCount(4L)
                    .setSessionId(sessionId)
                    .setClientIdentity("client-id");
            op.complete(response);
            assertThat(callback)
                    .isCompletedWithValue(
                            new PutResult(
                                    "my-key",
                                    new io.oxia.client.api.Version(
                                            1L, 2L, 3L, 4L, Optional.of(sessionId), Optional.of("client-id"))));
        }
    }

    @Nested
    @DisplayName("Tests of delete operation")
    class DeleteOperationTests {
        CompletableFuture<Boolean> callback = new CompletableFuture<>();
        DeleteOperation op =
                new DeleteOperation(batchManager, 1L, callback, "key", OptionalLong.of(10L));

        @Test
        void constructInvalidExpectedVersionId() {
            assertThatNoException()
                    .isThrownBy(
                            () -> new DeleteOperation(batchManager, 1L, callback, "key", OptionalLong.of(0L)));
            assertThatThrownBy(
                            () ->
                                    new DeleteOperation(
                                            batchManager, 1L, callback, "key", OptionalLong.of(KEY_NOT_EXISTS)))
                    .isInstanceOf(OxiaStatusException.class)
                    .extracting("statusCode")
                    .isEqualTo(OxiaStatusCode.INVALID_ARGUMENT);
            assertThatThrownBy(
                            () -> new DeleteOperation(batchManager, 1L, callback, "key", OptionalLong.of(-2L)))
                    .isInstanceOf(OxiaStatusException.class)
                    .extracting("statusCode")
                    .isEqualTo(OxiaStatusCode.INVALID_ARGUMENT);
        }

        @Test
        void toProtoNoExpectedVersion() {
            var op = new DeleteOperation(batchManager, 1L, callback, "key");
            var request = new DeleteRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.hasExpectedVersionId()).isFalse();
                            });
        }

        @Test
        void toProtoExpectedVersion() {
            var request = new DeleteRequest();
            op.toProto(request);
            assertThat(request)
                    .satisfies(
                            r -> {
                                assertThat(r.getKey()).isEqualTo(op.key());
                                assertThat(r.getExpectedVersionId()).isEqualTo(10L);
                            });
        }

        @Test
        void completeUnexpectedVersion() {
            var response = new DeleteResponse();
            response.setStatus(UNEXPECTED_VERSION_ID);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(UnexpectedVersionIdException.class)
                                        .hasMessage("key 'key' has unexpected versionId (expected 10)");
                                assertThat(e.getCause().getStackTrace()).isEmpty();
                            });
            var stage = callback.whenComplete((r, t) -> {});
            assertThatThrownBy(stage::join)
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(UnexpectedVersionIdException.class)
                    .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
        }

        @Test
        void completeOk() {
            var response = new DeleteResponse();
            response.setStatus(OK);
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(r -> r);
        }

        @Test
        void completeKeyNotFound() {
            var response = new DeleteResponse();
            response.setStatus(KEY_NOT_FOUND);
            op.complete(response);
            assertThat(callback).isCompletedWithValueMatching(r -> !r);
        }

        @Test
        void completeUnknownStatus() {
            var response = new DeleteResponse();
            response.parseFrom(UNKNOWN_STATUS);
            op.complete(response);
            assertFailedWithUnknown(callback, "GRPC.Status: UNRECOGNIZED(99)");
        }
    }

    @Nested
    @DisplayName("Tests of delete range operation")
    class DeleteRangeOperationTests {
        CompletableFuture<Void> callback = new CompletableFuture<>();
        DeleteRangeOperation op = new DeleteRangeOperation(batchManager, 1L, callback, "a", "b");

        @Test
        void toProto() {
            var request = new DeleteRangeRequest();
            op.toProto(request);
            assertThat(request.getStartInclusive()).isEqualTo(op.startKeyInclusive());
            assertThat(request.getEndExclusive()).isEqualTo(op.endKeyExclusive());
        }

        @Test
        void completeOk() {
            var response = new DeleteRangeResponse();
            response.setStatus(OK);
            op.complete(response);
            assertThat(callback).isCompleted();
        }

        @Test
        void completeInvalidArgument() {
            var response = new DeleteRangeResponse();
            response.setStatus(INVALID_ARGUMENT);
            op.complete(response);
            assertThat(callback).isCompletedExceptionally();
            assertThatThrownBy(callback::get)
                    .satisfies(
                            e -> {
                                assertThat(e).isInstanceOf(ExecutionException.class);
                                assertThat(e.getCause())
                                        .isInstanceOf(InvalidArgumentException.class)
                                        .hasMessage("delete range ['a', 'b') rejected for an invalid argument");
                            });
        }

        @Test
        void completeUnknownStatus() {
            var response = new DeleteRangeResponse();
            response.parseFrom(UNKNOWN_STATUS);
            op.complete(response);
            assertFailedWithUnknown(callback, "GRPC.Status: UNRECOGNIZED(99)");
        }
    }

    private static PutOperation put(
            BatchManager batchManager,
            CompletableFuture<PutResult> callback,
            String key,
            Optional<String> partitionKey,
            Optional<List<Long>> sequenceKeysDeltas,
            byte[] value,
            OptionalLong expectedVersionId,
            Optional<Session> session,
            OptionalLong overrideVersionId,
            OptionalLong overrideModificationsCount) {
        return new PutOperation(
                batchManager,
                1L,
                callback,
                key,
                partitionKey,
                sequenceKeysDeltas,
                value,
                expectedVersionId,
                session,
                Optional.empty(),
                Collections.emptyList(),
                overrideVersionId,
                overrideModificationsCount,
                0);
    }

    static Stream<Arguments> nullArguments() {
        var putCallback = new CompletableFuture<PutResult>();
        var deleteCallback = new CompletableFuture<Boolean>();
        Optional<String> noString = Optional.empty();
        Optional<List<Long>> noDeltas = Optional.empty();
        Optional<Session> noSession = Optional.empty();
        var noVersion = OptionalLong.empty();
        var value = new byte[0];
        var get =
                new GetOperation(
                        batchManager,
                        1L,
                        new CompletableFuture<>(),
                        "k",
                        new GetOptions(null, true, KeyComparisonType.EQUAL, null));
        var putOp =
                put(
                        batchManager,
                        putCallback,
                        "k",
                        noString,
                        noDeltas,
                        value,
                        noVersion,
                        noSession,
                        noVersion,
                        noVersion);
        var delete = new DeleteOperation(batchManager, 1L, deleteCallback, "k");
        var deleteRange =
                new DeleteRangeOperation(batchManager, 1L, new CompletableFuture<>(), "a", "b");
        return Stream.of(
                arguments(
                        "put batchManager",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                null,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put callback",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                null,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put key",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                null,
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put partitionKey",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                null,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put sequenceKeysDeltas",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                null,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put value",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                null,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put expectedVersionId",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                null,
                                                noSession,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put session",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                null,
                                                noVersion,
                                                noVersion)),
                arguments(
                        "put overrideVersionId",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                null,
                                                noVersion)),
                arguments(
                        "put overrideModificationsCount",
                        (ThrowingCallable)
                                () ->
                                        put(
                                                batchManager,
                                                putCallback,
                                                "k",
                                                noString,
                                                noDeltas,
                                                value,
                                                noVersion,
                                                noSession,
                                                noVersion,
                                                null)),
                arguments(
                        "delete batchManager",
                        (ThrowingCallable)
                                () -> new DeleteOperation(null, 1L, deleteCallback, "k", noVersion, 0)),
                arguments(
                        "delete callback",
                        (ThrowingCallable)
                                () -> new DeleteOperation(batchManager, 1L, null, "k", noVersion, 0)),
                arguments(
                        "delete key",
                        (ThrowingCallable)
                                () -> new DeleteOperation(batchManager, 1L, deleteCallback, null, noVersion, 0)),
                arguments(
                        "delete expectedVersionId",
                        (ThrowingCallable)
                                () -> new DeleteOperation(batchManager, 1L, deleteCallback, "k", null, 0)),
                arguments("get complete", (ThrowingCallable) () -> get.complete(null)),
                arguments("put complete", (ThrowingCallable) () -> putOp.complete(null)),
                arguments("delete complete", (ThrowingCallable) () -> delete.complete(null)),
                arguments("delete range complete", (ThrowingCallable) () -> deleteRange.complete(null)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nullArguments")
    void rejectsNullArguments(String argument, ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOf(NullPointerException.class);
    }

    @Nested
    @DisplayName("Tests of put operation fields")
    class PutOperationFieldTests {
        CompletableFuture<PutResult> callback = new CompletableFuture<>();
        Optional<Session> noSession = Optional.empty();
        OptionalLong noVersion = OptionalLong.empty();

        @Test
        void sequenceKeysRejectExpectedVersionId() {
            assertThatThrownBy(
                            () ->
                                    put(
                                            batchManager,
                                            callback,
                                            "k",
                                            Optional.of("pk"),
                                            Optional.of(List.of(1L)),
                                            new byte[0],
                                            OptionalLong.of(1),
                                            noSession,
                                            noVersion,
                                            noVersion))
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.INVALID_ARGUMENT));
        }

        @Test
        void sequenceKeysRequirePartitionKey() {
            assertThatThrownBy(
                            () ->
                                    put(
                                            batchManager,
                                            callback,
                                            "k",
                                            Optional.empty(),
                                            Optional.of(List.of(1L)),
                                            new byte[0],
                                            noVersion,
                                            noSession,
                                            noVersion,
                                            noVersion))
                    .isInstanceOfSatisfying(
                            OxiaStatusException.class,
                            e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.INVALID_ARGUMENT));
        }

        @Test
        void toProtoSequenceKeysAndSecondaryIndexes() {
            var op =
                    new PutOperation(
                            batchManager,
                            1L,
                            callback,
                            "k",
                            Optional.of("pk"),
                            Optional.of(List.of(1L, 2L)),
                            new byte[0],
                            noVersion,
                            noSession,
                            Optional.empty(),
                            List.of(new OptionSecondaryIndex("idx", "sk")),
                            noVersion,
                            noVersion);
            var request = new PutRequest();
            op.toProto(request);
            assertThat(request.getPartitionKey()).isEqualTo("pk");
            assertThat(request.getSequenceKeyDeltasCount()).isEqualTo(2);
            assertThat(request.getSequenceKeyDeltaAt(1)).isEqualTo(2L);
            assertThat(request.getSecondaryIndexesCount()).isEqualTo(1);
            assertThat(request.getSecondaryIndexeAt(0).getIndexName()).isEqualTo("idx");
            assertThat(request.getSecondaryIndexeAt(0).getSecondaryKey()).isEqualTo("sk");
        }

        @Test
        void equalityCoversKeyValueAndExpectedVersionId() {
            var op =
                    put(
                            batchManager,
                            callback,
                            "k",
                            Optional.empty(),
                            Optional.empty(),
                            new byte[] {1},
                            OptionalLong.of(1),
                            noSession,
                            noVersion,
                            noVersion);
            var same =
                    put(
                            batchManager,
                            new CompletableFuture<>(),
                            "k",
                            Optional.of("pk"),
                            Optional.empty(),
                            new byte[] {1},
                            OptionalLong.of(1),
                            noSession,
                            noVersion,
                            noVersion);
            var otherKey =
                    put(
                            batchManager,
                            callback,
                            "j",
                            Optional.empty(),
                            Optional.empty(),
                            new byte[] {1},
                            OptionalLong.of(1),
                            noSession,
                            noVersion,
                            noVersion);
            var otherValue =
                    put(
                            batchManager,
                            callback,
                            "k",
                            Optional.empty(),
                            Optional.empty(),
                            new byte[] {2},
                            OptionalLong.of(1),
                            noSession,
                            noVersion,
                            noVersion);
            var otherVersion =
                    put(
                            batchManager,
                            callback,
                            "k",
                            Optional.empty(),
                            Optional.empty(),
                            new byte[] {1},
                            OptionalLong.of(2),
                            noSession,
                            noVersion,
                            noVersion);

            assertThat(op.equals(op)).isTrue();
            assertThat(op.equals(same)).isTrue();
            assertThat(op.hashCode()).isEqualTo(same.hashCode());
            assertThat(op.equals(null)).isFalse();
            assertThat(op.equals("k")).isFalse();
            assertThat(op.equals(otherKey)).isFalse();
            assertThat(op.equals(otherValue)).isFalse();
            assertThat(op.equals(otherVersion)).isFalse();
        }
    }

    @Test
    void getToProtoSecondaryIndexName() {
        var op =
                new GetOperation(
                        batchManager,
                        1L,
                        new CompletableFuture<>(),
                        "k",
                        new GetOptions(null, true, KeyComparisonType.EQUAL, "idx"));
        var request = new GetRequest();
        op.toProto(request);
        assertThat(request.getSecondaryIndexName()).isEqualTo("idx");
    }
}
