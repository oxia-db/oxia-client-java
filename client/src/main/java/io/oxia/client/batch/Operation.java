/*
 * Copyright © 2022-2025 StreamNative Inc.
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

import static io.oxia.client.api.Version.KeyNotExists;
import static io.oxia.client.batch.Operation.ReadOperation;
import static io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import static io.oxia.client.batch.Operation.WriteOperation;
import static io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import static io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import static io.oxia.client.batch.Operation.WriteOperation.PutOperation;

import com.google.protobuf.ByteString;
import io.oxia.client.ProtoUtil;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OptionSecondaryIndex;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.options.GetOptions;
import io.oxia.proto.DeleteRangeRequest;
import io.oxia.proto.DeleteRangeResponse;
import io.oxia.proto.DeleteRequest;
import io.oxia.proto.DeleteResponse;
import io.oxia.proto.GetRequest;
import io.oxia.proto.GetResponse;
import io.oxia.proto.PutRequest;
import io.oxia.proto.PutResponse;
import io.oxia.proto.Status;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public sealed interface Operation<R> permits ReadOperation, WriteOperation {

    CompletableFuture<R> callback();

    default void fail(Throwable t) {
        callback().completeExceptionally(t);
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation {
        record GetOperation(
                @NonNull CompletableFuture<GetResult> callback,
                @NonNull String key,
                @NonNull GetOptions options)
                implements ReadOperation<GetResult> {
            GetRequest toProto() {
                var builder =
                        GetRequest.newBuilder()
                                .setKey(key)
                                .setComparisonType(options.comparisonType())
                                .setIncludeValue(options.includeValue());
                if (options.secondaryIndexName() != null) {
                    builder.setSecondaryIndexName(options.secondaryIndexName());
                }
                return builder.build();
            }

            void complete(@NonNull GetResponse response) {
                switch (response.getStatus()) {
                    case KEY_NOT_FOUND -> callback.complete(null);
                    case OK -> callback.complete(ProtoUtil.getResultFromProto(key, response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }
        }
    }

    sealed interface WriteOperation<R> extends Operation<R>
            permits PutOperation, DeleteOperation, DeleteRangeOperation {
        record PutOperation(
                @NonNull CompletableFuture<PutResult> callback,
                @NonNull String key,
                @NonNull Optional<String> partitionKey,
                @NonNull Optional<List<Long>> sequenceKeysDeltas,
                byte @NonNull [] value,
                @NonNull OptionalLong expectedVersionId,
                OptionalLong sessionId,
                Optional<String> clientIdentifier,
                List<OptionSecondaryIndex> secondaryIndexes)
                implements WriteOperation<PutResult> {

            public PutOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.getAsLong() < KeyNotExists) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= -1 (KeyNotExists), was: "
                                    + expectedVersionId.getAsLong());
                }

                if (sequenceKeysDeltas.isPresent()) {
                    if (expectedVersionId.isPresent()) {
                        throw new IllegalArgumentException(
                                "Usage of sequential keys does not allow to specify an ExpectedVersionId");
                    }

                    if (partitionKey.isEmpty()) {
                        throw new IllegalArgumentException(
                                "usage of sequential keys requires PartitionKey() to be set");
                    }
                }
            }

            PutRequest toProto() {
                var builder = PutRequest.newBuilder().setKey(key).setValue(ByteString.copyFrom(value));
                partitionKey.ifPresent(builder::setPartitionKey);
                expectedVersionId.ifPresent(builder::setExpectedVersionId);
                sessionId.ifPresent(builder::setSessionId);
                clientIdentifier.ifPresent(builder::setClientIdentity);
                sequenceKeysDeltas.ifPresent(builder::addAllSequenceKeyDelta);
                if (!secondaryIndexes.isEmpty()) {
                    secondaryIndexes.forEach(
                            si -> {
                                builder
                                        .addSecondaryIndexesBuilder()
                                        .setIndexName(si.indexName())
                                        .setSecondaryKey(si.secondaryKey())
                                        .build();
                            });
                }
                return builder.build();
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case SESSION_DOES_NOT_EXIST -> fail(new SessionDoesNotExistException());
                    case UNEXPECTED_VERSION_ID -> {
                        if (expectedVersionId.getAsLong() == KeyNotExists) {
                            fail(new KeyAlreadyExistsException(key));
                        } else {
                            fail(new UnexpectedVersionIdException(key, expectedVersionId.getAsLong()));
                        }
                    }
                    case OK -> callback.complete(ProtoUtil.getPutResultFromProto(key, response));
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                PutOperation that = (PutOperation) o;
                return key.equals(that.key)
                        && Arrays.equals(value, that.value)
                        && Objects.equals(expectedVersionId, that.expectedVersionId);
            }

            @Override
            public int hashCode() {
                int result = Objects.hash(key, expectedVersionId);
                result = 31 * result + Arrays.hashCode(value);
                return result;
            }
        }

        record DeleteOperation(
                @NonNull CompletableFuture<Boolean> callback,
                @NonNull String key,
                @NonNull OptionalLong expectedVersionId)
                implements WriteOperation<Boolean> {

            public DeleteOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.getAsLong() < 0) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= 0, was: " + expectedVersionId.getAsLong());
                }
            }

            DeleteRequest toProto() {
                var builder = DeleteRequest.newBuilder().setKey(key);
                expectedVersionId.ifPresent(builder::setExpectedVersionId);
                return builder.build();
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION_ID ->
                            fail(new UnexpectedVersionIdException(key, expectedVersionId.getAsLong()));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public DeleteOperation(@NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(callback, key, OptionalLong.empty());
            }
        }

        record DeleteRangeOperation(
                @NonNull CompletableFuture<Void> callback,
                @NonNull String startKeyInclusive,
                @NonNull String endKeyExclusive)
                implements WriteOperation<Void> {
            DeleteRangeRequest toProto() {
                return DeleteRangeRequest.newBuilder()
                        .setStartInclusive(startKeyInclusive)
                        .setEndExclusive(endKeyExclusive)
                        .build();
            }

            void complete(@NonNull DeleteRangeResponse response) {
                if (response.getStatus() == Status.OK) {
                    callback.complete(null);
                } else {
                    fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }
        }
    }
}
