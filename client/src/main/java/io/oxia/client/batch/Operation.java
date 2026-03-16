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
import static io.oxia.client.batch.Operation.ReadOperation;
import static io.oxia.client.batch.Operation.ReadOperation.GetOperation;
import static io.oxia.client.batch.Operation.WriteOperation;
import static io.oxia.client.batch.Operation.WriteOperation.DeleteOperation;
import static io.oxia.client.batch.Operation.WriteOperation.DeleteRangeOperation;
import static io.oxia.client.batch.Operation.WriteOperation.PutOperation;

import io.oxia.client.ProtoUtil;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.defs.OptionSecondaryIndex;
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
                var req = new GetRequest();
                req.setKey(key)
                        .setComparisonType(options.comparisonType())
                        .setIncludeValue(options.includeValue());
                if (options.secondaryIndexName() != null) {
                    req.setSecondaryIndexName(options.secondaryIndexName());
                }
                return req;
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
                List<OptionSecondaryIndex> secondaryIndexes,
                @NonNull OptionalLong overrideVersionId,
                @NonNull OptionalLong overrideModificationsCount)
                implements WriteOperation<PutResult> {

            public PutOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.getAsLong() < KEY_NOT_EXISTS) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= -1 (KEY_NOT_EXISTS), was: "
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

            void toProto(PutRequest req) {
                req.setKey(key).setValue(value);
                partitionKey.ifPresent(req::setPartitionKey);
                expectedVersionId.ifPresent(req::setExpectedVersionId);
                sessionId.ifPresent(req::setSessionId);
                clientIdentifier.ifPresent(req::setClientIdentity);
                sequenceKeysDeltas.ifPresent(deltas -> deltas.forEach(req::addSequenceKeyDelta));
                if (!secondaryIndexes.isEmpty()) {
                    secondaryIndexes.forEach(
                            si ->
                                    req.addSecondaryIndexe()
                                            .setIndexName(si.indexName())
                                            .setSecondaryKey(si.secondaryKey()));
                }
                overrideVersionId.ifPresent(req::setOverrideVersionId);
                overrideModificationsCount.ifPresent(req::setOverrideModificationsCount);
            }

            void complete(@NonNull PutResponse response) {
                switch (response.getStatus()) {
                    case SESSION_DOES_NOT_EXIST -> fail(new SessionDoesNotExistException());
                    case UNEXPECTED_VERSION_ID -> {
                        if (expectedVersionId.getAsLong() == KEY_NOT_EXISTS) {
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

            void toProto(DeleteRequest req) {
                req.setKey(key);
                expectedVersionId.ifPresent(req::setExpectedVersionId);
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
            void toProto(DeleteRangeRequest req) {
                req.setStartInclusive(startKeyInclusive).setEndExclusive(endKeyExclusive);
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
