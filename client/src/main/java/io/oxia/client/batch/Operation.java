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

import io.netty.buffer.ByteBufUtil;
import io.oxia.client.ProtoUtil;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.defs.OptionSecondaryIndex;
import io.oxia.client.options.GetOptions;
import io.oxia.client.session.Session;
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

    /** The shard this operation is routed to; the batching threads group operations by shard. */
    long shardId();

    default void fail(Throwable t) {
        callback().completeExceptionally(t);
    }

    /**
     * Fails the operation with an expected outcome, such as a failed conditional write, without
     * walking the stack. Dependent stages pass a {@code CompletionException} on as is, but wrap any
     * other exception in a new one, which walks the stack.
     */
    default void failExpected(Throwable t) {
        callback().completeExceptionally(new StacklessCompletionException(t));
    }

    sealed interface ReadOperation<R> extends Operation<R> permits GetOperation {
        record GetOperation(
                long shardId,
                @NonNull CompletableFuture<GetResult> callback,
                @NonNull String key,
                @NonNull GetOptions options)
                implements ReadOperation<GetResult> {
            /** Fills in the given request with this operation's fields. */
            void toProto(GetRequest req) {
                req.setKey(key)
                        .setComparisonType(options.comparisonType())
                        .setIncludeValue(options.includeValue());
                if (options.secondaryIndexName() != null) {
                    req.setSecondaryIndexName(options.secondaryIndexName());
                }
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

        /**
         * The UTF-8 encoded size of the keys, plus the value, that the operation adds to a write batch.
         * The client computes it once, for the pending bytes limit, and passes it in.
         */
        int byteSize();

        record PutOperation(
                long shardId,
                @NonNull CompletableFuture<PutResult> callback,
                @NonNull String key,
                @NonNull Optional<String> partitionKey,
                @NonNull Optional<List<Long>> sequenceKeysDeltas,
                byte @NonNull [] value,
                @NonNull OptionalLong expectedVersionId,
                @NonNull Optional<Session> session,
                Optional<String> clientIdentifier,
                List<OptionSecondaryIndex> secondaryIndexes,
                @NonNull OptionalLong overrideVersionId,
                @NonNull OptionalLong overrideModificationsCount,
                int byteSize)
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

            public PutOperation(
                    long shardId,
                    @NonNull CompletableFuture<PutResult> callback,
                    @NonNull String key,
                    @NonNull Optional<String> partitionKey,
                    @NonNull Optional<List<Long>> sequenceKeysDeltas,
                    byte @NonNull [] value,
                    @NonNull OptionalLong expectedVersionId,
                    @NonNull Optional<Session> session,
                    Optional<String> clientIdentifier,
                    List<OptionSecondaryIndex> secondaryIndexes,
                    @NonNull OptionalLong overrideVersionId,
                    @NonNull OptionalLong overrideModificationsCount) {
                this(
                        shardId,
                        callback,
                        key,
                        partitionKey,
                        sequenceKeysDeltas,
                        value,
                        expectedVersionId,
                        session,
                        clientIdentifier,
                        secondaryIndexes,
                        overrideVersionId,
                        overrideModificationsCount,
                        ByteBufUtil.utf8Bytes(key) + value.length);
            }

            /** Fills in the given request with this operation's fields. */
            void toProto(PutRequest req) {
                req.setKey(key).setValue(value);
                partitionKey.ifPresent(req::setPartitionKey);
                expectedVersionId.ifPresent(req::setExpectedVersionId);
                session.ifPresent(s -> req.setSessionId(s.getSessionId()));
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
                            failExpected(new KeyAlreadyExistsException(key));
                        } else {
                            failExpected(new UnexpectedVersionIdException(key, expectedVersionId.getAsLong()));
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
                long shardId,
                @NonNull CompletableFuture<Boolean> callback,
                @NonNull String key,
                @NonNull OptionalLong expectedVersionId,
                int byteSize)
                implements WriteOperation<Boolean> {

            public DeleteOperation {
                if (expectedVersionId.isPresent() && expectedVersionId.getAsLong() < 0) {
                    throw new IllegalArgumentException(
                            "expectedVersionId must be >= 0, was: " + expectedVersionId.getAsLong());
                }
            }

            /** Fills in the given request with this operation's fields. */
            void toProto(DeleteRequest req) {
                req.setKey(key);
                expectedVersionId.ifPresent(req::setExpectedVersionId);
            }

            void complete(@NonNull DeleteResponse response) {
                switch (response.getStatus()) {
                    case UNEXPECTED_VERSION_ID ->
                            failExpected(new UnexpectedVersionIdException(key, expectedVersionId.getAsLong()));
                    case KEY_NOT_FOUND -> callback.complete(false);
                    case OK -> callback.complete(true);
                    default -> fail(new IllegalStateException("GRPC.Status: " + response.getStatus().name()));
                }
            }

            public DeleteOperation(
                    long shardId,
                    @NonNull CompletableFuture<Boolean> callback,
                    @NonNull String key,
                    @NonNull OptionalLong expectedVersionId) {
                this(shardId, callback, key, expectedVersionId, ByteBufUtil.utf8Bytes(key));
            }

            public DeleteOperation(
                    long shardId, @NonNull CompletableFuture<Boolean> callback, @NonNull String key) {
                this(shardId, callback, key, OptionalLong.empty());
            }
        }

        record DeleteRangeOperation(
                long shardId,
                @NonNull CompletableFuture<Void> callback,
                @NonNull String startKeyInclusive,
                @NonNull String endKeyExclusive,
                int byteSize)
                implements WriteOperation<Void> {

            public DeleteRangeOperation(
                    long shardId,
                    @NonNull CompletableFuture<Void> callback,
                    @NonNull String startKeyInclusive,
                    @NonNull String endKeyExclusive) {
                this(
                        shardId,
                        callback,
                        startKeyInclusive,
                        endKeyExclusive,
                        ByteBufUtil.utf8Bytes(startKeyInclusive) + ByteBufUtil.utf8Bytes(endKeyExclusive));
            }

            /** Fills in the given request with this operation's fields. */
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
