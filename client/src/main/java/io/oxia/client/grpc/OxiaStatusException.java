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
package io.oxia.client.grpc;

import static io.oxia.client.grpc.OxiaStatusCode.RESOURCE_UNAVAILABLE;
import static io.oxia.client.grpc.OxiaStatusCode.SHARD_NOT_FOUND;
import static io.oxia.client.grpc.OxiaStatusCode.TIMEOUT;
import static io.oxia.client.grpc.OxiaStatusCode.UNKNOWN;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.oxia.client.util.CompletableFutures;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.NonNull;

/** Exception carrying an Oxia status code translated from a GRPC status error. */
public class OxiaStatusException extends RuntimeException {
    // Keep the legacy decoder first because StatusProto rejects Oxia 0.16.x custom status code 106.
    private static final List<OxiaStatusDecoder> STATUS_DECODERS =
            List.of(new LegacyLeaderHintStatusDecoder(), new StandardGrpcStatusDecoder());

    @Getter private final @NonNull OxiaStatusCode statusCode;
    @Getter private final @NonNull Map<String, String> metadata;

    OxiaStatusException(
            @NonNull OxiaStatusCode statusCode,
            @NonNull Map<String, String> metadata,
            String message,
            Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.metadata = Map.copyOf(metadata);
    }

    public boolean isRetryable() {
        return switch (statusCode) {
            case ABORTED, NODE_IS_NOT_MEMBER, NODE_IS_NOT_LEADER, NOT_INITIALIZED, RESOURCE_UNAVAILABLE ->
                    true;
            default -> false;
        };
    }

    public @NonNull Optional<String> getLeaderHint(long shardId) {
        if (!Long.toString(shardId).equals(metadata.get("shard"))) {
            return Optional.empty();
        }
        return Optional.ofNullable(metadata.get("leader")).filter(leader -> !leader.isEmpty());
    }

    public static @NonNull OxiaStatusException shardNotFound(@NonNull String key) {
        return new OxiaStatusException(
                SHARD_NOT_FOUND, Map.of("key", key), "No shard available to accept to key: " + key, null);
    }

    public static @NonNull OxiaStatusException shardNotFound(long shardId) {
        return new OxiaStatusException(
                SHARD_NOT_FOUND,
                Map.of("shard", Long.toString(shardId)),
                "Shard not available : " + shardId,
                null);
    }

    public static @NonNull OxiaStatusException leaderNotAvailable(long shardId) {
        return new OxiaStatusException(
                RESOURCE_UNAVAILABLE,
                Map.of("shard", Long.toString(shardId)),
                "Leader not available for shard : " + shardId,
                null);
    }

    public static @NonNull OxiaStatusException resourceUnavailable(@NonNull String message) {
        return new OxiaStatusException(RESOURCE_UNAVAILABLE, Map.of(), message, null);
    }

    public static @NonNull OxiaStatusException timeout(@NonNull Throwable error) {
        return new OxiaStatusException(
                TIMEOUT, Map.of(), "Request timed out", CompletableFutures.unwrapException(error));
    }

    public static @NonNull OxiaStatusException from(@NonNull Throwable error) {
        Throwable cause = CompletableFutures.unwrapException(error);
        try {
            if (cause instanceof OxiaStatusException oxiaError) {
                return oxiaError;
            }
            if (cause instanceof TimeoutException) {
                return timeout(cause);
            }
            if (!(cause instanceof StatusException || cause instanceof StatusRuntimeException)) {
                return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
            }

            OxiaStatusException decodedError = null;
            for (final var decoder : STATUS_DECODERS) {
                decodedError = decoder.decode(cause);
                if (decodedError.getStatusCode() != UNKNOWN) {
                    return decodedError;
                }
            }
            return decodedError != null
                    ? decodedError
                    : new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
        } catch (RuntimeException e) {
            return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
        }
    }
}
