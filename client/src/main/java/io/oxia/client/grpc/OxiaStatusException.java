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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.oxia.client.util.CompletableFutures;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.NonNull;

/** Exception carrying an Oxia status code translated from a GRPC status error. */
public class OxiaStatusException extends RuntimeException {
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

            final var grpcStatus = StatusProto.fromThrowable(cause);
            if (grpcStatus == null) {
                return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
            }
            return fromGrpcStatus(grpcStatus, cause);
        } catch (RuntimeException e) {
            return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
        }
    }

    private static OxiaStatusException fromGrpcStatus(
            com.google.rpc.Status grpcStatus, Throwable cause) {
        OxiaStatusCode statusCode = UNKNOWN;
        Map<String, String> metadata = Map.of();

        // parse from the error info
        for (var detail : grpcStatus.getDetailsList()) {
            if (!detail.is(ErrorInfo.class)) {
                continue;
            }
            try {
                var info = detail.unpack(ErrorInfo.class);
                if (!"oxia.io".equals(info.getDomain())) {
                    continue;
                }
                try {
                    statusCode = OxiaStatusCode.valueOf(info.getReason());
                } catch (IllegalArgumentException e) {
                    statusCode = UNKNOWN;
                }
                metadata = info.getMetadataMap();
                break;
            } catch (InvalidProtocolBufferException ignored) {
            }
        }
        // parse from the grpc standard code
        if (statusCode == UNKNOWN) {
            statusCode =
                    switch (io.grpc.Status.fromCodeValue(grpcStatus.getCode()).getCode()) {
                        case UNAVAILABLE -> OxiaStatusCode.RESOURCE_UNAVAILABLE;
                        case ABORTED -> OxiaStatusCode.ABORTED;
                        default -> UNKNOWN;
                    };
        }
        // parse by the error messages
        if (statusCode == UNKNOWN) {
            statusCode =
                    switch (grpcStatus.getMessage()) {
                        case "oxia: server not initialized yet" -> OxiaStatusCode.NOT_INITIALIZED;
                        case "oxia: operation was cancelled",
                                "oxia: resource is already closed",
                                "oxia: leader is already connected" ->
                                OxiaStatusCode.ABORTED;
                        case "oxia: invalid term" -> OxiaStatusCode.INVALID_TERM;
                        case "oxia: invalid status" -> OxiaStatusCode.INVALID_STATUS;
                        case "oxia: session not found" -> OxiaStatusCode.SESSION_NOT_FOUND;
                        case "oxia: invalid session timeout" -> OxiaStatusCode.INVALID_SESSION_TIMEOUT;
                        case "oxia: namespace not found" -> OxiaStatusCode.NAMESPACE_NOT_FOUND;
                        case "oxia: notifications not enabled on namespace" ->
                                OxiaStatusCode.NOTIFICATIONS_NOT_ENABLED;
                        case "oxia: node is not a member" -> OxiaStatusCode.NODE_IS_NOT_MEMBER;
                        default -> {
                            if (grpcStatus.getMessage().startsWith("node is not leader for shard ")) {
                                yield OxiaStatusCode.NODE_IS_NOT_LEADER;
                            }
                            if (grpcStatus.getMessage().startsWith("node is not follower for shard ")) {
                                yield OxiaStatusCode.ABORTED;
                            }
                            yield UNKNOWN;
                        }
                    };
        }
        final var message =
                grpcStatus.getMessage().isEmpty() ? "oxia status " + statusCode : grpcStatus.getMessage();
        return new OxiaStatusException(statusCode, metadata, message, cause);
    }
}
