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

import static io.oxia.client.grpc.OxiaStatusCode.NAMESPACE_NOT_FOUND;
import static io.oxia.client.grpc.OxiaStatusCode.UNKNOWN;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.Getter;
import lombok.NonNull;

/** Exception carrying an Oxia status code translated from a GRPC status error. */
public class OxiaStatusException extends RuntimeException {
    @Getter private final @NonNull OxiaStatusCode statusCode;
    @Getter private final @NonNull Map<String, String> metadata;

    OxiaStatusException(
            @NonNull OxiaStatusCode statusCode,
            @NonNull Map<String, String> metadata,
            @NonNull String message,
            @NonNull Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.metadata = Map.copyOf(metadata);
    }

    public static boolean isNamespaceNotFound(@NonNull Throwable error) {
        return toException(error) instanceof OxiaStatusException oxiaError
                && oxiaError.getStatusCode() == NAMESPACE_NOT_FOUND;
    }

    public static boolean isRetryable(@NonNull Throwable error) {
        return toException(error) instanceof OxiaStatusException oxiaError && oxiaError.isRetryable();
    }

    public boolean isRetryable() {
        return switch (statusCode) {
            case ABORTED, NODE_IS_NOT_MEMBER, NODE_IS_NOT_LEADER, NOT_INITIALIZED, RESOURCE_UNAVAILABLE ->
                    true;
            default -> false;
        };
    }

    public static @NonNull Throwable toException(@NonNull Throwable error) {
        var cause = error;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        if (cause instanceof OxiaStatusException) {
            return cause;
        }
        final var grpcStatus = StatusProto.fromThrowable(cause);
        if (grpcStatus == null) {
            return cause;
        }
        return fromGrpcStatus(grpcStatus, cause);
    }

    private static OxiaStatusException fromGrpcStatus(
            com.google.rpc.Status grpcStatus, Throwable cause) {
        OxiaStatusCode statusCode = UNKNOWN;
        Map<String, String> metadata = Map.of();
        for (var detail : grpcStatus.getDetailsList()) {
            if (!detail.is(ErrorInfo.class)) {
                continue;
            }
            try {
                var info = detail.unpack(ErrorInfo.class);
                if (!"oxia.io".equals(info.getDomain())) {
                    continue;
                }
                statusCode = codeFromReason(info.getReason());
                metadata = info.getMetadataMap();
                break;
            } catch (InvalidProtocolBufferException ignored) {
            }
        }
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

    private static OxiaStatusCode codeFromReason(String reason) {
        try {
            return OxiaStatusCode.valueOf(reason);
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
