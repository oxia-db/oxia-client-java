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
package io.oxia.client.grpc;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Custom Oxia gRPC status codes, matching the Go server definitions in
 * common/constant/grpc_errors.go.
 */
public enum OxiaStatus {
    NOT_INITIALIZED(100, false),
    INVALID_TERM(101, false),
    INVALID_STATUS(102, true),
    CANCELLED(103, false),
    ALREADY_CLOSED(104, true),
    LEADER_ALREADY_CONNECTED(105, false),
    NODE_IS_NOT_LEADER(106, true),
    NODE_IS_NOT_FOLLOWER(107, false),
    SESSION_NOT_FOUND(108, false),
    INVALID_SESSION_TIMEOUT(109, false),
    NAMESPACE_NOT_FOUND(110, false),
    NOTIFICATIONS_NOT_ENABLED(111, false),
    UNKNOWN(-1, false);

    private final int code;
    private final boolean retriable;

    OxiaStatus(int code, boolean retriable) {
        this.code = code;
        this.retriable = retriable;
    }

    public int code() {
        return code;
    }

    public boolean isRetriable() {
        return retriable;
    }

    private static final Map<Integer, OxiaStatus> BY_CODE =
            Stream.of(values())
                    .filter(s -> s.code >= 0)
                    .collect(Collectors.toMap(OxiaStatus::code, Function.identity()));

    public static OxiaStatus fromCode(int code) {
        return BY_CODE.getOrDefault(code, UNKNOWN);
    }

    // ---- Static helpers for extracting status from gRPC errors ----

    private static final Metadata.Key<com.google.rpc.Status> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    /**
     * Returns whether the error is retriable. Standard gRPC UNAVAILABLE and custom Oxia retriable
     * codes return true.
     */
    public static boolean isRetriable(@Nullable Throwable err) {
        if (err == null) {
            return false;
        }

        Status grpcStatus = grpcStatusFromThrowable(err);
        if (grpcStatus != null && grpcStatus.getCode() == Status.Code.UNAVAILABLE) {
            return true;
        }

        return fromError(err).retriable;
    }

    /**
     * Extracts the OxiaStatus from a gRPC error. Reads the custom code from the
     * grpc-status-details-bin trailer, then falls back to the gRPC status description. The fallback
     * is needed because Go gRPC only sends the trailer when details are explicitly attached.
     */
    public static OxiaStatus fromError(@Nullable Throwable err) {
        if (err == null) {
            return UNKNOWN;
        }
        com.google.rpc.Status rpcStatus = rpcStatusFromThrowable(err);
        if (rpcStatus != null) {
            return fromCode(rpcStatus.getCode());
        }
        Status grpcStatus = grpcStatusFromThrowable(err);
        if (grpcStatus != null && grpcStatus.getDescription() != null) {
            return fromDescription(grpcStatus.getDescription());
        }
        return UNKNOWN;
    }

    static OxiaStatus fromDescription(String description) {
        if (description.startsWith("oxia: namespace not found")) {
            return NAMESPACE_NOT_FOUND;
        }
        return UNKNOWN;
    }

    @Nullable
    private static com.google.rpc.Status rpcStatusFromThrowable(@Nullable Throwable err) {
        if (err == null) {
            return null;
        }
        Metadata trailers = trailersFromThrowable(err);
        if (trailers != null) {
            com.google.rpc.Status status = trailers.get(STATUS_DETAILS_KEY);
            if (status != null) {
                return status;
            }
        }
        Throwable cause = err.getCause();
        if (cause != null && cause != err) {
            return rpcStatusFromThrowable(cause);
        }
        return null;
    }

    @Nullable
    private static Status grpcStatusFromThrowable(@Nullable Throwable err) {
        if (err == null) {
            return null;
        }
        if (err instanceof StatusRuntimeException sre) {
            return sre.getStatus();
        } else if (err instanceof StatusException se) {
            return se.getStatus();
        }
        Throwable cause = err.getCause();
        if (cause != null && cause != err) {
            return grpcStatusFromThrowable(cause);
        }
        return null;
    }

    @Nullable
    private static Metadata trailersFromThrowable(@Nullable Throwable err) {
        if (err instanceof StatusRuntimeException sre) {
            return sre.getTrailers();
        } else if (err instanceof StatusException se) {
            return se.getTrailers();
        }
        return null;
    }
}
