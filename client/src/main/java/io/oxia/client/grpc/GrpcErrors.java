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

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.oxia.proto.LeaderHint;
import javax.annotation.Nullable;

public final class GrpcErrors {

    // Custom Oxia gRPC status codes (must match the Go server definitions)
    public static final int CODE_INVALID_STATUS = 102;
    public static final int CODE_ALREADY_CLOSED = 104;
    public static final int CODE_NODE_IS_NOT_LEADER = 106;

    private static final Metadata.Key<com.google.rpc.Status> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    private GrpcErrors() {}

    public static boolean isRetriable(@Nullable Throwable err) {
        if (err == null) {
            return false;
        }

        Status status = statusFromThrowable(err);
        if (status == null) {
            return false;
        }

        // Standard gRPC UNAVAILABLE: connection failure, ok to re-attempt
        if (status.getCode() == Status.Code.UNAVAILABLE) {
            return true;
        }

        // Custom Oxia codes are transmitted in the grpc-status-details-bin trailer.
        int rawCode = rawCodeFromThrowable(err);
        return switch (rawCode) {
            case CODE_INVALID_STATUS, // Leader fenced the shard; new leader expected
                    CODE_ALREADY_CLOSED, // Leader closing; new leader expected
                    CODE_NODE_IS_NOT_LEADER -> // Request sent to non-leader; retry to new leader
                    true;
            default -> false;
        };
    }

    @Nullable
    public static LeaderHint findLeaderHint(@Nullable Throwable err) {
        com.google.rpc.Status rpcStatus = rpcStatusFromThrowable(err);
        if (rpcStatus == null) {
            return null;
        }

        for (Any detail : rpcStatus.getDetailsList()) {
            if (detail.is(LeaderHint.class)) {
                try {
                    return detail.unpack(LeaderHint.class);
                } catch (InvalidProtocolBufferException e) {
                    return null;
                }
            }
        }
        return null;
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
        // Unwrap
        Throwable cause = err.getCause();
        if (cause != null && cause != err) {
            return rpcStatusFromThrowable(cause);
        }
        return null;
    }

    private static int rawCodeFromThrowable(@Nullable Throwable err) {
        com.google.rpc.Status rpcStatus = rpcStatusFromThrowable(err);
        return rpcStatus != null ? rpcStatus.getCode() : -1;
    }

    @Nullable
    private static Status statusFromThrowable(@Nullable Throwable err) {
        if (err instanceof StatusRuntimeException sre) {
            return sre.getStatus();
        } else if (err instanceof StatusException se) {
            return se.getStatus();
        }
        // Unwrap CompletionException/ExecutionException
        Throwable cause = err.getCause();
        if (cause != null && cause != err) {
            return statusFromThrowable(cause);
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
