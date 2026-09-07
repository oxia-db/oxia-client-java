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

import static io.oxia.client.grpc.OxiaStatusCode.UNKNOWN;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.protobuf.StatusProto;
import java.util.Map;

final class StandardGrpcStatusDecoder implements OxiaStatusDecoder {
    @Override
    public OxiaStatusException decode(Throwable cause) {
        final var grpcStatus = StatusProto.fromThrowable(cause);
        if (grpcStatus == null) {
            return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
        }

        var statusCode = UNKNOWN;
        Map<String, String> metadata = Map.of();
        for (final var detail : grpcStatus.getDetailsList()) {
            if (!detail.is(ErrorInfo.class)) {
                continue;
            }
            try {
                final var info = detail.unpack(ErrorInfo.class);
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
        if (statusCode == UNKNOWN) {
            statusCode =
                    switch (io.grpc.Status.fromCodeValue(grpcStatus.getCode()).getCode()) {
                        case UNAVAILABLE -> OxiaStatusCode.RESOURCE_UNAVAILABLE;
                        case ABORTED -> OxiaStatusCode.ABORTED;
                        default -> UNKNOWN;
                    };
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
}
