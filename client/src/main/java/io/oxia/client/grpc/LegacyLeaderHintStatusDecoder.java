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

import static io.oxia.client.grpc.OxiaStatusCode.NODE_IS_NOT_LEADER;
import static io.oxia.client.grpc.OxiaStatusCode.UNKNOWN;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.oxia.proto.LeaderHint;
import java.util.Map;

final class LegacyLeaderHintStatusDecoder implements OxiaStatusDecoder {
    private static final Metadata.Key<com.google.rpc.Status> GRPC_STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    @Override
    public OxiaStatusException decode(Throwable cause) {
        final var trailers = Status.trailersFromThrowable(cause);
        final var grpcStatus = trailers == null ? null : trailers.get(GRPC_STATUS_DETAILS_KEY);
        if (grpcStatus == null || grpcStatus.getCode() != 106) {
            return new OxiaStatusException(UNKNOWN, Map.of(), cause.getMessage(), cause);
        }

        for (final var detail : grpcStatus.getDetailsList()) {
            if (!"type.googleapis.com/io.oxia.proto.v1.LeaderHint".equals(detail.getTypeUrl())) {
                continue;
            }
            try {
                final var hint = new LeaderHint();
                hint.parseFrom(detail.getValue().toByteArray());
                final var metadata =
                        Map.of(
                                "shard", Long.toString(hint.getShard()),
                                "leader", hint.getLeaderAddress());
                final var message =
                        grpcStatus.getMessage().isEmpty()
                                ? "oxia status " + NODE_IS_NOT_LEADER
                                : grpcStatus.getMessage();
                return new OxiaStatusException(NODE_IS_NOT_LEADER, metadata, message, cause);
            } catch (RuntimeException ignored) {
                // Ignore malformed hint details
            }
            break;
        }
        return new OxiaStatusException(UNKNOWN, Map.of(), grpcStatus.getMessage(), cause);
    }
}
