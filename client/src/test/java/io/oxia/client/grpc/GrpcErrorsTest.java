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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.oxia.proto.LeaderHint;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class GrpcErrorsTest {

    // The trailer key where gRPC stores the full google.rpc.Status proto
    private static final Metadata.Key<com.google.rpc.Status> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    /**
     * Creates a StatusRuntimeException that simulates what the Go gRPC server sends for custom Oxia
     * status codes. The Go server sets a custom code in google.rpc.Status and transmits it via the
     * grpc-status-details-bin trailer. The gRPC framework maps unknown codes to UNKNOWN.
     */
    private static StatusRuntimeException customStatusError(int code, String message) {
        var rpcStatus = com.google.rpc.Status.newBuilder().setCode(code).setMessage(message).build();
        Metadata trailers = new Metadata();
        trailers.put(STATUS_DETAILS_KEY, rpcStatus);
        return Status.UNKNOWN.withDescription(message).asRuntimeException(trailers);
    }

    private static StatusRuntimeException customStatusErrorWithHint(
            int code, String message, LeaderHint hint) {
        var rpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(code)
                        .setMessage(message)
                        .addDetails(Any.pack(hint))
                        .build();
        Metadata trailers = new Metadata();
        trailers.put(STATUS_DETAILS_KEY, rpcStatus);
        return Status.UNKNOWN.withDescription(message).asRuntimeException(trailers);
    }

    @Nested
    class IsRetriable {

        @Test
        void nullIsNotRetriable() {
            assertThat(GrpcErrors.isRetriable(null)).isFalse();
        }

        @Test
        void unavailableIsRetriable() {
            var err = Status.UNAVAILABLE.withDescription("connection refused").asRuntimeException();
            assertThat(GrpcErrors.isRetriable(err)).isTrue();
        }

        @Test
        void unknownIsNotRetriable() {
            var err = Status.UNKNOWN.asRuntimeException();
            assertThat(GrpcErrors.isRetriable(err)).isFalse();
        }

        @Test
        void notFoundIsNotRetriable() {
            var err = Status.NOT_FOUND.asRuntimeException();
            assertThat(GrpcErrors.isRetriable(err)).isFalse();
        }

        @Test
        void nodeIsNotLeaderIsRetriable() {
            var err =
                    customStatusError(GrpcErrors.CODE_NODE_IS_NOT_LEADER, "node is not leader for shard 0");
            assertThat(GrpcErrors.isRetriable(err)).isTrue();
        }

        @Test
        void invalidStatusIsRetriable() {
            var err = customStatusError(GrpcErrors.CODE_INVALID_STATUS, "oxia: invalid status");
            assertThat(GrpcErrors.isRetriable(err)).isTrue();
        }

        @Test
        void alreadyClosedIsRetriable() {
            var err =
                    customStatusError(GrpcErrors.CODE_ALREADY_CLOSED, "oxia: resource is already closed");
            assertThat(GrpcErrors.isRetriable(err)).isTrue();
        }

        @Test
        void sessionNotFoundIsNotRetriable() {
            var err = customStatusError(108, "oxia: session not found");
            assertThat(GrpcErrors.isRetriable(err)).isFalse();
        }
    }

    @Nested
    class FindLeaderHintTests {

        @Test
        void nullReturnsNull() {
            assertThat(GrpcErrors.findLeaderHint(null)).isNull();
        }

        @Test
        void noDetailsReturnsNull() {
            var err = Status.UNKNOWN.asRuntimeException();
            assertThat(GrpcErrors.findLeaderHint(err)).isNull();
        }

        @Test
        void extractsLeaderHintFromDetails() {
            var hint = LeaderHint.newBuilder().setShard(42).setLeaderAddress("leader:6648").build();
            var err =
                    customStatusErrorWithHint(
                            GrpcErrors.CODE_NODE_IS_NOT_LEADER, "node is not leader for shard 42", hint);

            LeaderHint extracted = GrpcErrors.findLeaderHint(err);
            assertThat(extracted).isNotNull();
            assertThat(extracted.getShard()).isEqualTo(42);
            assertThat(extracted.getLeaderAddress()).isEqualTo("leader:6648");
        }

        @Test
        void returnsNullWhenNoLeaderHintInDetails() {
            var err =
                    customStatusError(GrpcErrors.CODE_NODE_IS_NOT_LEADER, "node is not leader for shard 0");
            assertThat(GrpcErrors.findLeaderHint(err)).isNull();
        }
    }
}
