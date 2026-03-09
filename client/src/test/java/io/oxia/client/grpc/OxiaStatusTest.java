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

class OxiaStatusTest {

    private static final Metadata.Key<com.google.rpc.Status> STATUS_DETAILS_KEY =
            Metadata.Key.of(
                    "grpc-status-details-bin",
                    ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance()));

    private static StatusRuntimeException oxiaError(OxiaStatus oxiaStatus, String message) {
        var rpcStatus =
                com.google.rpc.Status.newBuilder().setCode(oxiaStatus.code()).setMessage(message).build();
        Metadata trailers = new Metadata();
        trailers.put(STATUS_DETAILS_KEY, rpcStatus);
        return Status.UNKNOWN.withDescription(message).asRuntimeException(trailers);
    }

    private static StatusRuntimeException oxiaErrorWithHint(
            OxiaStatus oxiaStatus, String message, LeaderHint hint) {
        var rpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(oxiaStatus.code())
                        .setMessage(message)
                        .addDetails(Any.pack(hint))
                        .build();
        Metadata trailers = new Metadata();
        trailers.put(STATUS_DETAILS_KEY, rpcStatus);
        return Status.UNKNOWN.withDescription(message).asRuntimeException(trailers);
    }

    @Nested
    class FromCode {
        @Test
        void knownCodes() {
            assertThat(OxiaStatus.fromCode(100)).isEqualTo(OxiaStatus.NOT_INITIALIZED);
            assertThat(OxiaStatus.fromCode(106)).isEqualTo(OxiaStatus.NODE_IS_NOT_LEADER);
            assertThat(OxiaStatus.fromCode(110)).isEqualTo(OxiaStatus.NAMESPACE_NOT_FOUND);
        }

        @Test
        void unknownCode() {
            assertThat(OxiaStatus.fromCode(999)).isEqualTo(OxiaStatus.UNKNOWN);
        }
    }

    @Nested
    class FromError {
        @Test
        void extractsOxiaStatusFromTrailer() {
            var err = oxiaError(OxiaStatus.NODE_IS_NOT_LEADER, "node is not leader for shard 0");
            assertThat(OxiaStatus.fromError(err)).isEqualTo(OxiaStatus.NODE_IS_NOT_LEADER);
        }

        @Test
        void fallsBackToDescriptionWhenNoTrailer() {
            // Simulates what the real Go server sends: no grpc-status-details-bin trailer,
            // just a plain gRPC error with description
            var err = Status.UNKNOWN.withDescription("oxia: namespace not found").asRuntimeException();
            assertThat(OxiaStatus.fromError(err)).isEqualTo(OxiaStatus.NAMESPACE_NOT_FOUND);
        }

        @Test
        void descriptionFallbackMatchesPrefix() {
            // NODE_IS_NOT_LEADER uses prefix matching (description may include shard info)
            var err =
                    Status.UNKNOWN.withDescription("node is not leader for shard 42").asRuntimeException();
            assertThat(OxiaStatus.fromError(err)).isEqualTo(OxiaStatus.NODE_IS_NOT_LEADER);
        }

        @Test
        void returnsUnknownForPlainGrpcError() {
            var err = Status.INTERNAL.asRuntimeException();
            assertThat(OxiaStatus.fromError(err)).isEqualTo(OxiaStatus.UNKNOWN);
        }

        @Test
        void returnsUnknownForNull() {
            assertThat(OxiaStatus.fromError(null)).isEqualTo(OxiaStatus.UNKNOWN);
        }
    }

    @Nested
    class IsRetriableTests {

        @Test
        void nullIsNotRetriable() {
            assertThat(OxiaStatus.isRetriable(null)).isFalse();
        }

        @Test
        void unavailableIsRetriable() {
            var err = Status.UNAVAILABLE.withDescription("connection refused").asRuntimeException();
            assertThat(OxiaStatus.isRetriable(err)).isTrue();
        }

        @Test
        void unknownIsNotRetriable() {
            var err = Status.UNKNOWN.asRuntimeException();
            assertThat(OxiaStatus.isRetriable(err)).isFalse();
        }

        @Test
        void nodeIsNotLeaderIsRetriable() {
            var err = oxiaError(OxiaStatus.NODE_IS_NOT_LEADER, "node is not leader for shard 0");
            assertThat(OxiaStatus.isRetriable(err)).isTrue();
        }

        @Test
        void invalidStatusIsRetriable() {
            var err = oxiaError(OxiaStatus.INVALID_STATUS, "oxia: invalid status");
            assertThat(OxiaStatus.isRetriable(err)).isTrue();
        }

        @Test
        void alreadyClosedIsRetriable() {
            var err = oxiaError(OxiaStatus.ALREADY_CLOSED, "oxia: resource is already closed");
            assertThat(OxiaStatus.isRetriable(err)).isTrue();
        }

        @Test
        void sessionNotFoundIsNotRetriable() {
            var err = oxiaError(OxiaStatus.SESSION_NOT_FOUND, "oxia: session not found");
            assertThat(OxiaStatus.isRetriable(err)).isFalse();
        }

        @Test
        void namespaceNotFoundIsNotRetriable() {
            var err = oxiaError(OxiaStatus.NAMESPACE_NOT_FOUND, "oxia: namespace not found");
            assertThat(OxiaStatus.isRetriable(err)).isFalse();
        }
    }

    @Nested
    class FindLeaderHintTests {

        @Test
        void nullReturnsNull() {
            assertThat(OxiaStatus.findLeaderHint(null)).isNull();
        }

        @Test
        void noDetailsReturnsNull() {
            var err = Status.UNKNOWN.asRuntimeException();
            assertThat(OxiaStatus.findLeaderHint(err)).isNull();
        }

        @Test
        void extractsLeaderHintFromDetails() {
            var hint = LeaderHint.newBuilder().setShard(42).setLeaderAddress("leader:6648").build();
            var err =
                    oxiaErrorWithHint(OxiaStatus.NODE_IS_NOT_LEADER, "node is not leader for shard 42", hint);

            LeaderHint extracted = OxiaStatus.findLeaderHint(err);
            assertThat(extracted).isNotNull();
            assertThat(extracted.getShard()).isEqualTo(42);
            assertThat(extracted.getLeaderAddress()).isEqualTo("leader:6648");
        }

        @Test
        void returnsNullWhenNoLeaderHintInDetails() {
            var err = oxiaError(OxiaStatus.NODE_IS_NOT_LEADER, "node is not leader for shard 0");
            assertThat(OxiaStatus.findLeaderHint(err)).isNull();
        }
    }
}
