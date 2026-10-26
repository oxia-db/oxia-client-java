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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.oxia.proto.LeaderHint;
import org.junit.jupiter.api.Test;

class LegacyLeaderHintStatusDecoderTest {
    private static final String LEADER_HINT_TYPE_URL =
            "type.googleapis.com/io.oxia.proto.v1.LeaderHint";

    private final LegacyLeaderHintStatusDecoder decoder = new LegacyLeaderHintStatusDecoder();

    @Test
    void decodesLeaderHintAfterOtherDetails() {
        var hint = new LeaderHint().setShard(2).setLeaderAddress("server-2:6648");
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(106)
                        .addDetails(Any.newBuilder().setTypeUrl("type.googleapis.com/other.Detail"))
                        .addDetails(
                                Any.newBuilder()
                                        .setTypeUrl(LEADER_HINT_TYPE_URL)
                                        .setValue(ByteString.copyFrom(hint.toByteArray())))
                        .build();

        var error = decoder.decode(legacyError(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.NODE_IS_NOT_LEADER);
        assertThat(error).hasMessage("oxia status NODE_IS_NOT_LEADER");
        assertThat(error.getLeaderHint(2)).contains("server-2:6648");
    }

    @Test
    void returnsUnknownWithoutLeaderHint() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(106)
                        .setMessage("node is not leader for shard 2")
                        .build();

        var error = decoder.decode(legacyError(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(error).hasMessage("node is not leader for shard 2");
    }

    @Test
    void returnsUnknownForMalformedLeaderHint() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(106)
                        .setMessage("node is not leader for shard 2")
                        .addDetails(
                                Any.newBuilder()
                                        .setTypeUrl(LEADER_HINT_TYPE_URL)
                                        .setValue(ByteString.copyFrom(new byte[] {0x12, 0x7F})))
                        .build();

        var error = decoder.decode(legacyError(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(error.getMetadata()).isEmpty();
    }

    private static Throwable legacyError(com.google.rpc.Status grpcStatus) {
        var trailers = new Metadata();
        trailers.put(
                Metadata.Key.of(
                        "grpc-status-details-bin",
                        ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance())),
                grpcStatus);
        return Status.fromCodeValue(grpcStatus.getCode())
                .withDescription(grpcStatus.getMessage())
                .asRuntimeException(trailers);
    }
}
