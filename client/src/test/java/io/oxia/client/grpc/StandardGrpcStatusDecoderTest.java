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
import com.google.protobuf.Duration;
import com.google.rpc.ErrorInfo;
import com.google.rpc.RetryInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StandardGrpcStatusDecoderTest {
    private final StandardGrpcStatusDecoder decoder = new StandardGrpcStatusDecoder();

    @Test
    void returnsUnknownForNonGrpcError() {
        var cause = new IllegalStateException("failed");

        var error = decoder.decode(cause);

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(error).hasMessage("failed").hasCause(cause);
    }

    @Test
    void skipsDetailsThatAreNotOxiaErrorInfo() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.NOT_FOUND.value())
                        .setMessage("session not found")
                        .addDetails(
                                Any.pack(
                                        RetryInfo.newBuilder()
                                                .setRetryDelay(Duration.newBuilder().setSeconds(1))
                                                .build()))
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("example.com")
                                                .setReason("NAMESPACE_NOT_FOUND")
                                                .build()))
                        .addDetails(
                                Any.newBuilder()
                                        .setTypeUrl("type.googleapis.com/google.rpc.ErrorInfo")
                                        .setValue(ByteString.copyFrom(new byte[] {0x0A, 0x05})))
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("oxia.io")
                                                .setReason("SESSION_NOT_FOUND")
                                                .putMetadata("session", "1")
                                                .build()))
                        .build();

        var error = decoder.decode(StatusProto.toStatusRuntimeException(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.SESSION_NOT_FOUND);
        assertThat(error.getMetadata()).containsExactlyEntriesOf(Map.of("session", "1"));
    }

    @Test
    void fallsBackToGrpcCodeForUnknownReason() {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.UNAVAILABLE.value())
                        .addDetails(
                                Any.pack(
                                        ErrorInfo.newBuilder()
                                                .setDomain("oxia.io")
                                                .setReason("NOT_A_STATUS_CODE")
                                                .build()))
                        .build();

        var error = decoder.decode(StatusProto.toStatusRuntimeException(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
        assertThat(error).hasMessage("oxia status RESOURCE_UNAVAILABLE");
    }
}
