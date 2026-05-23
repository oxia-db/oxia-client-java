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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class OxiaStatusExceptionTest {
    @Test
    void convertsOxiaErrorInfoDetails() {
        var grpcStatus = grpcStatus("NAMESPACE_NOT_FOUND", "oxia: namespace not found");

        var error = OxiaStatusException.from(StatusProto.toStatusRuntimeException(grpcStatus));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.NAMESPACE_NOT_FOUND);
        assertThat(error.getMetadata()).containsEntry("shard", "1").containsEntry("leader", "server-1");
        assertThat(error.getLeaderHint(1)).contains("server-1");
        assertThat(error.getLeaderHint(2)).isEmpty();
        assertThat(error).hasMessage("oxia: namespace not found");
        assertThat(error.isRetryable()).isFalse();
    }

    @Test
    void convertsLegacyNamespaceNotFoundStatus() {
        var error =
                OxiaStatusException.from(
                        Status.UNKNOWN.withDescription("oxia: namespace not found").asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.NAMESPACE_NOT_FOUND);
    }

    @Test
    void convertsLegacyCustomCodeMessage() {
        var error =
                OxiaStatusException.from(
                        Status.UNKNOWN.withDescription("node is not leader for shard 1").asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.NODE_IS_NOT_LEADER);
        assertThat(error.isRetryable()).isTrue();
    }

    @Test
    void convertsLegacy0163Messages() {
        Map.ofEntries(
                        entry("oxia: server not initialized yet", OxiaStatusCode.NOT_INITIALIZED),
                        entry("oxia: operation was cancelled", OxiaStatusCode.ABORTED),
                        entry("oxia: invalid term", OxiaStatusCode.INVALID_TERM),
                        entry("oxia: invalid status", OxiaStatusCode.INVALID_STATUS),
                        entry("oxia: leader is already connected", OxiaStatusCode.ABORTED),
                        entry("oxia: resource is already closed", OxiaStatusCode.ABORTED),
                        entry("oxia: session not found", OxiaStatusCode.SESSION_NOT_FOUND),
                        entry("oxia: invalid session timeout", OxiaStatusCode.INVALID_SESSION_TIMEOUT),
                        entry("oxia: namespace not found", OxiaStatusCode.NAMESPACE_NOT_FOUND),
                        entry(
                                "oxia: notifications not enabled on namespace",
                                OxiaStatusCode.NOTIFICATIONS_NOT_ENABLED),
                        entry("oxia: node is not a member", OxiaStatusCode.NODE_IS_NOT_MEMBER))
                .forEach(
                        (message, code) -> {
                            var error =
                                    OxiaStatusException.from(
                                            Status.UNKNOWN.withDescription(message).asRuntimeException());

                            assertThat(error.getStatusCode()).isEqualTo(code);
                        });
    }

    @Test
    void mapsLegacy0163NodeIsNotFollowerToAborted() {
        var error =
                OxiaStatusException.from(
                        Status.UNKNOWN
                                .withDescription("node is not follower for shard 1")
                                .asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.ABORTED);
    }

    @Test
    void mapsLegacy0163AlreadyClosedToRetryableAborted() {
        var error =
                OxiaStatusException.from(
                        Status.UNKNOWN
                                .withDescription("oxia: resource is already closed")
                                .asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.ABORTED);
        assertThat(error.isRetryable()).isTrue();
    }

    @Test
    void doesNotInferCurrentMainMessagesWithoutErrorInfo() {
        for (var message :
                new String[] {
                    "oxia: operation was aborted",
                    "oxia: shard not found",
                    "oxia: node is not leader",
                    "oxia: resource conflict",
                    "oxia: resource unavailable"
                }) {
            var error =
                    OxiaStatusException.from(Status.UNKNOWN.withDescription(message).asRuntimeException());

            assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        }
    }

    @Test
    void identifiesRetryableOxiaErrorInfoDetails() {
        var grpcStatus = grpcStatus("RESOURCE_UNAVAILABLE", "oxia: resource unavailable");

        var error =
                OxiaStatusException.from(
                        new CompletionException(StatusProto.toStatusRuntimeException(grpcStatus)));

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
        assertThat(error.isRetryable()).isTrue();
    }

    @Test
    void mapsGrpcUnavailableToRetryableResourceUnavailable() {
        var error =
                OxiaStatusException.from(
                        Status.UNAVAILABLE.withDescription("connection unavailable").asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE);
        assertThat(error.isRetryable()).isTrue();
    }

    @Test
    void mapsGrpcAbortedToRetryableAborted() {
        var error =
                OxiaStatusException.from(
                        Status.ABORTED.withDescription("operation aborted").asRuntimeException());

        assertThat(error.getStatusCode()).isEqualTo(OxiaStatusCode.ABORTED);
        assertThat(error.isRetryable()).isTrue();
    }

    @Test
    void doesNotTreatGrpcDeadlineExceededAsRetryable() {
        var translated =
                OxiaStatusException.from(
                        Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded").asRuntimeException());

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(translated.isRetryable()).isFalse();
    }

    @Test
    void returnsUnknownOxiaErrorForNonGrpcError() {
        var error = new IllegalStateException("failed");

        var translated = OxiaStatusException.from(error);

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(translated).hasMessage("failed");
        assertThat(translated).hasCause(error);
        assertThat(translated.isRetryable()).isFalse();
    }

    @Test
    void returnsUnwrappedOxiaError() throws ExecutionException {
        var error =
                new OxiaStatusException(
                        OxiaStatusCode.ABORTED, Map.of(), "aborted", new IllegalStateException("cause"));

        assertThat(OxiaStatusException.from(new CompletionException(error))).isSameAs(error);
        assertThat(OxiaStatusException.from(new ExecutionException(error))).isSameAs(error);
    }

    @Test
    void returnsUnknownOxiaErrorForWrappedNonGrpcError() {
        var cause = new IllegalStateException("failed");

        var translated = OxiaStatusException.from(new CompletionException(cause));

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(translated).hasMessage("failed");
        assertThat(translated).hasCause(cause);
    }

    @Test
    void createsShardNotFoundForMissingShardId() {
        var translated = OxiaStatusException.shardNotFound(1);

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
        assertThat(translated).hasMessage("Shard not available : 1");
        assertThat(translated.getMetadata()).containsEntry("shard", "1");
        assertThat(translated).hasNoCause();
        assertThat(translated.isRetryable()).isFalse();
        assertThat(OxiaStatusException.from(new CompletionException(translated))).isSameAs(translated);
    }

    @Test
    void createsShardNotFoundForMissingKeyShard() {
        var translated = OxiaStatusException.shardNotFound("key");

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.SHARD_NOT_FOUND);
        assertThat(translated).hasMessage("No shard available to accept to key: key");
        assertThat(translated.getMetadata()).containsEntry("key", "key");
        assertThat(translated).hasNoCause();
        assertThat(translated.isRetryable()).isFalse();
    }

    @Test
    void fallsBackToUnknownForMismatchedGrpcStatusDetails() {
        var trailers = new Metadata();
        trailers.put(
                Metadata.Key.of(
                        "grpc-status-details-bin",
                        ProtoUtils.metadataMarshaller(com.google.rpc.Status.getDefaultInstance())),
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value())
                        .setMessage("inner aborted")
                        .build());

        var translated =
                OxiaStatusException.from(
                        Status.UNKNOWN.withDescription("outer unknown").asRuntimeException(trailers));

        assertThat(translated.getStatusCode()).isEqualTo(OxiaStatusCode.UNKNOWN);
        assertThat(translated).hasMessageContaining("outer unknown");
    }

    private static com.google.rpc.Status grpcStatus(String reason, String message) {
        return com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.NOT_FOUND.value())
                .setMessage(message)
                .addDetails(
                        Any.pack(
                                ErrorInfo.newBuilder()
                                        .setDomain("oxia.io")
                                        .setReason(reason)
                                        .putMetadata("shard", "1")
                                        .putMetadata("leader", "server-1")
                                        .build()))
                .build();
    }
}
