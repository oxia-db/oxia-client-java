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
package io.oxia.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import dev.failsafe.Timeout;
import dev.failsafe.TimeoutExceededException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.NamespaceNotFoundException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OxiaExceptionsTest {

    @Test
    void translatesTimeouts() {
        var timeout = new TimeoutException();
        assertThat(OxiaExceptions.translate(new CompletionException(timeout)))
                .isInstanceOf(RequestTimeoutException.class)
                .hasMessage("Request timed out")
                .cause()
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        status -> assertThat(status.getStatusCode()).isEqualTo(OxiaStatusCode.TIMEOUT))
                .hasCause(timeout);

        var status = OxiaStatusException.timeout(new TimeoutException());
        assertThat(OxiaExceptions.translate(status))
                .isInstanceOf(RequestTimeoutException.class)
                .hasCause(status);

        assertThat(
                        OxiaExceptions.translate(
                                new TimeoutExceededException(Timeout.of(Duration.ofSeconds(1)))))
                .isInstanceOf(RequestTimeoutException.class);
    }

    @Test
    void translatesResourceUnavailable() {
        var unavailable = OxiaStatusException.resourceUnavailable("Write stream is closed");
        assertThat(OxiaExceptions.translate(new CompletionException(unavailable)))
                .isInstanceOf(ResourceUnavailableException.class)
                .hasMessage("Write stream is closed")
                .hasCause(unavailable);

        // A plain gRPC status without an Oxia reason, e.g. the server is unreachable
        assertThat(OxiaExceptions.translate(Status.UNAVAILABLE.asRuntimeException()))
                .isInstanceOf(ResourceUnavailableException.class);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "RESOURCE_UNAVAILABLE",
                "NODE_IS_NOT_LEADER",
                "NODE_IS_NOT_MEMBER",
                "NOT_INITIALIZED",
                "ABORTED"
            })
    void translatesTemporaryServerStatuses(String reason) {
        assertThat(OxiaExceptions.translate(oxiaStatusError(reason)))
                .isInstanceOf(ResourceUnavailableException.class)
                .cause()
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        status -> assertThat(status.getStatusCode().name()).isEqualTo(reason));
    }

    @Test
    void translatesNamespaceNotFound() {
        assertThat(OxiaExceptions.translate(oxiaStatusError("NAMESPACE_NOT_FOUND")))
                .isInstanceOf(NamespaceNotFoundException.class)
                .cause()
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        status ->
                                assertThat(status.getStatusCode()).isEqualTo(OxiaStatusCode.NAMESPACE_NOT_FOUND));

        // The namespace is missing from the shard assignments
        var namespace = OxiaStatusException.namespaceNotFound("ns");
        assertThat(OxiaExceptions.translate(new CompletionException(namespace)))
                .isInstanceOf(NamespaceNotFoundException.class)
                .hasMessage("Namespace not found: ns")
                .hasCause(namespace);
    }

    @Test
    void keepsOtherErrorsAsIs() {
        var expected = new CompletionException(new KeyAlreadyExistsException("key"));
        assertThat(OxiaExceptions.translate(expected)).isSameAs(expected);

        // Not translated yet
        var sessionNotFound = oxiaStatusError("SESSION_NOT_FOUND");
        assertThat(OxiaExceptions.translate(sessionNotFound)).isSameAs(sessionNotFound);

        var clientError = new IllegalStateException("Client has been closed");
        assertThat(OxiaExceptions.translate(clientError)).isSameAs(clientError);

        var error = new OutOfMemoryError();
        assertThat(OxiaExceptions.translate(error)).isSameAs(error);
    }

    private static StatusRuntimeException oxiaStatusError(String reason) {
        var grpcStatus =
                com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.FAILED_PRECONDITION.value())
                        .setMessage("oxia: " + reason.toLowerCase())
                        .addDetails(
                                Any.pack(ErrorInfo.newBuilder().setDomain("oxia.io").setReason(reason).build()))
                        .build();
        return StatusProto.toStatusRuntimeException(grpcStatus);
    }
}
