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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.oxia.client.api.exceptions.AlreadyClosedException;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.NamespaceNotFoundException;
import io.oxia.client.api.exceptions.OxiaClientException;
import io.oxia.client.api.exceptions.OxiaServerException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.grpc.OxiaStatusException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class OxiaExceptionsTest {

    @ParameterizedTest
    @CsvSource({
        "RESOURCE_UNAVAILABLE, io.oxia.client.api.exceptions.ResourceUnavailableException",
        "NAMESPACE_NOT_FOUND, io.oxia.client.api.exceptions.NamespaceNotFoundException",
        "SESSION_NOT_FOUND, io.oxia.client.api.exceptions.OxiaServerException",
        "INVALID_SESSION_TIMEOUT, io.oxia.client.api.exceptions.OxiaServerException",
        "NODE_IS_NOT_LEADER, io.oxia.client.api.exceptions.OxiaServerException",
        "SHARD_NOT_FOUND, io.oxia.client.api.exceptions.OxiaServerException",
    })
    void translatesOxiaStatus(String reason, Class<?> expected) {
        var error = oxiaStatusError(reason);

        var translated = OxiaExceptions.translate(new CompletionException(error));

        assertThat(translated).isInstanceOf(expected);
        assertThat(translated.getCause())
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        cause -> assertThat(cause.getStatusCode().name()).isEqualTo(reason));
    }

    @Test
    void otherStatusesKeepTheirNameInTheMessage() {
        assertThat(OxiaExceptions.translate(oxiaStatusError("NODE_IS_NOT_LEADER")))
                .isInstanceOf(OxiaServerException.class)
                .hasMessageStartingWith("NODE_IS_NOT_LEADER: ");
    }

    @Test
    void translatesClientSideStatuses() {
        assertThat(OxiaExceptions.translate(OxiaStatusException.alreadyClosed("closed")))
                .isInstanceOf(AlreadyClosedException.class)
                .hasMessage("closed");
        assertThat(OxiaExceptions.translate(OxiaStatusException.resourceUnavailable("unavailable")))
                .isInstanceOf(ResourceUnavailableException.class)
                .hasMessage("unavailable");
        assertThat(OxiaExceptions.translate(new TimeoutException()))
                .isInstanceOf(RequestTimeoutException.class);
        // A plain gRPC status without an Oxia reason, e.g. the server is unreachable
        assertThat(OxiaExceptions.translate(Status.UNAVAILABLE.asRuntimeException()))
                .isInstanceOf(ResourceUnavailableException.class);
    }

    @Test
    void keepsPublicExceptionsAndJvmErrorsAsIs() {
        var expected = new CompletionException(new KeyAlreadyExistsException("key"));
        assertThat(OxiaExceptions.translate(expected)).isSameAs(expected);

        var error = new OutOfMemoryError();
        assertThat(OxiaExceptions.translate(error)).isSameAs(error);
    }

    @Test
    void translatesClientErrors() {
        var invalid = new IllegalArgumentException("key must not be null");
        assertThat(OxiaExceptions.translate(invalid))
                .isInstanceOf(InvalidArgumentException.class)
                .hasMessage("key must not be null")
                .hasCause(invalid);

        var namespace = new io.oxia.client.shard.NamespaceNotFoundException("ns");
        assertThat(OxiaExceptions.translate(namespace))
                .isInstanceOf(NamespaceNotFoundException.class)
                .hasCause(namespace);

        var unexpected = new NullPointerException("bug");
        assertThat(OxiaExceptions.translate(unexpected))
                .isInstanceOf(OxiaClientException.class)
                .hasCause(unexpected);
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
