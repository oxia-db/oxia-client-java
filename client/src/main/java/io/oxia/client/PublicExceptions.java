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

import dev.failsafe.FailsafeException;
import dev.failsafe.TimeoutExceededException;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.NamespaceNotFoundException;
import io.oxia.client.api.exceptions.OxiaServerException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.util.CompletableFutures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Translates the errors of the client internals, such as {@link OxiaStatusException} and the gRPC
 * status errors, to the public {@link OxiaUncheckedException} subclasses, at the boundary of the
 * client API.
 */
final class PublicExceptions {
    private PublicExceptions() {}

    /** Returns a future failed with the public exception for an error, for exceptionallyCompose. */
    static <T> CompletableFuture<T> failedFuture(Throwable error) {
        return CompletableFuture.failedFuture(translate(error));
    }

    /**
     * Returns the public exception for an error. Errors that are already public, or that carry no
     * status, such as an {@link IllegalStateException} for a closed client, are returned as is.
     */
    static Throwable translate(Throwable error) {
        Throwable cause = CompletableFutures.unwrapException(error);
        while (cause instanceof FailsafeException && cause.getCause() != null) {
            cause = CompletableFutures.unwrapException(cause.getCause());
        }
        if (!(cause instanceof OxiaStatusException
                || cause instanceof StatusException
                || cause instanceof StatusRuntimeException
                || cause instanceof TimeoutException
                || cause instanceof TimeoutExceededException)) {
            // Already public, or no status: keep the original error, e.g. a stackless completion
            // exception, which the dependent stages pass on as is
            return error;
        }
        final OxiaStatusException status = OxiaStatusException.from(cause);
        final String message = status.getMessage();
        return switch (status.getStatusCode()) {
            case RESOURCE_UNAVAILABLE -> new ResourceUnavailableException(message, status);
            case TIMEOUT -> new RequestTimeoutException(message, status);
            case NAMESPACE_NOT_FOUND -> new NamespaceNotFoundException(message, status);
            case SESSION_NOT_FOUND -> withCause(new SessionDoesNotExistException(), status);
            case INVALID_SESSION_TIMEOUT -> withCause(new InvalidArgumentException(message), status);
            default -> new OxiaServerException(status.getStatusCode() + ": " + message, status);
        };
    }

    private static OxiaUncheckedException withCause(
            OxiaUncheckedException exception, Throwable cause) {
        exception.initCause(cause);
        return exception;
    }
}
