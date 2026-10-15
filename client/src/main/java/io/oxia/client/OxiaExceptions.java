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

import static io.oxia.client.grpc.OxiaStatusCode.UNKNOWN;

import dev.failsafe.TimeoutExceededException;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.oxia.client.api.exceptions.AlreadyClosedException;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.NamespaceNotFoundException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.api.exceptions.UnknownException;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.util.CompletableFutures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Translates the internal errors of the client, such as {@link OxiaStatusException}, to the public
 * {@link OxiaException} subclasses, at the boundary of the client API.
 */
final class OxiaExceptions {
    private OxiaExceptions() {}

    /** Returns a future failed with the public exception for an error, for exceptionallyCompose. */
    static <T> CompletableFuture<T> failedFuture(Throwable error) {
        return CompletableFuture.failedFuture(translate(error));
    }

    /**
     * Returns the public exception for an error, to throw from a method that declares no checked
     * exception: an {@link OxiaException} is wrapped in an {@link OxiaUncheckedException}.
     */
    static RuntimeException unchecked(Throwable error) {
        final Throwable translated = translate(error);
        return translated instanceof RuntimeException e ? e : new OxiaUncheckedException(translated);
    }

    /** Returns the public exception for an internal error. Any other error is returned as is. */
    static Throwable translate(Throwable error) {
        final Throwable cause = CompletableFutures.unwrapException(error);
        if (cause instanceof OxiaStatusException
                || cause instanceof StatusException
                || cause instanceof StatusRuntimeException
                || cause instanceof TimeoutException
                || cause instanceof TimeoutExceededException) {
            final OxiaStatusException status = OxiaStatusException.from(cause);
            switch (status.getStatusCode()) {
                case RESOURCE_UNAVAILABLE,
                        NODE_IS_NOT_LEADER,
                        NODE_IS_NOT_MEMBER,
                        NOT_INITIALIZED,
                        ABORTED -> {
                    return new ResourceUnavailableException(status.getMessage(), status);
                }
                case ALREADY_CLOSED -> {
                    return new AlreadyClosedException(status.getMessage(), status);
                }
                case INVALID_ARGUMENT -> {
                    return new InvalidArgumentException(status.getMessage(), status);
                }
                case NAMESPACE_NOT_FOUND -> {
                    return new NamespaceNotFoundException(status.getMessage(), status);
                }
                case TIMEOUT -> {
                    return new RequestTimeoutException(status.getMessage(), status);
                }
                default -> {
                    // An unrecognized status keeps the message of the original error, e.g. the name
                    // of a gRPC status
                    final String message =
                            status.getStatusCode() == UNKNOWN ? cause.getMessage() : status.getMessage();
                    return new UnknownException(status.getStatusCode() + ": " + message, status);
                }
            }
        }
        // Keep the original error, e.g. a stackless completion exception, which the dependent stages
        // pass on as is
        return error;
    }
}
