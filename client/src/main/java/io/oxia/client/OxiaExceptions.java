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

import dev.failsafe.TimeoutExceededException;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.oxia.client.api.exceptions.ClientClosedException;
import io.oxia.client.api.exceptions.InvalidArgumentException;
import io.oxia.client.api.exceptions.NamespaceNotFoundException;
import io.oxia.client.api.exceptions.OxiaClientException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.OxiaServerException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.client.grpc.OxiaStatusException;
import io.oxia.client.util.ClosedException;
import io.oxia.client.util.CompletableFutures;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Translates the errors of the client internals, such as {@link OxiaStatusException}, the gRPC
 * status errors and the errors of the client itself, to the public {@link OxiaException}
 * subclasses, at the boundary of the client API.
 */
final class OxiaExceptions {
    private OxiaExceptions() {}

    /** Returns a future failed with the public exception for an error, for exceptionallyCompose. */
    static <T> CompletableFuture<T> failedFuture(Throwable error) {
        return CompletableFuture.failedFuture(translate(error));
    }

    /**
     * Returns the public exception for an error: an {@link OxiaException} for any error but a JVM
     * {@link Error}.
     */
    static Throwable translate(Throwable error) {
        final Throwable cause = CompletableFutures.unwrapException(error);
        if (cause instanceof OxiaException || cause instanceof Error) {
            // Keep the original error, e.g. a stackless completion exception, which the dependent
            // stages pass on as is
            return error;
        }
        if (cause instanceof io.oxia.client.shard.NamespaceNotFoundException) {
            return new NamespaceNotFoundException(cause.getMessage(), cause);
        }
        if (cause instanceof ClosedException) {
            return new ClientClosedException(cause.getMessage(), cause);
        }
        if (cause instanceof IllegalArgumentException) {
            return new InvalidArgumentException(cause.getMessage(), cause);
        }
        if (!(cause instanceof OxiaStatusException
                || cause instanceof StatusException
                || cause instanceof StatusRuntimeException
                || cause instanceof TimeoutException
                || cause instanceof TimeoutExceededException)) {
            return new OxiaClientException("Unexpected error: " + cause, cause);
        }
        final OxiaStatusException status = OxiaStatusException.from(cause);
        final String message = status.getMessage();
        return switch (status.getStatusCode()) {
            case RESOURCE_UNAVAILABLE -> new ResourceUnavailableException(message, status);
            case TIMEOUT -> new RequestTimeoutException(message, status);
            case NAMESPACE_NOT_FOUND -> new NamespaceNotFoundException(message, status);
            default -> new OxiaServerException(status.getStatusCode() + ": " + message, status);
        };
    }
}
