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

import static io.oxia.client.grpc.OxiaStatusCode.TIMEOUT;

import dev.failsafe.TimeoutExceededException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
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

    /** Returns the public exception for an internal error. Any other error is returned as is. */
    static Throwable translate(Throwable error) {
        final Throwable cause = CompletableFutures.unwrapException(error);
        if (cause instanceof OxiaStatusException
                || cause instanceof TimeoutException
                || cause instanceof TimeoutExceededException) {
            final OxiaStatusException status = OxiaStatusException.from(cause);
            if (status.getStatusCode() == TIMEOUT) {
                return new RequestTimeoutException(status.getMessage(), status);
            }
        }
        // Keep the original error, e.g. a stackless completion exception, which the dependent stages
        // pass on as is
        return error;
    }
}
