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
package io.oxia.client.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class CompletableFuturesTest {
    @Test
    void unwrapsCompletionAndExecutionExceptions() {
        var cause = new IllegalStateException("failed");

        assertThat(
                        CompletableFutures.unwrapException(
                                new CompletionException(new ExecutionException(cause))))
                .isSameAs(cause);
    }

    @Test
    void returnsOriginalExceptionWhenThereIsNoWrapper() {
        var cause = new IllegalStateException("failed");

        assertThat(CompletableFutures.unwrapException(cause)).isSameAs(cause);
    }

    @Test
    void keepsWrapperWithoutCause() {
        var error = new CompletionException(null);

        assertThat(CompletableFutures.unwrapException(error)).isSameAs(error);
    }

    @Test
    void awaitReturnsTheValue() throws Exception {
        assertThat(CompletableFutures.await(CompletableFuture.completedFuture("value")))
                .isEqualTo("value");
    }

    @Test
    void awaitThrowsTheCauseOfTheFailure() {
        var oxiaException = new KeyAlreadyExistsException("key");
        var runtimeException = new IllegalStateException("failed");
        var error = new AssertionError("failed");

        assertThatThrownBy(
                        () -> CompletableFutures.await(CompletableFuture.failedFuture(oxiaException)))
                .isSameAs(oxiaException);
        assertThatThrownBy(
                        () -> CompletableFutures.await(CompletableFuture.failedFuture(runtimeException)))
                .isSameAs(runtimeException);
        assertThatThrownBy(() -> CompletableFutures.await(CompletableFuture.failedFuture(error)))
                .isSameAs(error);
    }

    @Test
    void awaitWrapsOtherCheckedExceptions() {
        var cause = new IOException("failed");

        assertThatThrownBy(() -> CompletableFutures.await(CompletableFuture.failedFuture(cause)))
                .isInstanceOf(OxiaUncheckedException.class)
                .hasCause(cause);
    }

    @Test
    void awaitWrapsTheInterruption() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> CompletableFutures.await(new CompletableFuture<>()))
                    .isInstanceOf(OxiaUncheckedException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
