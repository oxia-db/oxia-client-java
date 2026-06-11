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
}
