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

import dev.failsafe.Timeout;
import dev.failsafe.TimeoutExceededException;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.RequestTimeoutException;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

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
    void keepsOtherErrorsAsIs() {
        var expected = new CompletionException(new KeyAlreadyExistsException("key"));
        assertThat(OxiaExceptions.translate(expected)).isSameAs(expected);

        // Not translated yet
        var unavailable = OxiaStatusException.resourceUnavailable("unavailable");
        assertThat(OxiaExceptions.translate(unavailable)).isSameAs(unavailable);

        var clientError = new IllegalStateException("Client has been closed");
        assertThat(OxiaExceptions.translate(clientError)).isSameAs(clientError);

        var error = new OutOfMemoryError();
        assertThat(OxiaExceptions.translate(error)).isSameAs(error);
    }
}
