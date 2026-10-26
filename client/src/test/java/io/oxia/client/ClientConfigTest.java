/*
 * Copyright © 2022-2025 The Oxia Authors
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ClientConfigTest {

    private static final Duration D = Duration.ofSeconds(1);

    @Test
    void defaultsSubscriptionMaxAge() {
        assertThat(config("addr", D, D, "id", "ns", D, D).subscriptionMaxAge())
                .isEqualTo(OxiaClientBuilderImpl.DefaultSubscriptionMaxAge);
    }

    static Stream<Arguments> nullArguments() {
        return Stream.of(
                Arguments.of(null, D, D, "id", "ns", D, D),
                Arguments.of("addr", null, D, "id", "ns", D, D),
                Arguments.of("addr", D, null, "id", "ns", D, D),
                Arguments.of("addr", D, D, null, "ns", D, D),
                Arguments.of("addr", D, D, "id", null, D, D),
                Arguments.of("addr", D, D, "id", "ns", null, D),
                Arguments.of("addr", D, D, "id", "ns", D, null));
    }

    @ParameterizedTest
    @MethodSource("nullArguments")
    void rejectsNullArguments(
            String serviceAddress,
            Duration requestTimeout,
            Duration sessionTimeout,
            String clientIdentifier,
            String namespace,
            Duration backoffMin,
            Duration backoffMax) {
        assertThatThrownBy(
                        () ->
                                config(
                                        serviceAddress,
                                        requestTimeout,
                                        sessionTimeout,
                                        clientIdentifier,
                                        namespace,
                                        backoffMin,
                                        backoffMax))
                .isInstanceOf(NullPointerException.class);
    }

    private static ClientConfig config(
            String serviceAddress,
            Duration requestTimeout,
            Duration sessionTimeout,
            String clientIdentifier,
            String namespace,
            Duration backoffMin,
            Duration backoffMax) {
        return new ClientConfig(
                serviceAddress,
                requestTimeout,
                1,
                1,
                1,
                1,
                1,
                1,
                sessionTimeout,
                clientIdentifier,
                OpenTelemetry.noop(),
                namespace,
                null,
                false,
                backoffMin,
                backoffMax,
                D,
                D,
                1);
    }
}
