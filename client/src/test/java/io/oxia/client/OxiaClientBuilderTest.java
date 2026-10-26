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

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.SharedResources;
import io.oxia.client.api.exceptions.UnsupportedAuthenticationException;
import io.oxia.client.auth.TokenAuthentication;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OxiaClientBuilderTest {

    OxiaClientBuilder builder = OxiaClientBuilder.create("address:1234");

    @Test
    void requestTimeout() {
        assertThatThrownBy(() -> builder.requestTimeout(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.requestTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.requestTimeout(Duration.ofMillis(1)));
    }

    @Test
    void subscriptionMaxAge() {
        assertThatThrownBy(() -> builder.subscriptionMaxAge(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.subscriptionMaxAge(Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.subscriptionMaxAge(Duration.ofNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.subscriptionMaxAge(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);

        var timeout = Duration.ofMinutes(1);
        builder.subscriptionMaxAge(timeout);
        var impl = (OxiaClientBuilderImpl) builder;
        assertThat(impl.subscriptionMaxAge).isEqualTo(timeout);

        builder.disableSubscriptionMaxAge();
        assertThat(impl.subscriptionMaxAge).isNull();

        builder.subscriptionMaxAge(timeout);
        assertThat(impl.subscriptionMaxAge).isEqualTo(timeout);
    }

    @Test
    void batchLinger() {
        assertThatThrownBy(() -> builder.batchLinger(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchLinger(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.batchLinger(Duration.ofMillis(1)));
    }

    @Test
    void maxRequestsPerBatch() {
        assertThatThrownBy(() -> builder.maxRequestsPerBatch(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxRequestsPerBatch(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.maxRequestsPerBatch(1));
    }

    @Test
    void sessionTimeout() {
        assertThatThrownBy(() -> builder.sessionTimeout(ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.sessionTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatNoException().isThrownBy(() -> builder.sessionTimeout(Duration.ofMillis(1)));
    }

    @Test
    void connectionBackoff() {
        assertThatThrownBy(() -> builder.connectionBackoff(ZERO, Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.connectionBackoff(Duration.ofMillis(-1), Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.connectionBackoff(Duration.ofMillis(5), Duration.ofMillis(5)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.connectionBackoff(Duration.ofMillis(5), Duration.ofMillis(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.connectionBackoff(null, Duration.ofMillis(1)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.connectionBackoff(Duration.ofMillis(1), null))
                .isInstanceOf(NullPointerException.class);

        builder.connectionBackoff(Duration.ofMillis(1), Duration.ofMillis(2));
        var config = ((OxiaClientBuilderImpl) builder).getClientConfig();
        assertThat(config.connectionBackoffMinDelay()).isEqualTo(Duration.ofMillis(1));
        assertThat(config.connectionBackoffMaxDelay()).isEqualTo(Duration.ofMillis(2));
    }

    @Test
    void loadConfigWithIllegalArgument() {
        assertThatThrownBy(() -> builder.loadConfig("configPath"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.loadConfig(new java.io.File("configFile")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.loadConfig((Properties) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void loadConfigWithProperties() {
        Properties properties = new Properties();
        properties.setProperty("serviceAddress", "address:5678");
        properties.setProperty("requestTimeout", "1");
        properties.setProperty("subscriptionMaxAge", "5");
        properties.setProperty("batchLinger", "2");
        properties.setProperty("maxRequestsPerBatch", "3");
        properties.setProperty("sessionTimeout", "4");
        properties.setProperty("clientIdentifier", "client-identifier");
        properties.setProperty("namespace", "namespace");
        properties.setProperty("enableTls", "true");
        properties.setProperty("authPluginClassName", TokenAuthentication.class.getName());
        properties.setProperty("authParams", "token:1234");
        builder.loadConfig(properties);
        OxiaClientBuilderImpl impl = (OxiaClientBuilderImpl) builder;
        assertThat(impl.serviceAddress).isEqualTo("address:5678");
        assertThat(impl.requestTimeout).isEqualTo(Duration.ofMillis(1));
        assertThat(impl.subscriptionMaxAge).isEqualTo(Duration.ofMillis(5));
        assertThat(impl.batchLinger).isEqualTo(Duration.ofMillis(2));
        assertThat(impl.maxRequestsPerBatch).isEqualTo(3);
        assertThat(impl.sessionTimeout).isEqualTo(Duration.ofMillis(4));
        assertThat(impl.clientIdentifier).isEqualTo("client-identifier");
        assertThat(impl.namespace).isEqualTo("namespace");
        assertThat(impl.enableTls).isTrue();
        assertThat(impl.authPluginClassName).isEqualTo(TokenAuthentication.class.getName());
        assertThat(impl.authParams).isEqualTo("token:1234");
        assertThat(impl.authentication).isNotNull();
        assertThat(impl.authentication).isInstanceOf(TokenAuthentication.class);
        Map<String, String> metadata = impl.authentication.generateCredentials();
        assertThat(metadata).isNotNull();
        String token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 1234");
    }

    @Test
    void loadConfigFromFile() {
        builder.loadConfig("src/test/resources/test-client.properties");
        OxiaClientBuilderImpl impl = (OxiaClientBuilderImpl) builder;
        assertThat(impl.serviceAddress).isEqualTo("address:5678");
        assertThat(impl.requestTimeout).isEqualTo(Duration.ofMillis(1));
        assertThat(impl.batchLinger).isEqualTo(Duration.ofMillis(2));
        assertThat(impl.maxRequestsPerBatch).isEqualTo(3);
        assertThat(impl.sessionTimeout).isEqualTo(Duration.ofMillis(4));
        assertThat(impl.clientIdentifier).isEqualTo("client-identifier");
        assertThat(impl.namespace).isEqualTo("namespace");
        assertThat(impl.enableTls).isTrue();
        assertThat(impl.authPluginClassName).isEqualTo(TokenAuthentication.class.getName());
        assertThat(impl.authParams).isEqualTo("token:1234");
        assertThat(impl.authentication).isNotNull();
        assertThat(impl.authentication).isInstanceOf(TokenAuthentication.class);
        Map<String, String> metadata = impl.authentication.generateCredentials();
        assertThat(metadata).isNotNull();
        String token = metadata.get("Authorization");
        assertThat(token).isEqualTo("Bearer 1234");
    }

    @Test
    void loadConfigPreservesPreSetAuthentication() {
        Authentication preSet =
                new Authentication() {
                    @Override
                    public Map<String, String> generateCredentials() {
                        return Map.of("X-Pre-Set", "yes");
                    }

                    @Override
                    public void configure(String params) {}
                };

        Properties properties = new Properties();
        properties.setProperty("requestTimeout", "1");

        builder.authentication(preSet).loadConfig(properties);

        OxiaClientBuilderImpl impl = (OxiaClientBuilderImpl) builder;
        assertThat(impl.authentication).isSameAs(preSet);
        assertThat(impl.authentication.generateCredentials()).containsEntry("X-Pre-Set", "yes");
    }

    @Test
    void rejectsNullArguments() {
        var impl = (OxiaClientBuilderImpl) builder;
        assertThatThrownBy(() -> new OxiaClientBuilderImpl(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> OxiaClientBuilder.create(null))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.requestTimeout(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> impl.batchLinger(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.namespace(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.sessionTimeout(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.clientIdentifier((String) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.clientIdentifier((Supplier<String>) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.openTelemetry(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> builder.subscriptionMaxAge(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void validatesArguments() {
        assertThatThrownBy(() -> builder.maxPendingBytes(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxWriteBatchesInFlight(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxReadBatchesInFlight(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.batchingThreads(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.namespace("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.maxConnectionPerNode(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.subscriptionMaxAge(Duration.ofSeconds(Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    void clientConfig() throws Exception {
        var minDelay = Duration.ofMillis(5);
        var maxDelay = Duration.ofSeconds(5);
        var config =
                ((OxiaClientBuilderImpl)
                                builder
                                        .maxPendingBytes(0)
                                        .maxWriteBatchesInFlight(2)
                                        .maxReadBatchesInFlight(3)
                                        .batchingThreads(4)
                                        .maxConnectionPerNode(5)
                                        .connectionBackoff(minDelay, maxDelay)
                                        .clientIdentifier(() -> "supplied")
                                        .authentication(TokenAuthentication.class.getName(), "token:1234"))
                        .getClientConfig();
        assertThat(config.maxPendingBytes()).isZero();
        assertThat(config.maxWriteBatchesInFlight()).isEqualTo(2);
        assertThat(config.maxReadBatchesInFlight()).isEqualTo(3);
        assertThat(config.batchingThreads()).isEqualTo(4);
        assertThat(config.maxConnectionPerNode()).isEqualTo(5);
        assertThat(config.connectionBackoffMinDelay()).isEqualTo(minDelay);
        assertThat(config.connectionBackoffMaxDelay()).isEqualTo(maxDelay);
        assertThat(config.clientIdentifier()).isEqualTo("supplied");
        assertThat(config.authentication().generateCredentials())
                .containsEntry("Authorization", "Bearer 1234");

        var openTelemetry = OpenTelemetry.noop();
        config =
                ((OxiaClientBuilderImpl) builder.clientIdentifier("fixed").openTelemetry(openTelemetry))
                        .getClientConfig();
        assertThat(config.clientIdentifier()).isEqualTo("fixed");
        assertThat(config.openTelemetry()).isSameAs(openTelemetry);
    }

    @Test
    void loadConfigRejectsUnknownOrConstantProperties() {
        var unknown = new Properties();
        unknown.setProperty("unknown", "1");
        assertThatThrownBy(() -> builder.loadConfig(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");

        var constant = new Properties();
        constant.setProperty("DefaultNamespace", "ns");
        assertThatThrownBy(() -> builder.loadConfig(constant))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DefaultNamespace");
    }

    @Test
    void loadConfigRejectsUnknownAuthPlugin() {
        var properties = new Properties();
        properties.setProperty("authPluginClassName", "io.oxia.client.auth.Missing");
        assertThatThrownBy(() -> builder.loadConfig(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasCauseInstanceOf(UnsupportedAuthenticationException.class);
    }

    @Test
    void loadConfigRecordsTransportSettings() {
        var properties = new Properties();
        properties.setProperty("maxConnectionsPerNode", "2");
        properties.setProperty("connectionKeepAliveTime", "1000");
        properties.setProperty("connectionKeepAliveTimeout", "1000");
        properties.setProperty("batchingThreads", "2");
        properties.setProperty("authParams", "token:1234");
        try (var shared = SharedResources.builder().numWorkerThreads(1).build()) {
            assertThatThrownBy(() -> builder.loadConfig(properties).sharedResources(shared).asyncClient())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContainingAll(
                            "maxConnectionPerNode",
                            "connectionKeepAliveTime",
                            "connectionKeepAliveTimeout",
                            "batchingThreads",
                            "authentication");
        }
        var impl = (OxiaClientBuilderImpl) builder;
        assertThat(impl.maxConnectionsPerNode).isEqualTo(2);
        assertThat(impl.authentication).isNull();
    }

    @Test
    void connectionKeepAlive() {
        final var keepAliveTime = Duration.ofMillis(10);
        final var keepAliveTimeout = Duration.ofMillis(10);
        builder.connectionKeepAliveTime(keepAliveTime);
        builder.connectionKeepAliveTimeout(keepAliveTimeout);
        final var impl = (OxiaClientBuilderImpl) builder;
        Assertions.assertEquals(keepAliveTimeout, impl.connectionKeepAliveTimeout);
        Assertions.assertEquals(keepAliveTime, impl.connectionKeepAliveTime);
    }
}
