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

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.auth.TokenAuthentication;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
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
