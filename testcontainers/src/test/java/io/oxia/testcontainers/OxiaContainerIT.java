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
package io.oxia.testcontainers;

import static io.oxia.testcontainers.OxiaContainer.DEFAULT_IMAGE_NAME;
import static io.oxia.testcontainers.OxiaContainer.OXIA_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class OxiaContainerIT {
    private static final String NETWORK_ALIAS = "oxia";
    private static final Network network = Network.newNetwork();

    @Container
    private static OxiaContainer standalone =
            new OxiaContainer(DEFAULT_IMAGE_NAME).withNetworkAliases(NETWORK_ALIAS).withNetwork(network);

    @Container
    private static OxiaContainer cli =
            new OxiaContainer(DEFAULT_IMAGE_NAME)
                    .withNetwork(network)
                    .withCommand("tail", "-f", "/dev/null")
                    .waitingFor(noopWaitStrategy());

    @Test
    void testPutGetWithCLI() throws Exception {
        var address = NETWORK_ALIAS + ":" + OXIA_PORT;

        var result = cli.execInContainer("oxia", "client", "-a", address, "put", "hello", "world");
        assertThat(result.getExitCode()).isEqualTo(0);

        result = cli.execInContainer("oxia", "client", "-a", address, "get", "hello");
        assertThat(result.getStdout()).contains("world");
    }

    @Test
    public void testMetricsUrl() throws Exception {
        var httpClient = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(standalone.getMetricsUrl())).build();
        var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("oxia");
    }

    private static WaitStrategy noopWaitStrategy() {
        return new AbstractWaitStrategy() {
            @Override
            protected void waitUntilReady() {}
        };
    }
}
