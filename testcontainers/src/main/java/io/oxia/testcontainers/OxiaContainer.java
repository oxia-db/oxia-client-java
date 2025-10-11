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

import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import lombok.NonNull;
import lombok.With;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class OxiaContainer extends GenericContainer<OxiaContainer> {
    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;
    private static final int DEFAULT_SHARDS = 1;

    @With(PRIVATE)
    private final @NonNull DockerImageName imageName;

    @With private final int shards;

    public static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("oxia/oxia:main");

    public OxiaContainer(@NonNull DockerImageName imageName) {
        this(imageName, DEFAULT_SHARDS, false);
    }

    public OxiaContainer(@NonNull DockerImageName imageName, int shards) {
        this(imageName, shards, false);
    }

    @SuppressWarnings("resource")
    public OxiaContainer(@NonNull DockerImageName imageName, int shards, boolean fixedServicePort) {
        super(imageName);
        this.imageName = imageName;
        this.shards = shards;
        if (shards <= 0) {
            throw new IllegalArgumentException("shards must be greater than zero");
        }
        if (fixedServicePort) {
            int freePort = 0;
            try {
                freePort = findFreePort();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            addFixedExposedPort(freePort, OXIA_PORT);
            addExposedPorts(METRICS_PORT);
        } else {
            addExposedPorts(OXIA_PORT, METRICS_PORT);
        }
        setCommand("oxia", "standalone", "--shards=" + shards);
        waitingFor(
                Wait.forHttp("/metrics")
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(30)));
    }

    private static int findFreePort() throws IOException {
        for (int i = 10000; i <= 20000; i++) {
            try (ServerSocket socket = new ServerSocket(i)) {
                return i;
            } catch (Throwable ignore) {

            }
        }
        throw new IOException("No free port found in the specified range");
    }

    public String getServiceAddress() {
        return getHost() + ":" + getMappedPort(OXIA_PORT);
    }

    public String getMetricsUrl() {
        return "http://" + getHost() + ":" + getMappedPort(METRICS_PORT) + "/metrics";
    }
}
