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
package io.oxia.client.grpc;

import static io.oxia.client.util.ConfigUtils.*;

import io.grpc.InsecureChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.StreamObserver;
import io.oxia.proto.GetRequest;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import io.oxia.testcontainers.OxiaContainer;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@Slf4j
public class OxiaStubTest {
    @Container
    private static final OxiaContainer oxia =
            new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME, 4, true)
                    .withLogConsumer(new Slf4jLogConsumer(log));

    @Test
    @SneakyThrows
    public void testMaxConnectionPerNode() {
        final var maxConnectionPerNode = 10;
        final var clientConfig =
                getDefaultClientConfig(
                        builder -> {
                            builder.maxConnectionPerNode(maxConnectionPerNode);
                        });
        @Cleanup var stubManager = new OxiaStubManager(clientConfig);
        for (int i = 0; i < 1000; i++) {
            stubManager.getStub(oxia.getServiceAddress());
        }
        Assertions.assertEquals(maxConnectionPerNode, stubManager.stubs.size());
    }

    @Test
    public void testAddressTrim() {
        final var tlsAddress = "tls://localhost:6648";
        Assertions.assertEquals("localhost:6648", OxiaStub.getAddress(tlsAddress));

        final var planTxtAddress = "localhost:6648";
        Assertions.assertEquals("localhost:6648", OxiaStub.getAddress(planTxtAddress));
    }

    @Test
    public void testTlsCredential() {
        final var tlsAddress = "tls://localhost:6648";
        var channelCredential = OxiaStub.getChannelCredential(tlsAddress, false);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);

        channelCredential = OxiaStub.getChannelCredential(tlsAddress, true);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);

        final var planTxtAddress = "localhost:6648";
        channelCredential = OxiaStub.getChannelCredential(planTxtAddress, false);
        Assertions.assertInstanceOf(InsecureChannelCredentials.class, channelCredential);

        channelCredential = OxiaStub.getChannelCredential(planTxtAddress, true);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);
    }
}
