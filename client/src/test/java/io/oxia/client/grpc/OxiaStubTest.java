/*
 * Copyright © 2022-2025 StreamNative Inc.
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

    public enum BackoffType {
        GRPC,
        Oxia,
    }

    @ParameterizedTest
    @EnumSource(BackoffType.class)
    public void testOxiaReconnectBackoff(BackoffType type) throws Exception {
        final OxiaStubManager stubManager;
        if (type == BackoffType.Oxia) {
            stubManager = new OxiaStubManager(getDefaultClientConfig(), OxiaBackoffProvider.DEFAULT);
        } else {
            stubManager = new OxiaStubManager(getDefaultClientConfig(), null);
        }

        final OxiaStub stub = stubManager.getStub(oxia.getServiceAddress());
        sendMessage(stub).join();

        oxia.stop();

        // failed to send messages
        long startTime = System.currentTimeMillis();
        long elapse = 30 * 1000;
        while (System.currentTimeMillis() - startTime <= elapse) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                sendMessage(stub).join();
            } catch (Throwable ignore) {
            }
        }

        oxia.start();

        startTime = System.currentTimeMillis();
        elapse = 10 * 1000;
        boolean success = false;
        while (System.currentTimeMillis() - startTime <= elapse) {
            try {
                sendMessage(stub).join();
                success = true;
            } catch (Throwable ignore) {

            }
        }
        if (type == BackoffType.Oxia) {
            Assertions.assertTrue(success);
        } else {
            Assertions.assertFalse(success);
        }
        stubManager.close();
    }

    private static CompletableFuture<Void> sendMessage(OxiaStub stub) {
        final var readRequest =
                ReadRequest.newBuilder()
                        .setShard(0)
                        .addGets(GetRequest.newBuilder().setKey("test").build())
                        .build();
        final CompletableFuture<Void> f = new CompletableFuture<>();
        stub.async()
                .read(
                        readRequest,
                        new StreamObserver<>() {
                            @Override
                            public void onNext(ReadResponse value) {
                                f.complete(null);
                            }

                            @Override
                            public void onError(Throwable t) {
                                f.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                f.complete(null);
                            }
                        });
        return f;
    }

    @Test
    @SneakyThrows
    public void testMaxConnectionPerNode() {
        final var maxConnectionPerNode = 10;
        final var clientConfig =
                getDefaultClientConfig(
                        builder -> {
                            builder.maxConnectionPerNode(maxConnectionPerNode);
                        });
        @Cleanup var stubManager = new OxiaStubManager(clientConfig, OxiaBackoffProvider.DEFAULT);
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
