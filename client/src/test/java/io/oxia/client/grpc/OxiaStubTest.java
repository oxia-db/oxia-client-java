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

import io.grpc.InsecureChannelCredentials;
import io.grpc.TlsChannelCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OxiaStubTest {

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
