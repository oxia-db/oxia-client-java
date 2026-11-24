/*
 * Copyright © 2022-2026 The Oxia Authors
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
import static org.awaitility.Awaitility.await;

import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.grpc.TestCertificates;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A client or shared pool whose TLS files fail to load is never handed to the caller, who therefore
 * can't close it: it must stop its threads by itself. The client certificate loads, so its
 * reloading has started a thread by the time the missing trust file fails.
 */
class TlsFilesStartFailureTest {
    @TempDir Path dir;

    private String certificate;
    private String key;
    private String missing;
    private Set<Thread> threadsBefore;

    @BeforeEach
    void setup() throws Exception {
        var client = TestCertificates.issue(TestCertificates.ca("Test CA"), "client");
        certificate = client.writeCertificate(dir, "client").toString();
        key = client.writeKey(dir, "client").toString();
        missing = dir.resolve("missing.crt").toString();
        threadsBefore = Thread.getAllStackTraces().keySet();
    }

    @Test
    void standaloneClientStopsItsThreads() {
        assertThatThrownBy(
                        () ->
                                OxiaClientBuilder.create("localhost:1")
                                        .enableTls(true)
                                        .tlsCertificateFilePath(certificate)
                                        .tlsKeyFilePath(key)
                                        .tlsTrustCertsFilePath(missing)
                                        .asyncClient()
                                        .join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(missing);

        assertOxiaThreadsStopped();
    }

    @Test
    void sharedResourcesStopItsThreads() {
        assertThatThrownBy(
                        () ->
                                SharedResourcesImpl.builder()
                                        .enableTls(true)
                                        .tlsCertificateFilePath(certificate)
                                        .tlsKeyFilePath(key)
                                        .tlsTrustCertsFilePath(missing)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(missing);

        assertOxiaThreadsStopped();
    }

    private void assertOxiaThreadsStopped() {
        await().untilAsserted(() -> assertThat(newOxiaThreads()).filteredOn(Thread::isAlive).isEmpty());
    }

    private List<Thread> newOxiaThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("oxia-client-") && !threadsBefore.contains(t))
                .toList();
    }
}
