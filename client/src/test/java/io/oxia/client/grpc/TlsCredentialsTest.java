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
package io.oxia.client.grpc;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.UnaryOperator;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsCredentialsTest {
    private static final String[] KEY_TYPES = {"EC", "RSA"};

    @TempDir Path dir;

    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> reloadTask = mock(ScheduledFuture.class);
    private TestCertificates.Issued ca;
    private Path certificateFile;
    private Path keyFile;
    private Path trustFile;
    private long modified = System.currentTimeMillis();

    @BeforeEach
    void setup() throws Exception {
        ca = TestCertificates.ca("Test CA");
        certificateFile = dir.resolve("tls.crt");
        keyFile = dir.resolve("tls.key");
        trustFile = dir.resolve("ca.crt");
        doReturn(reloadTask).when(executor).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
    }

    @Test
    void checksTheFilesForChangesOnceAMinute() throws Exception {
        publish(TestCertificates.issue(ca, "client"));

        try (var credentials = identityCredentials()) {
            verify(executor).scheduleWithFixedDelay(any(), eq(1L), eq(1L), eq(MINUTES));
        }
        verify(reloadTask).cancel(false);
    }

    @Test
    void schedulesNothingWithoutFiles() {
        try (var credentials = credentials(b -> b)) {
            assertThat(credentials.keyManager).isNull();
            assertThat(credentials.trustManager).isNull();
        }
        verifyNoInteractions(executor);
    }

    @Test
    void reloadsTheCertificateWhenOnlyItChanges() throws Exception {
        var first = TestCertificates.issue(ca, "client");
        publish(first);
        try (var credentials = identityCredentials()) {
            var alias = credentials.keyManager.chooseEngineClientAlias(KEY_TYPES, null, null);
            credentials.reload();
            assertThat(credentials.keyManager.chooseEngineClientAlias(KEY_TYPES, null, null))
                    .isEqualTo(alias);

            // A renewal with the same key replaces only the certificate.
            var renewal = TestCertificates.issue(ca, "client", first.keyPair());
            touch(renewal.writeCertificate(dir, "tls"));
            credentials.reload();

            assertThat(presented(credentials)).isEqualTo(renewal.certificate());
        }
    }

    @Test
    void keepsThePairWhenTheNewKeyDoesNotMatch() throws Exception {
        var first = TestCertificates.issue(ca, "client");
        publish(first);
        try (var credentials = identityCredentials()) {
            var second = TestCertificates.issue(ca, "client");
            touch(second.writeCertificate(dir, "tls"));
            touch(TestCertificates.issue(ca, "other").writeKey(dir, "tls"));
            credentials.reload();
            assertThat(presented(credentials)).isEqualTo(first.certificate());

            publish(second);
            credentials.reload();
            assertThat(presented(credentials)).isEqualTo(second.certificate());
        }
    }

    @Test
    void keepsReloadingAfterAMalformedKey() throws Exception {
        var first = TestCertificates.issue(ca, "client");
        publish(first);
        try (var credentials = identityCredentials()) {
            var second = TestCertificates.issue(ca, "client");
            touch(second.writeCertificate(dir, "tls"));
            Files.writeString(keyFile, "-----BEGIN PRIVATE KEY-----\nAAAAA\n-----END PRIVATE KEY-----\n");
            var malformedAt = touch(keyFile);
            credentials.reload();
            assertThat(presented(credentials)).isEqualTo(first.certificate());

            // The key is complete now, with the time of the malformed write: the failed reload didn't
            // record it.
            second.writeKey(dir, "tls");
            Files.setLastModifiedTime(keyFile, malformedAt);
            credentials.reload();
            assertThat(presented(credentials)).isEqualTo(second.certificate());
        }
    }

    @Test
    void servesAHandshakeTheChosenPairThroughAReplacement() throws Exception {
        var first = TestCertificates.issue(ca, "client");
        publish(first);
        try (var credentials = identityCredentials()) {
            var keyManager = credentials.keyManager;
            var alias = keyManager.chooseEngineClientAlias(KEY_TYPES, null, null);

            var second = TestCertificates.issue(ca, "client");
            publish(second);
            credentials.reload();

            // A handshake that chose its alias before the replacement gets both halves of that pair...
            assertThat(keyManager.getPrivateKey(alias)).isEqualTo(first.keyPair().getPrivate());
            assertThat(keyManager.getCertificateChain(alias)).containsExactly(first.certificate());
            // ...and a new one gets the new pair.
            var newAlias = keyManager.chooseEngineClientAlias(KEY_TYPES, null, null);
            assertThat(newAlias).isNotEqualTo(alias);
            assertThat(keyManager.getCertificateChain(newAlias)).containsExactly(second.certificate());

            // Only the previous pair is kept.
            publish(TestCertificates.issue(ca, "client"));
            credentials.reload();
            assertThat(keyManager.getPrivateKey(alias)).isNull();
            assertThat(keyManager.getCertificateChain(alias)).isNull();
        }
    }

    @Test
    void readsRsaKeys() throws Exception {
        var rsa = TestCertificates.issue(ca, "client", TestCertificates.keyPair("RSA"));
        publish(rsa);

        try (var credentials = identityCredentials()) {
            assertThat(presented(credentials)).isEqualTo(rsa.certificate());
        }
    }

    @Test
    void rejectsKeyFilesWithoutAUsableKey() throws Exception {
        publish(TestCertificates.issue(ca, "client"));
        List<String> keys =
                List.of(
                        Files.readString(certificateFile),
                        "-----BEGIN PRIVATE KEY-----\n"
                                + Base64.getEncoder().encodeToString(new byte[32])
                                + "\n-----END PRIVATE KEY-----\n");

        for (var key : keys) {
            Files.writeString(keyFile, key);
            assertThatThrownBy(this::identityCredentials)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(keyFile.toString());
        }
    }

    @Test
    void rejectsAnEmptyTrustBundleAndKeepsTheLastOne() throws Exception {
        Files.writeString(trustFile, "");
        assertThatThrownBy(this::trustCredentials)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(trustFile.toString());

        touch(ca.writeCertificate(dir, "ca"));
        try (var credentials = trustCredentials()) {
            Files.writeString(trustFile, "");
            touch(trustFile);
            credentials.reload();
            assertThat(credentials.trustManager.getAcceptedIssuers()).containsExactly(ca.certificate());

            var other = TestCertificates.ca("Other CA");
            touch(other.writeCertificate(dir, "ca"));
            credentials.reload();
            assertThat(credentials.trustManager.getAcceptedIssuers())
                    .containsExactly(other.certificate());
        }
    }

    @Test
    void servesOnlyAClientOnAnSslEngine() throws Exception {
        publish(TestCertificates.issue(ca, "client"));
        touch(ca.writeCertificate(dir, "ca"));
        try (var credentials =
                credentials(
                        b ->
                                b.tlsCertificateFilePath(certificateFile.toString())
                                        .tlsKeyFilePath(keyFile.toString())
                                        .tlsTrustCertsFilePath(trustFile.toString()))) {
            var keyManager = credentials.keyManager;
            var alias = keyManager.chooseEngineClientAlias(KEY_TYPES, null, null);
            assertThat(keyManager.chooseClientAlias(KEY_TYPES, null, new Socket())).isEqualTo(alias);
            assertThat(keyManager.getClientAliases("EC", null)).containsExactly(alias);
            assertThat(keyManager.getServerAliases("EC", null)).isNull();
            assertThat(keyManager.chooseServerAlias("EC", null, null)).isNull();

            var trustManager = credentials.trustManager;
            var chain = new X509Certificate[] {ca.certificate()};
            List<ThrowingCallable> unsupported =
                    List.of(
                            () -> trustManager.checkServerTrusted(chain, "EC"),
                            () -> trustManager.checkServerTrusted(chain, "EC", new Socket()),
                            () -> trustManager.checkClientTrusted(chain, "EC"),
                            () -> trustManager.checkClientTrusted(chain, "EC", new Socket()),
                            () -> trustManager.checkClientTrusted(chain, "EC", (javax.net.ssl.SSLEngine) null));
            unsupported.forEach(
                    call -> assertThatThrownBy(call).isInstanceOf(CertificateException.class));
        }
    }

    private void publish(TestCertificates.Issued identity) throws Exception {
        touch(identity.writeCertificate(dir, "tls"));
        touch(identity.writeKey(dir, "tls"));
    }

    // Each write gets a later time, however coarse the file system's clock.
    private FileTime touch(Path file) throws Exception {
        modified += 10_000;
        var time = FileTime.fromMillis(modified);
        Files.setLastModifiedTime(file, time);
        return time;
    }

    private TlsCredentials identityCredentials() {
        return credentials(
                b ->
                        b.tlsCertificateFilePath(certificateFile.toString())
                                .tlsKeyFilePath(keyFile.toString()));
    }

    private TlsCredentials trustCredentials() {
        return credentials(b -> b.tlsTrustCertsFilePath(trustFile.toString()));
    }

    private TlsCredentials credentials(UnaryOperator<OxiaClientBuilder> tls) {
        var builder = tls.apply(OxiaClientBuilder.create("localhost:6648").enableTls(true));
        return new TlsCredentials(((OxiaClientBuilderImpl) builder).getClientConfig(), executor);
    }

    private static X509Certificate presented(TlsCredentials credentials) {
        var keyManager = credentials.keyManager;
        return keyManager
                .getCertificateChain(keyManager.chooseEngineClientAlias(KEY_TYPES, null, null))[0];
    }
}
