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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.annotations.VisibleForTesting;
import io.github.merlimat.slog.Logger;
import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.oxia.client.ClientConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * The credentials of the client's TLS connections, from the PEM files in the client's config.
 *
 * <p>The files are read and checked here, and a file that fails fails the client. They're then read
 * again once a minute on the executor: when the contents of the trusted CAs' file, or of either
 * file of the client's certificate and key, differ from the contents last tried, those contents are
 * checked the same way and replace the previous ones for new connections. Contents that fail keep
 * the previous ones in use, and aren't tried again until they change. Comparing contents rather
 * than modification times catches a copy that keeps a file's time, and costs little for files this
 * small. Without files, the JVM's defaults apply.
 */
final class TlsCredentials implements AutoCloseable {
    private static final Logger log = Logger.get(TlsCredentials.class);
    private static final Pattern PKCS8_KEY =
            Pattern.compile("-----BEGIN PRIVATE KEY-----([^-]+)-----END PRIVATE KEY-----");

    @Getter private final ChannelCredentials channelCredentials;
    @VisibleForTesting @Nullable final ReloadingKeyManager keyManager;
    @VisibleForTesting @Nullable final ReloadingTrustManager trustManager;
    private final List<WatchedFiles> watchedFiles = new ArrayList<>();
    @Nullable private final ScheduledFuture<?> reloadTask;

    TlsCredentials(ClientConfig config, ScheduledExecutorService executor) {
        var builder = TlsChannelCredentials.newBuilder();
        if (config.tlsCertificateFilePath() != null) {
            var certificateFile = Path.of(config.tlsCertificateFilePath());
            var keyFile = Path.of(config.tlsKeyFilePath());
            var manager = new ReloadingKeyManager();
            watchedFiles.add(
                    new WatchedFiles(
                            List.of(certificateFile, keyFile),
                            contents -> {
                                var chain = parseCertificates(certificateFile, contents.get(0));
                                var key = parsePrivateKey(keyFile, contents.get(1));
                                checkKeyMatches(key, chain.get(0));
                                manager.update(key, chain);
                            }));
            builder.keyManager(manager);
            this.keyManager = manager;
        } else {
            this.keyManager = null;
        }
        if (config.tlsTrustCertsFilePath() != null) {
            var trustFile = Path.of(config.tlsTrustCertsFilePath());
            var manager = new ReloadingTrustManager();
            watchedFiles.add(
                    new WatchedFiles(
                            List.of(trustFile),
                            contents -> manager.update(parseCertificates(trustFile, contents.get(0)))));
            builder.trustManager(manager);
            this.trustManager = manager;
        } else {
            this.trustManager = null;
        }
        this.channelCredentials = builder.build();
        this.reloadTask =
                watchedFiles.isEmpty()
                        ? null
                        : executor.scheduleWithFixedDelay(this::reload, 1, 1, MINUTES);
    }

    /** Reload the files whose contents changed since they were last tried. */
    @VisibleForTesting
    void reload() {
        watchedFiles.forEach(WatchedFiles::reloadIfChanged);
    }

    @Override
    public void close() {
        if (reloadTask != null) {
            reloadTask.cancel(false);
        }
    }

    private static List<X509Certificate> parseCertificates(Path file, String pem)
            throws CertificateException {
        var certificates =
                CertificateFactory.getInstance("X.509")
                        .generateCertificates(new ByteArrayInputStream(pem.getBytes(US_ASCII)))
                        .stream()
                        .map(X509Certificate.class::cast)
                        .toList();
        if (certificates.isEmpty()) {
            throw new CertificateException(file + " has no certificates");
        }
        return certificates;
    }

    private static PrivateKey parsePrivateKey(Path file, String pem) throws GeneralSecurityException {
        var match = PKCS8_KEY.matcher(pem);
        if (!match.find()) {
            throw new InvalidKeySpecException(
                    file + " has no unencrypted PKCS#8 private key (BEGIN PRIVATE KEY)");
        }
        var spec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(match.group(1)));
        for (var algorithm : List.of("EC", "RSA")) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(spec);
            } catch (InvalidKeySpecException e) {
                // Try the next algorithm.
            }
        }
        throw new InvalidKeySpecException(file + " has neither an EC nor an RSA private key");
    }

    // A key that doesn't match the certificate would fail every handshake: sign with the key and
    // check the signature with the certificate's public key.
    private static void checkKeyMatches(PrivateKey key, X509Certificate certificate)
            throws GeneralSecurityException {
        var algorithm = key.getAlgorithm().equals("RSA") ? "SHA256withRSA" : "SHA256withECDSA";
        var data = new byte[32];
        var signer = Signature.getInstance(algorithm);
        signer.initSign(key);
        signer.update(data);
        var signature = signer.sign();
        var verifier = Signature.getInstance(algorithm);
        verifier.initVerify(certificate.getPublicKey());
        verifier.update(data);
        if (!verifier.verify(signature)) {
            throw new InvalidKeyException("The private key doesn't match the certificate");
        }
    }

    private interface Loader {
        void load(List<String> contents) throws GeneralSecurityException, IOException;
    }

    /** Files loaded together, from their contents read at the same time. */
    private static final class WatchedFiles {
        private final List<Path> files;
        private final Loader loader;
        private List<String> triedContents;

        WatchedFiles(List<Path> files, Loader loader) {
            this.files = files;
            this.loader = loader;
            try {
                this.triedContents = read();
                loader.load(triedContents);
            } catch (IOException | GeneralSecurityException | RuntimeException e) {
                throw new IllegalArgumentException("Failed to load the TLS files " + files, e);
            }
        }

        void reloadIfChanged() {
            try {
                var contents = read();
                if (contents.equals(triedContents)) {
                    return;
                }
                triedContents = contents;
                loader.load(contents);
                log.info().attr("files", files).log("Reloaded the TLS files");
            } catch (IOException | GeneralSecurityException | RuntimeException e) {
                log.warn()
                        .attr("files", files)
                        .exceptionMessage(e)
                        .log("Failed to reload the TLS files; the previous ones stay in use");
            }
        }

        private List<String> read() throws IOException {
            var contents = new ArrayList<String>(files.size());
            for (var file : files) {
                contents.add(Files.readString(file));
            }
            return contents;
        }
    }
}
