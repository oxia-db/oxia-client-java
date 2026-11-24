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

import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.grpc.util.AdvancedTlsX509KeyManager;
import io.grpc.util.AdvancedTlsX509TrustManager;
import io.grpc.util.AdvancedTlsX509TrustManager.Verification;
import io.oxia.client.ClientConfig;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;

/**
 * The credentials of the client's TLS connections, from the PEM files in the client's config.
 *
 * <p>Each file is read once here, then checked for changes on the executor and reloaded, so that
 * rotated certificates apply to new connections without a restart. A file that fails to reload
 * leaves the previous certificates in use. Without files, the JVM's default trust store applies.
 */
final class TlsCredentials implements AutoCloseable {
    // grpc-util checks the files no more often than once a minute.
    private static final long RELOAD_INTERVAL_MINUTES = 1;

    @Getter private final ChannelCredentials channelCredentials;
    private final List<Runnable> stopReloads = new ArrayList<>();

    TlsCredentials(ClientConfig config, ScheduledExecutorService executor) {
        var builder = TlsChannelCredentials.newBuilder();
        try {
            if (config.tlsCertificateFilePath() != null) {
                var keyManager = new AdvancedTlsX509KeyManager();
                stopReloads.add(
                        keyManager.updateIdentityCredentials(
                                        new File(config.tlsCertificateFilePath()),
                                        new File(config.tlsKeyFilePath()),
                                        RELOAD_INTERVAL_MINUTES,
                                        MINUTES,
                                        executor)
                                ::close);
                builder.keyManager(keyManager);
            }
            if (config.tlsTrustCertsFilePath() != null) {
                var trustManager =
                        AdvancedTlsX509TrustManager.newBuilder()
                                .setVerification(Verification.CERTIFICATE_AND_HOST_NAME_VERIFICATION)
                                .build();
                stopReloads.add(
                        trustManager.updateTrustCredentials(
                                        new File(config.tlsTrustCertsFilePath()),
                                        RELOAD_INTERVAL_MINUTES,
                                        MINUTES,
                                        executor)
                                ::close);
                builder.trustManager(trustManager);
            }
        } catch (IOException | GeneralSecurityException e) {
            close();
            throw new IllegalArgumentException(
                    "Failed to load the TLS files: tlsTrustCertsFilePath="
                            + config.tlsTrustCertsFilePath()
                            + ", tlsCertificateFilePath="
                            + config.tlsCertificateFilePath()
                            + ", tlsKeyFilePath="
                            + config.tlsKeyFilePath(),
                    e);
        }
        this.channelCredentials = builder.build();
    }

    @Override
    public void close() {
        stopReloads.forEach(Runnable::run);
    }
}
