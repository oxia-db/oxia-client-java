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

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Grpc;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.TlsServerCredentials.ClientAuth;
import io.grpc.stub.StreamObserver;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The client's TLS connections with certificates from files. The server has no Oxia service, so an
 * {@code UNIMPLEMENTED} reply shows that the TLS handshake succeeded, and {@code UNAVAILABLE} that
 * it didn't.
 */
class TlsConnectionTest {
    @TempDir Path dir;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private TestCertificates.Issued ca;
    private Path caFile;
    private Path otherCaFile;
    private Server server;

    @BeforeEach
    void setup() throws Exception {
        ca = TestCertificates.ca("Test CA");
        caFile = ca.writeCertificate(dir, "ca");
        otherCaFile = TestCertificates.ca("Other CA").writeCertificate(dir, "other-ca");
    }

    @AfterEach
    void cleanup() {
        if (server != null) {
            server.shutdownNow();
        }
        executor.shutdownNow();
    }

    @Test
    void connectsToServerWithCertificateFromTrustedCa() throws Exception {
        startServer(ClientAuth.NONE);

        assertThat(call("localhost", b -> b.tlsTrustCertsFilePath(caFile.toString())))
                .isEqualTo(Status.Code.UNIMPLEMENTED);
    }

    @Test
    void refusesServerCertificateFromAnotherCa() throws Exception {
        startServer(ClientAuth.NONE);

        assertThat(call("localhost", b -> b.tlsTrustCertsFilePath(otherCaFile.toString())))
                .isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void refusesServerCertificateForAnotherHost() throws Exception {
        startServer(ClientAuth.NONE);

        // The server's certificate names localhost only.
        assertThat(call("127.0.0.1", b -> b.tlsTrustCertsFilePath(caFile.toString())))
                .isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    void presentsClientCertificateForMutualTls() throws Exception {
        startServer(ClientAuth.REQUIRE);
        var client = TestCertificates.issue(ca, "client");
        var certificate = client.writeCertificate(dir, "client");
        var key = client.writeKey(dir, "client");

        assertThat(call("localhost", b -> b.tlsTrustCertsFilePath(caFile.toString())))
                .isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(
                        call(
                                "localhost",
                                b ->
                                        b.tlsTrustCertsFilePath(caFile.toString())
                                                .tlsCertificateFilePath(certificate.toString())
                                                .tlsKeyFilePath(key.toString())))
                .isEqualTo(Status.Code.UNIMPLEMENTED);
    }

    @Test
    void picksUpRotatedTrustCertificates() throws Exception {
        startServer(ClientAuth.NONE);
        var trustFile = Files.copy(otherCaFile, dir.resolve("trust.crt"));
        var address = "localhost:" + server.getPort();

        try (var connections =
                connectionManager(address, b -> b.tlsTrustCertsFilePath(trustFile.toString()))) {
            assertThat(call(connections, address)).isEqualTo(Status.Code.UNAVAILABLE);

            replace(caFile, trustFile);
            assertThat(reloadAndCall(connections, address)).isEqualTo(Status.Code.UNIMPLEMENTED);
        }
    }

    @Test
    void keepsConnectingAfterAnEmptyTrustUpdate() throws Exception {
        startServer(ClientAuth.NONE);
        var trustFile = Files.copy(caFile, dir.resolve("trust.crt"));
        var address = "localhost:" + server.getPort();

        try (var connections =
                connectionManager(address, b -> b.tlsTrustCertsFilePath(trustFile.toString()))) {
            Files.writeString(trustFile, "");
            assertThat(reloadAndCall(connections, address)).isEqualTo(Status.Code.UNIMPLEMENTED);
        }
    }

    @Test
    void recoversFromAMalformedClientKey() throws Exception {
        startServer(ClientAuth.REQUIRE);
        var untrusted = TestCertificates.issue(TestCertificates.ca("Other CA"), "client");
        var certificate = untrusted.writeCertificate(dir, "client");
        var key = untrusted.writeKey(dir, "client");
        var address = "localhost:" + server.getPort();

        try (var connections =
                connectionManager(
                        address,
                        b ->
                                b.tlsTrustCertsFilePath(caFile.toString())
                                        .tlsCertificateFilePath(certificate.toString())
                                        .tlsKeyFilePath(key.toString()))) {
            assertThat(call(connections, address)).isEqualTo(Status.Code.UNAVAILABLE);

            Files.writeString(key, "-----BEGIN PRIVATE KEY-----\nAAAAA\n-----END PRIVATE KEY-----\n");
            assertThat(reloadAndCall(connections, address)).isEqualTo(Status.Code.UNAVAILABLE);

            var trusted = TestCertificates.issue(ca, "client");
            trusted.writeCertificate(dir, "client");
            trusted.writeKey(dir, "client");
            assertThat(reloadAndCall(connections, address)).isEqualTo(Status.Code.UNIMPLEMENTED);
        }
    }

    private void startServer(ClientAuth clientAuth) throws Exception {
        var serverCertificate = TestCertificates.issue(ca, "localhost");
        var credentials =
                TlsServerCredentials.newBuilder()
                        .keyManager(
                                serverCertificate.writeCertificate(dir, "server").toFile(),
                                serverCertificate.writeKey(dir, "server").toFile())
                        .trustManager(caFile.toFile())
                        .clientAuth(clientAuth)
                        .build();
        server = Grpc.newServerBuilderForPort(0, credentials).directExecutor().build().start();
    }

    private static void replace(Path source, Path target) throws Exception {
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    // Reload now rather than at the next check, then call on a new connection.
    private static Status.Code reloadAndCall(ConnectionManager connections, String address)
            throws Exception {
        connections.tlsCredentials.reload();
        connections.connections.values().forEach(Connection::close);
        connections.connections.clear();
        return call(connections, address);
    }

    private Status.Code call(String host, UnaryOperator<OxiaClientBuilder> tls) throws Exception {
        var address = host + ":" + server.getPort();
        try (var connections = connectionManager(address, tls)) {
            return call(connections, address);
        }
    }

    private ConnectionManager connectionManager(
            String address, UnaryOperator<OxiaClientBuilder> tls) {
        var builder = tls.apply(OxiaClientBuilder.create(address).enableTls(true));
        return new ConnectionManager(((OxiaClientBuilderImpl) builder).getClientConfig(), executor);
    }

    private static Status.Code call(ConnectionManager connections, String address) throws Exception {
        var result = new CompletableFuture<Status.Code>();
        connections
                .getConnection(address)
                .stub()
                .getShardAssignments(
                        new ShardAssignmentsRequest(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(ShardAssignments value) {}

                            @Override
                            public void onError(Throwable t) {
                                result.complete(Status.fromThrowable(t).getCode());
                            }

                            @Override
                            public void onCompleted() {
                                result.complete(Status.Code.OK);
                            }
                        });
        return result.get(30, TimeUnit.SECONDS);
    }
}
