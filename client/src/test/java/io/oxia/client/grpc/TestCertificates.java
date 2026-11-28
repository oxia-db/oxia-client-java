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

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** Short-lived ECDSA certificates for TLS tests, written as PEM files. */
public final class TestCertificates {
    private static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());
    private static final Duration HOUR = Duration.ofHours(1);
    private static final Duration DAY = Duration.ofDays(1);

    /** A certificate and the key pair of its subject. */
    public record Issued(X509Certificate certificate, KeyPair keyPair) {
        /** Write the certificate to {@code name}.crt in {@code dir}. */
        public Path writeCertificate(Path dir, String name)
                throws IOException, GeneralSecurityException {
            return write(dir.resolve(name + ".crt"), "CERTIFICATE", certificate.getEncoded());
        }

        /** Write the private key, as unencrypted PKCS#8, to {@code name}.key in {@code dir}. */
        public Path writeKey(Path dir, String name) throws IOException {
            return write(dir.resolve(name + ".key"), "PRIVATE KEY", keyPair.getPrivate().getEncoded());
        }
    }

    private TestCertificates() {}

    /** A self-signed CA. */
    public static Issued ca(String name) throws Exception {
        var keyPair = keyPair("EC");
        var subject = new X500Name("CN=" + name);
        var now = Instant.now();
        var builder = builder(subject, subject, keyPair, now.minus(HOUR), now.plus(DAY));
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        return new Issued(sign(builder, keyPair), keyPair);
    }

    /** A certificate from {@code ca} for {@code dnsName}, usable by a server or a client. */
    public static Issued issue(Issued ca, String dnsName) throws Exception {
        return issue(ca, dnsName, keyPair("EC"));
    }

    /** A certificate from {@code ca} for {@code dnsName} and the public key of {@code keyPair}. */
    public static Issued issue(Issued ca, String dnsName, KeyPair keyPair) throws Exception {
        var now = Instant.now();
        return issue(ca, dnsName, keyPair, now.minus(HOUR), now.plus(DAY));
    }

    /** A certificate from {@code ca} for {@code dnsName} that expired an hour ago. */
    public static Issued issueExpired(Issued ca, String dnsName, KeyPair keyPair) throws Exception {
        var now = Instant.now();
        return issue(ca, dnsName, keyPair, now.minus(DAY), now.minus(HOUR));
    }

    private static Issued issue(
            Issued ca, String dnsName, KeyPair keyPair, Instant notBefore, Instant notAfter)
            throws Exception {
        var issuer = new X500Name(ca.certificate().getSubjectX500Principal().getName());
        var builder = builder(issuer, new X500Name("CN=" + dnsName), keyPair, notBefore, notAfter);
        builder.addExtension(
                Extension.subjectAlternativeName,
                false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, dnsName)));
        builder.addExtension(
                Extension.extendedKeyUsage,
                false,
                new ExtendedKeyUsage(
                        new KeyPurposeId[] {KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}));
        return new Issued(sign(builder, ca.keyPair()), keyPair);
    }

    /** A new EC (P-256) or RSA (2048-bit) key pair. */
    public static KeyPair keyPair(String algorithm) throws GeneralSecurityException {
        var generator = KeyPairGenerator.getInstance(algorithm);
        generator.initialize(algorithm.equals("RSA") ? 2048 : 256);
        return generator.generateKeyPair();
    }

    private static JcaX509v3CertificateBuilder builder(
            X500Name issuer, X500Name subject, KeyPair keyPair, Instant notBefore, Instant notAfter) {
        return new JcaX509v3CertificateBuilder(
                issuer,
                BigInteger.valueOf(SERIAL.incrementAndGet()),
                Date.from(notBefore),
                Date.from(notAfter),
                subject,
                keyPair.getPublic());
    }

    private static X509Certificate sign(JcaX509v3CertificateBuilder builder, KeyPair signer)
            throws OperatorCreationException, GeneralSecurityException {
        var contentSigner = new JcaContentSignerBuilder("SHA256withECDSA").build(signer.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner));
    }

    private static Path write(Path file, String type, byte[] der) throws IOException {
        var base64 =
                Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der);
        var pem = "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
        return Files.writeString(file, pem);
    }
}
