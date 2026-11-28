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

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * The client's certificate and key for mutual TLS, replaced as a pair.
 *
 * <p>The JDK looks up a handshake's key and certificate chain in two calls, by the alias it chose
 * first. Each pair has its own alias, and the previous pair stays available, so a handshake that
 * started before a replacement gets both from the same pair.
 */
final class ReloadingKeyManager extends X509ExtendedKeyManager {
    private record Identity(String alias, PrivateKey key, X509Certificate[] chain) {}

    private record Identities(Identity current, @Nullable Identity previous) {}

    private long generation;
    private volatile Identities identities;

    /** Make {@code key} and {@code chain} the pair new handshakes use. Called from one thread. */
    void update(PrivateKey key, List<X509Certificate> chain) {
        var current =
                new Identity("oxia-client-" + ++generation, key, chain.toArray(X509Certificate[]::new));
        var before = identities;
        identities = new Identities(current, before == null ? null : before.current());
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        return identities.current().alias();
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        return identities.current().alias();
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        return new String[] {identities.current().alias()};
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        var identity = find(alias);
        return identity == null ? null : identity.key();
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        var identity = find(alias);
        return identity == null ? null : identity.chain().clone();
    }

    // A client presents certificates; it never serves them.
    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        return null;
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        return null;
    }

    @Nullable
    private Identity find(String alias) {
        var current = identities;
        if (current.current().alias().equals(alias)) {
            return current.current();
        }
        var previous = current.previous();
        return previous != null && previous.alias().equals(alias) ? previous : null;
    }
}
