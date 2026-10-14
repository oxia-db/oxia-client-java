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

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.Status;
import io.grpc.StatusOr;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves {@code oxia-passthrough:///host:port} targets on every refresh. The default gRPC DNS
 * resolver ignores a refresh within 30s of its last lookup, so a server that restarts with a new IP
 * stays unreachable for up to 30s.
 */
final class RefreshingDnsNameResolverProvider extends NameResolverProvider {
    static final String SCHEME = "oxia-passthrough";

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }
        var authority = targetUri.getPath().substring(1);
        var hostAndPort = URI.create("//" + authority);
        return new NameResolver() {
            private Listener2 listener;

            @Override
            public String getServiceAuthority() {
                return authority;
            }

            @Override
            public void start(Listener2 listener) {
                this.listener = listener;
                refresh();
            }

            @Override
            public void refresh() {
                listener.onResult2(
                        ResolutionResult.newBuilder()
                                .setAddressesOrError(lookup(hostAndPort.getHost(), hostAndPort.getPort()))
                                .build());
            }

            @Override
            public void shutdown() {}
        };
    }

    private static StatusOr<List<EquivalentAddressGroup>> lookup(String host, int port) {
        try {
            List<EquivalentAddressGroup> addresses = new ArrayList<>();
            for (var address : InetAddress.getAllByName(host)) {
                addresses.add(new EquivalentAddressGroup(new InetSocketAddress(address, port)));
            }
            return StatusOr.fromValue(addresses);
        } catch (UnknownHostException e) {
            return StatusOr.fromStatus(
                    Status.UNAVAILABLE.withDescription("Unable to resolve host " + host).withCause(e));
        }
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }
}
