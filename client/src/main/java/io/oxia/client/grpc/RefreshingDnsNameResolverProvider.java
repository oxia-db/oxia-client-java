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
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

/**
 * Resolves {@code oxia-passthrough:///host:port} targets with Netty's DNS resolver on every
 * refresh.
 *
 * <p>The default gRPC DNS resolver ignores a refresh within 30s of its last lookup, and the JVM
 * caches lookups for another 30s, so a server that restarts with a new IP stays unreachable for up
 * to 30s. Netty's resolver caches a record only for its DNS TTL.
 */
final class RefreshingDnsNameResolverProvider extends NameResolverProvider {
    static final String SCHEME = "oxia-passthrough";

    private final DnsNameResolver dns;

    RefreshingDnsNameResolverProvider() {
        this(
                new DnsNameResolverBuilder(
                                new MultiThreadIoEventLoopGroup(
                                                1,
                                                new DefaultThreadFactory("oxia-dns-resolver", true),
                                                NioIoHandler.newFactory())
                                        .next())
                        .datagramChannelType(NioDatagramChannel.class)
                        .socketChannelType(NioSocketChannel.class)
                        .build());
    }

    RefreshingDnsNameResolverProvider(DnsNameResolver dns) {
        this.dns = dns;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }
        var authority = targetUri.getPath().substring(1);
        var hostAndPort = URI.create("//" + authority);
        var syncContext = args.getSynchronizationContext();
        return new NameResolver() {
            // Only accessed in the synchronization context
            private Listener2 listener;
            private boolean resolving;
            private boolean shutdown;

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
                if (resolving || shutdown) {
                    return;
                }
                resolving = true;
                dns.resolveAll(hostAndPort.getHost())
                        .addListener(
                                (Future<List<InetAddress>> lookup) ->
                                        syncContext.execute(
                                                () -> {
                                                    resolving = false;
                                                    if (!shutdown) {
                                                        listener.onResult2(
                                                                ResolutionResult.newBuilder()
                                                                        .setAddressesOrError(toAddresses(lookup, hostAndPort))
                                                                        .build());
                                                    }
                                                }));
            }

            @Override
            public void shutdown() {
                shutdown = true;
            }
        };
    }

    private static StatusOr<List<EquivalentAddressGroup>> toAddresses(
            Future<List<InetAddress>> lookup, URI hostAndPort) {
        if (!lookup.isSuccess()) {
            return StatusOr.fromStatus(
                    Status.UNAVAILABLE
                            .withDescription("Unable to resolve host " + hostAndPort.getHost())
                            .withCause(lookup.cause()));
        }
        return StatusOr.fromValue(
                lookup.getNow().stream()
                        .map(
                                address ->
                                        new EquivalentAddressGroup(
                                                new InetSocketAddress(address, hostAndPort.getPort())))
                        .toList());
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
