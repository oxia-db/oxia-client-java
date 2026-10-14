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
import io.grpc.SynchronizationContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import lombok.NonNull;

/**
 * Resolves {@code oxia-dns:///host:port} targets on every refresh.
 *
 * <p>The default gRPC DNS resolver ignores a refresh within 30s of its last lookup, so a server
 * that restarts with a new IP stays unreachable for up to 30s when the lookup triggered by the
 * disconnect still returned the old IP. The JVM still caches lookups for {@code
 * networkaddress.cache.ttl}.
 */
final class RefreshingDnsNameResolverProvider extends NameResolverProvider {
    static final String SCHEME = "oxia-dns";

    interface HostResolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    private final String scheme;
    private final HostResolver hostResolver;

    RefreshingDnsNameResolverProvider() {
        this(SCHEME, InetAddress::getAllByName);
    }

    RefreshingDnsNameResolverProvider(@NonNull String scheme, @NonNull HostResolver hostResolver) {
        this.scheme = scheme;
        this.hostResolver = hostResolver;
    }

    static String target(String address) {
        return SCHEME + ":///" + address;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!scheme.equals(targetUri.getScheme())) {
            return null;
        }
        var authority = targetUri.getPath().substring(1);
        var hostAndPort = URI.create("//" + authority);
        if (hostAndPort.getHost() == null) {
            throw new IllegalArgumentException("Invalid target: " + targetUri);
        }
        var port = hostAndPort.getPort() != -1 ? hostAndPort.getPort() : args.getDefaultPort();
        return new Resolver(
                authority,
                hostAndPort.getHost(),
                port,
                hostResolver,
                args.getOffloadExecutor(),
                args.getSynchronizationContext());
    }

    @Override
    public String getDefaultScheme() {
        return scheme;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        // Only used for its own scheme
        return 5;
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
        return List.of(InetSocketAddress.class);
    }

    private static final class Resolver extends NameResolver {
        private final String authority;
        private final String host;
        private final int port;
        private final HostResolver hostResolver;
        private final Executor executor;
        private final SynchronizationContext syncContext;

        // Only accessed in the synchronization context
        private Listener2 listener;
        private boolean resolving;
        private boolean shutdown;

        Resolver(
                String authority,
                String host,
                int port,
                HostResolver hostResolver,
                Executor executor,
                SynchronizationContext syncContext) {
            this.authority = authority;
            this.host = host;
            this.port = port;
            this.hostResolver = hostResolver;
            this.executor = executor;
            this.syncContext = syncContext;
        }

        @Override
        public String getServiceAuthority() {
            return authority;
        }

        @Override
        public void start(Listener2 listener) {
            this.listener = listener;
            resolve();
        }

        @Override
        public void refresh() {
            resolve();
        }

        private void resolve() {
            if (resolving || shutdown) {
                return;
            }
            resolving = true;
            executor.execute(
                    () -> {
                        var result = lookup();
                        syncContext.execute(
                                () -> {
                                    resolving = false;
                                    if (!shutdown) {
                                        listener.onResult2(
                                                ResolutionResult.newBuilder().setAddressesOrError(result).build());
                                    }
                                });
                    });
        }

        private StatusOr<List<EquivalentAddressGroup>> lookup() {
            try {
                List<EquivalentAddressGroup> addresses = new ArrayList<>();
                for (var address : hostResolver.resolve(host)) {
                    addresses.add(new EquivalentAddressGroup(new InetSocketAddress(address, port)));
                }
                return StatusOr.fromValue(addresses);
            } catch (UnknownHostException | RuntimeException e) {
                return StatusOr.fromStatus(
                        Status.UNAVAILABLE.withDescription("Unable to resolve host " + host).withCause(e));
            }
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }
    }
}
