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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RefreshingDnsNameResolverProviderTest {

    @Test
    void testIgnoresOtherSchemes() {
        var provider = new RefreshingDnsNameResolverProvider();
        assertNull(provider.newNameResolver(URI.create("dns:///localhost:6648"), args()));
    }

    @Test
    void testRefreshLooksUpTheNameAgain() throws Exception {
        // Stands in for DNS
        var address = new AtomicReference<>(InetAddress.getByName("10.0.0.1"));
        var provider = new RefreshingDnsNameResolverProvider(host -> new InetAddress[] {address.get()});
        var resolver =
                provider.newNameResolver(
                        URI.create(RefreshingDnsNameResolverProvider.target("oxia.test:6648")), args());
        assertEquals("oxia.test:6648", resolver.getServiceAuthority());

        List<SocketAddress> resolved = new ArrayList<>();
        resolver.start(
                new NameResolver.Listener2() {
                    @Override
                    public void onResult(NameResolver.ResolutionResult result) {
                        result
                                .getAddressesOrError()
                                .getValue()
                                .forEach(group -> resolved.addAll(group.getAddresses()));
                    }

                    @Override
                    public void onError(Status error) {}
                });
        assertEquals(List.of(new InetSocketAddress("10.0.0.1", 6648)), resolved);

        // A server restarted with a new IP: the default gRPC resolver ignores a
        // refresh within 30s of its last lookup
        address.set(InetAddress.getByName("10.0.0.2"));
        resolved.clear();
        resolver.refresh();
        assertEquals(List.of(new InetSocketAddress("10.0.0.2", 6648)), resolved);
    }

    private static NameResolver.Args args() {
        return NameResolver.Args.newBuilder()
                .setDefaultPort(6648)
                .setProxyDetector(target -> null)
                .setSynchronizationContext(new SynchronizationContext((t, e) -> {}))
                .setServiceConfigParser(
                        new NameResolver.ServiceConfigParser() {
                            @Override
                            public NameResolver.ConfigOrError parseServiceConfig(
                                    java.util.Map<String, ?> rawServiceConfig) {
                                return null;
                            }
                        })
                .build();
    }
}
