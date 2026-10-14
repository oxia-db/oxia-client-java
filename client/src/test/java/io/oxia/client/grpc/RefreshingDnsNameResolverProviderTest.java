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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.NameResolver;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class RefreshingDnsNameResolverProviderTest {

    @Test
    void testIgnoresOtherSchemes() {
        var provider = new RefreshingDnsNameResolverProvider();
        assertNull(provider.newNameResolver(URI.create("dns:///localhost:6648"), null));
    }

    @Test
    @SuppressWarnings("deprecation")
    void testReconnectsToNewAddressOfRestartedServer() throws Exception {
        var oldServer = startHealthServer(new InetSocketAddress("127.0.0.1", 0));
        var port = oldServer.getPort();
        Server newServer;
        try {
            newServer = startHealthServer(new InetSocketAddress("::1", port));
        } catch (IOException e) {
            oldServer.shutdownNow();
            Assumptions.abort("Cannot listen on [::1]:" + port + ": " + e);
            return;
        }

        // Stands in for DNS
        var address = new AtomicReference<>(InetAddress.getByName("127.0.0.1"));
        var provider =
                new RefreshingDnsNameResolverProvider(
                        "oxia-dns-test", host -> new InetAddress[] {address.get()});
        ManagedChannel channel =
                Grpc.newChannelBuilder(
                                "oxia-dns-test:///oxia.test:" + port, InsecureChannelCredentials.create())
                        .nameResolverFactory(provider)
                        .build();
        try {
            var health = HealthGrpc.newBlockingStub(channel);
            check(health);

            // Restart the server on a new IP, after the channel resolved the old one
            address.set(InetAddress.getByName("::1"));
            oldServer.shutdownNow().awaitTermination();

            // The default gRPC resolver would not look the name up again for 30s
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            boolean reconnected = false;
            while (!reconnected && System.nanoTime() < deadline) {
                try {
                    check(health);
                    reconnected = true;
                } catch (StatusRuntimeException e) {
                    Thread.sleep(50);
                }
            }
            assertTrue(reconnected);
        } finally {
            channel.shutdownNow();
            oldServer.shutdownNow();
            newServer.shutdownNow();
        }
    }

    @Test
    void testServiceAuthority() {
        var provider = new RefreshingDnsNameResolverProvider();
        var resolver =
                provider.newNameResolver(
                        URI.create(RefreshingDnsNameResolverProvider.target("oxia.test:6648")),
                        NameResolver.Args.newBuilder()
                                .setDefaultPort(443)
                                .setProxyDetector(target -> null)
                                .setSynchronizationContext(new io.grpc.SynchronizationContext((t, e) -> {}))
                                .setServiceConfigParser(
                                        new NameResolver.ServiceConfigParser() {
                                            @Override
                                            public NameResolver.ConfigOrError parseServiceConfig(
                                                    java.util.Map<String, ?> rawServiceConfig) {
                                                return null;
                                            }
                                        })
                                .build());
        assertEquals("oxia.test:6648", resolver.getServiceAuthority());
    }

    private static void check(HealthGrpc.HealthBlockingStub health) {
        health.withDeadlineAfter(1, TimeUnit.SECONDS).check(HealthCheckRequest.getDefaultInstance());
    }

    private static Server startHealthServer(InetSocketAddress address) throws IOException {
        return NettyServerBuilder.forAddress(address)
                .addService(new HealthStatusManager().getHealthService())
                .build()
                .start();
    }
}
