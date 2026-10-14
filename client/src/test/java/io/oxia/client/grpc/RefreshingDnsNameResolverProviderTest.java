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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RefreshingDnsNameResolverProviderTest {

    @Test
    void testIgnoresOtherSchemes() {
        var provider = new RefreshingDnsNameResolverProvider();
        assertNull(provider.newNameResolver(URI.create("dns:///localhost:6648"), null));
    }

    @Test
    void testLooksUpTheNameOnEveryRefresh() {
        var resolver =
                new RefreshingDnsNameResolverProvider()
                        .newNameResolver(URI.create("oxia-passthrough:///127.0.0.1:6648"), null);
        assertEquals("127.0.0.1:6648", resolver.getServiceAuthority());

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
        // The default gRPC resolver would ignore this refresh, within 30s of the first lookup
        resolver.refresh();

        var address = new InetSocketAddress("127.0.0.1", 6648);
        assertEquals(List.of(address, address), resolved);
    }
}
