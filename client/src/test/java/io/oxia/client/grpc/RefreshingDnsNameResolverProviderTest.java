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
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.SingletonDnsServerAddressStreamProvider;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RefreshingDnsNameResolverProviderTest {
    private final AtomicReference<InetAddress> address = new AtomicReference<>();
    private EventLoopGroup eventLoopGroup;
    private Channel dnsServer;
    private RefreshingDnsNameResolverProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        eventLoopGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        dnsServer = startDnsServer();
        provider =
                new RefreshingDnsNameResolverProvider(
                        new DnsNameResolverBuilder(eventLoopGroup.next())
                                .datagramChannelType(NioDatagramChannel.class)
                                .nameServerProvider(
                                        new SingletonDnsServerAddressStreamProvider(
                                                (InetSocketAddress) dnsServer.localAddress()))
                                .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY)
                                .searchDomains(List.of())
                                .build());
    }

    @AfterEach
    void tearDown() {
        dnsServer.close();
        eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }

    @Test
    void testIgnoresOtherSchemes() {
        assertNull(provider.newNameResolver(URI.create("dns:///localhost:6648"), args()));
    }

    @Test
    void testRefreshReturnsTheNewAddressOfARestartedServer() throws Exception {
        address.set(InetAddress.getByName("10.0.0.1"));
        var resolver =
                provider.newNameResolver(URI.create("oxia-passthrough:///oxia.test:6648"), args());
        assertEquals("oxia.test:6648", resolver.getServiceAuthority());

        BlockingQueue<List<SocketAddress>> results = new LinkedBlockingQueue<>();
        resolver.start(
                new NameResolver.Listener2() {
                    @Override
                    public void onResult(NameResolver.ResolutionResult result) {
                        results.add(
                                result.getAddressesOrError().getValue().stream()
                                        .flatMap(group -> group.getAddresses().stream())
                                        .toList());
                    }

                    @Override
                    public void onError(Status error) {}
                });
        assertEquals(
                List.of(new InetSocketAddress("10.0.0.1", 6648)), results.poll(5, TimeUnit.SECONDS));

        // The record expires after its TTL of 1s, while neither the gRPC resolver nor the JVM
        // would look the name up again within 30s
        address.set(InetAddress.getByName("10.0.0.2"));
        Thread.sleep(1500);
        resolver.refresh();
        assertEquals(
                List.of(new InetSocketAddress("10.0.0.2", 6648)), results.poll(5, TimeUnit.SECONDS));
        resolver.shutdown();
    }

    /** Answers the A queries of any name with {@link #address}, with a TTL of 1s. */
    private Channel startDnsServer() throws InterruptedException {
        return new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioDatagramChannel.class)
                .handler(
                        new ChannelInitializer<DatagramChannel>() {
                            @Override
                            protected void initChannel(DatagramChannel channel) {
                                channel
                                        .pipeline()
                                        .addLast(new DatagramDnsQueryDecoder())
                                        .addLast(new DatagramDnsResponseEncoder())
                                        .addLast(
                                                new SimpleChannelInboundHandler<DatagramDnsQuery>() {
                                                    @Override
                                                    protected void channelRead0(
                                                            ChannelHandlerContext ctx, DatagramDnsQuery query) {
                                                        DnsQuestion question = query.recordAt(DnsSection.QUESTION);
                                                        var response =
                                                                new DatagramDnsResponse(
                                                                        query.recipient(), query.sender(), query.id());
                                                        response.addRecord(DnsSection.QUESTION, question);
                                                        if (question.type() == DnsRecordType.A) {
                                                            response.addRecord(
                                                                    DnsSection.ANSWER,
                                                                    new DefaultDnsRawRecord(
                                                                            question.name(),
                                                                            DnsRecordType.A,
                                                                            1,
                                                                            Unpooled.wrappedBuffer(address.get().getAddress())));
                                                        }
                                                        ctx.writeAndFlush(response);
                                                    }
                                                });
                            }
                        })
                .bind(new InetSocketAddress("127.0.0.1", 0))
                .sync()
                .channel();
    }

    private static NameResolver.Args args() {
        return NameResolver.Args.newBuilder()
                .setDefaultPort(6648)
                .setProxyDetector(target -> null)
                .setSynchronizationContext(new SynchronizationContext((t, e) -> {}))
                .setServiceConfigParser(
                        new NameResolver.ServiceConfigParser() {
                            @Override
                            public NameResolver.ConfigOrError parseServiceConfig(Map<String, ?> rawConfig) {
                                return null;
                            }
                        })
                .build();
    }
}
