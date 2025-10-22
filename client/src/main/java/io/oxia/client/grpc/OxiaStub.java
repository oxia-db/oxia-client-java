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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.base.Throwables;
import io.grpc.CallCredentials;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.TlsChannelCredentials;
import io.grpc.internal.BackoffPolicy;
import io.oxia.client.ClientConfig;
import io.oxia.client.api.Authentication;
import io.oxia.proto.OxiaClientGrpc;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OxiaStub implements AutoCloseable {
    public static String TLS_SCHEMA = "tls://";
    private final ManagedChannel channel;
    private final @NonNull OxiaClientGrpc.OxiaClientStub asyncStub;

    static String getAddress(String address) {
        if (address.startsWith(TLS_SCHEMA)) {
            return address.substring(TLS_SCHEMA.length());
        }
        return address;
    }

    static ChannelCredentials getChannelCredential(String address, boolean tlsEnabled) {
        return tlsEnabled || address.startsWith(TLS_SCHEMA)
                ? TlsChannelCredentials.newBuilder().build()
                : InsecureChannelCredentials.create();
    }

    public OxiaStub(
            String address,
            ClientConfig clientConfig,
            @Nullable BackoffPolicy.Provider backoffProvider) {

        this(Grpc.newChannelBuilder(getAddress(address), getChannelCredential(address, clientConfig.enableTls()))
                        .keepAliveTime(clientConfig.connectionKeepAliveTime().toMillis(), MILLISECONDS)
                        .keepAliveTimeout(clientConfig.connectionKeepAliveTimeout().toMillis(), MILLISECONDS)
                        .keepAliveWithoutCalls(true)
                        .disableRetry()
                        .directExecutor()
                        .build(), clientConfig.authentication(), backoffProvider);
    }

    public OxiaStub(ManagedChannel channel) {
        this(channel, null, OxiaBackoffProvider.DEFAULT);
    }

    public OxiaStub(ManagedChannel channel, @Nullable final Authentication authentication,
                    @Nullable BackoffPolicy.Provider oxiaBackoffPolicyProvider) {
        /*
            The GRPC default backoff is from 2s to 120s, which is very long for time sensitive usage.

            Using reflection to replace the existing backoff here due to that is not configurable.
            FYI: https://github.com/grpc/grpc-java/issues/10932#issuecomment-1954913671
         */
        if (oxiaBackoffPolicyProvider != null) {
            configureBackoffPolicyIfPossible(channel, oxiaBackoffPolicyProvider);
        }
        this.channel = channel;
        if (authentication != null) {
            this.asyncStub =
                    OxiaClientGrpc.newStub(channel)
                            .withCallCredentials(
                                    new CallCredentials() {

                                        @Override
                                        public void applyRequestMetadata(
                                                RequestInfo requestInfo, Executor appExecutor,
                                                MetadataApplier applier) {
                                            Metadata credentials = new Metadata();
                                            authentication.generateCredentials()
                                                    .forEach((key, value) ->
                                                            credentials.put(
                                                                    Metadata.Key.of(key,
                                                                            Metadata.ASCII_STRING_MARSHALLER),
                                                                    value));
                                            applier.apply(credentials);
                                        }

                                        @Override
                                        public void thisUsesUnstableApi() {
                                            // Nothing to do.
                                        }
                                    });
        } else {
            this.asyncStub = OxiaClientGrpc.newStub(channel);
        }
    }

    private void configureBackoffPolicyIfPossible(ManagedChannel channel, BackoffPolicy.Provider oxiaBackoffPolicyProvider) {
        try {
            final Class<?> mcl = Class.forName("io.grpc.internal.ForwardingManagedChannel");
            final Field delegate = mcl.getDeclaredField("delegate");
            delegate.setAccessible(true);
            final Object mclInstance = delegate.get(channel);
            final Class<?> mclInstanceKlass = Class.forName("io.grpc.internal.ManagedChannelImpl");
            final Field backOffField = mclInstanceKlass.getDeclaredField("backoffPolicyProvider");
            backOffField.setAccessible(true);
            backOffField.set(mclInstance, oxiaBackoffPolicyProvider);
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException ex) {
            log.warn("Auto replace GRPC default backoff policy failed. fallback to the GRPC default implementation.",
                    Throwables.getRootCause(ex));
        }
    }

    public OxiaClientGrpc.OxiaClientStub async() {
        return asyncStub;
    }

    @Override
    public void close() throws Exception {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(100, MILLISECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
        }
    }
}
