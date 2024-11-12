/*
 * Copyright © 2022-2024 StreamNative Inc.
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
package io.streamnative.oxia.client.grpc;

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class OxiaWriteStreamManager {
    private final Map<Long, WriteStreamWrapper> writeStreams = new ConcurrentHashMap<>();
    private final OxiaStubProvider provider;

    public OxiaWriteStreamManager(OxiaStubProvider provider) {
        this.provider = provider;
    }

    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    public WriteStreamWrapper getWriteStream(long shardId) {
        return writeStreams.compute(
                shardId,
                (key, stream) -> {
                    if (stream == null || !stream.isValid()) {
                        Metadata headers = new Metadata();
                        headers.put(NAMESPACE_KEY, provider.getNamespace());
                        headers.put(SHARD_ID_KEY, String.format("%d", shardId));
                        final var asyncStub = provider.getStubForShard(shardId).async();
                        return new WriteStreamWrapper(
                                asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)));
                    }
                    return stream;
                });
    }
}
