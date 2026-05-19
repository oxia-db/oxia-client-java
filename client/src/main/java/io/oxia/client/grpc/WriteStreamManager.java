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

import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.oxia.proto.OxiaClientGrpc;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;

final class WriteStreamManager {
    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    private final Map<Long, WriteStreamWrapper> writeStreams;
    private final String namespace;
    private final LongFunction<OxiaClientGrpc.OxiaClientStub> stubProvider;

    WriteStreamManager(String namespace, LongFunction<OxiaClientGrpc.OxiaClientStub> stubProvider) {
        this.namespace = namespace;
        this.stubProvider = stubProvider;
        this.writeStreams = new ConcurrentHashMap<>();
    }

    WriteStreamWrapper getWriteStream(long shardId) {
        WriteStreamWrapper wrapper = null;
        for (int i = 0; i < 2; i++) {
            wrapper = writeStreams.get(shardId); // lock free first
            if (wrapper == null) {
                wrapper =
                        writeStreams.computeIfAbsent(
                                shardId,
                                (__) -> {
                                    Metadata headers = new Metadata();
                                    headers.put(NAMESPACE_KEY, namespace);
                                    headers.put(SHARD_ID_KEY, Long.toString(shardId));
                                    return new WriteStreamWrapper(
                                            stubProvider
                                                    .apply(shardId)
                                                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)),
                                            shardId);
                                });
            }
            if (wrapper.isValid()) {
                break;
            }
            writeStreams.remove(shardId, wrapper);
        }
        return wrapper;
    }
}
