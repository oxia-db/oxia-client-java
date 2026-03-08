/*
 * Copyright © 2022-2026 The Oxia Authors
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
import io.oxia.proto.LeaderHint;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public final class OxiaWriteStreamManager {
    private final Map<Long, WriteStreamWrapper> writeStreams;
    private final OxiaStubProvider provider;

    public OxiaWriteStreamManager(OxiaStubProvider provider) {
        this.provider = provider;
        this.writeStreams = new ConcurrentHashMap<>();
    }

    private static final Metadata.Key<String> NAMESPACE_KEY =
            Metadata.Key.of("namespace", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> SHARD_ID_KEY =
            Metadata.Key.of("shard-id", Metadata.ASCII_STRING_MARSHALLER);

    public WriteStreamWrapper getWriteStream(long shardId, @Nullable LeaderHint leaderHint) {
        WriteStreamWrapper wrapper = null;
        for (int i = 0; i < 2; i++) {
            wrapper = writeStreams.get(shardId); // lock free first
            // When a leader hint is provided, invalidate the cached stream so we
            // connect to the hinted leader instead of reusing a stale connection.
            if (wrapper != null
                    && wrapper.isValid()
                    && (leaderHint == null || leaderHint.getLeaderAddress().isEmpty())) {
                return wrapper;
            }
            if (wrapper != null) {
                writeStreams.remove(shardId, wrapper);
                wrapper = null;
            }
            wrapper =
                    writeStreams.computeIfAbsent(
                            shardId,
                            (__) -> {
                                Metadata headers = new Metadata();
                                headers.put(NAMESPACE_KEY, provider.getNamespace());
                                headers.put(SHARD_ID_KEY, String.format("%d", shardId));
                                final var asyncStub = provider.getStubForShard(shardId, leaderHint).async();
                                return new WriteStreamWrapper(
                                        asyncStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers)));
                            });
            if (wrapper.isValid()) {
                break;
            }
            writeStreams.remove(shardId, wrapper);
        }
        return wrapper;
    }
}
