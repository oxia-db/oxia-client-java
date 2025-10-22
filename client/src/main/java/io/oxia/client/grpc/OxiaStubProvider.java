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

import io.oxia.client.shard.ShardManager;
import lombok.Getter;

public class OxiaStubProvider {
    @Getter private final String namespace;
    private final OxiaStubManager stubManager;
    private final ShardManager shardManager;
    private final OxiaWriteStreamManager writeStreamManager;

    public OxiaStubProvider(
            String namespace, OxiaStubManager stubManager, ShardManager shardManager) {
        this.namespace = namespace;
        this.stubManager = stubManager;
        this.shardManager = shardManager;
        this.writeStreamManager = new OxiaWriteStreamManager(this);
    }

    public OxiaStub getStubForShard(long shardId) {
        String leader = shardManager.leader(shardId);
        return stubManager.getStub(leader);
    }

    public WriteStreamWrapper getWriteStreamForShard(long shardId) {
        return writeStreamManager.getWriteStream(shardId);
    }
}
