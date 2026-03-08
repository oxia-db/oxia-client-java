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

import io.oxia.client.shard.ShardManager;
import io.oxia.proto.LeaderHint;
import javax.annotation.Nullable;
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
        return getStubForShard(shardId, null);
    }

    public OxiaStub getStubForShard(long shardId, @Nullable LeaderHint leaderHint) {
        String target;
        if (leaderHint != null && !leaderHint.getLeaderAddress().isEmpty()) {
            target = leaderHint.getLeaderAddress();
        } else {
            target = shardManager.leader(shardId);
        }
        return stubManager.getStub(target);
    }

    public WriteStreamWrapper getWriteStreamForShard(long shardId) {
        return writeStreamManager.getWriteStream(shardId, null);
    }

    public WriteStreamWrapper getWriteStreamForShard(long shardId, @Nullable LeaderHint leaderHint) {
        return writeStreamManager.getWriteStream(shardId, leaderHint);
    }
}
