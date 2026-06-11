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
package io.oxia.client.shard;

import com.google.common.base.Strings;
import io.oxia.client.grpc.OxiaStatusException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;

public class ShardAssignmentsContainer {
    private final ConcurrentMap<Long, Shard> shards = new ConcurrentHashMap<>();
    private final ShardStrategy shardStrategy;

    // Immutable lookup structure, rebuilt on each assignments update
    private volatile ShardRouter router;

    @Getter private final String namespace;

    ShardAssignmentsContainer(ShardStrategy shardStrategy, String namespace) {
        if (Strings.isNullOrEmpty(namespace)) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        this.shardStrategy = shardStrategy;
        this.router = shardStrategy.createRouter(List.of());
        this.namespace = namespace;
    }

    public long getShardForKey(String key) {
        Shard shard = router.getShardForKey(key);
        if (shard == null) {
            throw OxiaStatusException.shardNotFound(key);
        }
        return shard.id();
    }

    public String leader(long shardId) {
        Shard shard = shards.get(shardId);
        if (shard == null) {
            throw OxiaStatusException.shardNotFound(shardId);
        }
        if (shard.leader().isBlank()) {
            throw OxiaStatusException.leaderNotAvailable(shardId);
        }

        return shard.leader();
    }

    synchronized void update(ShardManager.ShardAssignmentChanges changes) {
        changes.added().forEach(s -> shards.put(s.id(), s));
        changes.reassigned().forEach(s -> shards.put(s.id(), s));
        changes.removed().forEach(s -> shards.remove(s.id(), s));
        router = shardStrategy.createRouter(shards.values());
    }

    Set<Long> allShardIds() {
        return shards.keySet();
    }

    Map<Long, Shard> allShards() {
        return shards;
    }
}
