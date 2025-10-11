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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;

public class ShardAssignmentsContainer {
    private final ConcurrentMap<Long, Shard> shards = new ConcurrentHashMap<>();
    private final ShardStrategy shardStrategy;

    @Getter private final String namespace;

    ShardAssignmentsContainer(ShardStrategy shardStrategy, String namespace) {
        if (Strings.isNullOrEmpty(namespace)) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        this.shardStrategy = shardStrategy;
        this.namespace = namespace;
    }

    public long getShardForKey(String key) {
        var test = shardStrategy.acceptsKeyPredicate(key);
        Optional<Shard> shard = shards.values().stream().filter(test).findFirst();

        if (shard.isPresent()) {
            return shard.get().id();
        } else {
            throw new NoShardAvailableException((key));
        }
    }

    public String leader(long shardId) {
        Shard shard = shards.get(shardId);
        if (shard == null) {
            throw new NoShardAvailableException(shardId);
        }

        return shard.leader();
    }

    void update(ShardManager.ShardAssignmentChanges changes) {
        changes.added().forEach(s -> shards.put(s.id(), s));
        changes.reassigned().forEach(s -> shards.put(s.id(), s));
        changes.removed().forEach(s -> shards.remove(s.id(), s));
    }

    Set<Long> allShardIds() {
        return shards.keySet();
    }

    Map<Long, Shard> allShards() {
        return shards;
    }
}
