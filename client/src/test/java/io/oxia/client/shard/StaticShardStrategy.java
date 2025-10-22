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

import static lombok.AccessLevel.PACKAGE;

import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Maps keys directly to shards, but exercises the mechanisms of the manager to do so. */
@RequiredArgsConstructor(access = PACKAGE)
class StaticShardStrategy implements ShardStrategy {
    private final Map<String, ShardAssignment> assignments = new HashMap<>();

    @Override
    public @NonNull Predicate<Shard> acceptsKeyPredicate(@NonNull String key) {
        return Optional.ofNullable(assignments.get(key))
                .map(a -> (Predicate<Shard>) s -> isEquivalent(s, a))
                .orElse(s -> false);
    }

    public @NonNull StaticShardStrategy assign(@NonNull String key, @NonNull ShardAssignment shard) {
        assignments.put(key, shard);
        return this;
    }

    public @NonNull StaticShardStrategy assign(
            @NonNull String key, @NonNull ShardAssignments response) {
        var nsShardsAssignment =
                response.getNamespacesMap().get(OxiaClientBuilderImpl.DefaultNamespace);
        if (nsShardsAssignment == null) {
            throw new NamespaceNotFoundException(OxiaClientBuilderImpl.DefaultNamespace);
        }
        if (nsShardsAssignment.getAssignmentsCount() != 1) {
            throw new IllegalArgumentException();
        }
        return assign(key, nsShardsAssignment.getAssignments(0));
    }

    public @NonNull StaticShardStrategy remove(@NonNull String key) {
        assignments.remove(key);
        return this;
    }

    private static boolean isEquivalent(Shard shard, ShardAssignment assignment) {
        return shard.id() == assignment.getShard()
                && shard.hashRange().minInclusive() == assignment.getInt32HashRange().getMinHashInclusive()
                && shard.hashRange().maxInclusive() == assignment.getInt32HashRange().getMaxHashInclusive();
    }
}
