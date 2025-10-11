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

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;

import io.oxia.proto.Int32HashRange;
import io.oxia.proto.NamespaceShardsAssignment;
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import lombok.NonNull;

public class ModelFactory {
    static @NonNull Int32HashRange newHashRange(int min, int max) {
        return Int32HashRange.newBuilder().setMinHashInclusive(min).setMaxHashInclusive(max).build();
    }

    static @NonNull ShardAssignment newShardAssignment(
            long id, int min, int max, @NonNull String leader) {
        return ShardAssignment.newBuilder()
                .setShard(id)
                .setLeader(leader)
                .setInt32HashRange(newHashRange(min, max))
                .build();
    }

    static @NonNull ShardAssignments newShardAssignments(
            long id, int min, int max, @NonNull String leader) {
        return ShardAssignments.newBuilder()
                .putNamespaces(
                        DefaultNamespace,
                        NamespaceShardsAssignment.newBuilder()
                                .addAssignments(newShardAssignment(id, min, max, leader))
                                .build())
                .build();
    }
}
