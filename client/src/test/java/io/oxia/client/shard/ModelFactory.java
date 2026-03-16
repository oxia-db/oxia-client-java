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
import io.oxia.proto.ShardAssignment;
import io.oxia.proto.ShardAssignments;
import lombok.NonNull;

public class ModelFactory {
    static @NonNull Int32HashRange newHashRange(int min, int max) {
        var range = new Int32HashRange();
        range.setMinHashInclusive(min).setMaxHashInclusive(max);
        return range;
    }

    static @NonNull ShardAssignment newShardAssignment(
            long id, int min, int max, @NonNull String leader) {
        var assignment = new ShardAssignment();
        assignment.setShard(id).setLeader(leader);
        assignment.setInt32HashRange().setMinHashInclusive(min).setMaxHashInclusive(max);
        return assignment;
    }

    static @NonNull ShardAssignments newShardAssignments(
            long id, int min, int max, @NonNull String leader) {
        var assignments = new ShardAssignments();
        var nsAssignment = assignments.putNamespaces(DefaultNamespace);
        nsAssignment.addAssignment().copyFrom(newShardAssignment(id, min, max, leader));
        return assignments;
    }
}
