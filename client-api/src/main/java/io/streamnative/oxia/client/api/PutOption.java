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
package io.streamnative.oxia.client.api;

import java.util.List;
import lombok.NonNull;

public sealed interface PutOption
        permits OptionEphemeral, OptionPartitionKey, OptionSequenceKeysDeltas, OptionVersionId {

    PutOption IfRecordDoesNotExist = new OptionVersionId.OptionRecordDoesNotExist();
    PutOption AsEphemeralRecord = new OptionEphemeral();

    static OptionVersionId.OptionVersionIdEqual IfVersionIdEquals(long versionId) {
        return new OptionVersionId.OptionVersionIdEqual(versionId);
    }

    /**
     * PartitionKey overrides the partition routing with the specified `partitionKey` instead of the
     * regular record key.
     *
     * <p>Records with the same partitionKey will always be guaranteed to be co-located in the same
     * Oxia shard.
     *
     * @param partitionKey the partition key to use
     */
    static PutOption PartitionKey(@NonNull String partitionKey) {
        return new OptionPartitionKey(partitionKey);
    }

    /**
     * SequenceKeysDeltas will request that the final record key to be assigned by the server, based
     * on the prefix record key and appending one or more sequences.
     *
     * <p>The sequence numbers will be atomically added based on the deltas. Deltas must be >= 0 and
     * the first one strictly > 0. SequenceKeysDeltas also requires that a [PartitionKey] option is
     * provided.
     *
     * @param sequenceKeysDeltas
     * @return
     */
    static PutOption SequenceKeysDeltas(@NonNull List<Long> sequenceKeysDeltas) {
        return new OptionSequenceKeysDeltas(sequenceKeysDeltas);
    }
}
