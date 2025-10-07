/*
 * Copyright © 2022-2025 StreamNative Inc.
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
package io.oxia.client.api;

import java.util.List;
import java.util.Set;

public sealed interface PutOption
        permits OptionEphemeral,
                OptionPartitionKey,
                OptionSecondaryIndex,
                OptionSequenceKeysDeltas,
                OptionVersionId {

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
    static PutOption PartitionKey(String partitionKey) {
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
    static PutOption SequenceKeysDeltas(List<Long> sequenceKeysDeltas) {
        return new OptionSequenceKeysDeltas(sequenceKeysDeltas);
    }

    /**
     * SecondaryIndex let the users specify additional keys to index the record Index names are
     * arbitrary strings and can be used in {@link SyncOxiaClient#list(String, String, Set)} and
     * {@link SyncOxiaClient#rangeScan(String, String, Set)} requests.
     *
     * <p>Secondary keys are not required to be unique.
     *
     * <p>Multiple secondary indexes can be passed on the same record, even reusing multiple times the
     * same indexName.
     *
     * @param indexName the name of the secondary index
     * @param secondaryKey the secondary key for this record
     * @return
     */
    static PutOption SecondaryIndex(String indexName, String secondaryKey) {
        return new OptionSecondaryIndex(indexName, secondaryKey);
    }
}
