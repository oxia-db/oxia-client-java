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

/**
 * The PutOption interface defines a set of options for customizing the behavior of a "put"
 * operation in a data store. These options allow fine-grained control over conditions,
 * partitioning, indexing, and key management associated with the operation.
 */
public sealed interface PutOption
        permits OptionEphemeral,
                OptionPartitionKey,
                OptionSecondaryIndex,
                OptionSequenceKeysDeltas,
                OptionVersionId {

    /**
     * Specifies that the operation should only proceed if the record does not already exist. This
     * option is useful for enforcing the creation of a new record exclusively when no existing record
     * is found with the specified key.
     *
     * <p>When this option is used, if a record with the provided key already exists, the operation
     * will not proceed. The condition ensures idempotency in create operations.
     */
    PutOption IfRecordDoesNotExist = new OptionVersionId.OptionRecordDoesNotExist();

    /**
     * Indicates that the record should be treated as ephemeral. Ephemeral records are scoped to a
     * specific session, and their lifecycle is tied to the session's duration. Once the session
     * expires or is terminated, the ephemeral record will no longer exist.
     *
     * <p>This option is typically used when records do not need to persist beyond the lifespan of a
     * client session.
     */
    PutOption AsEphemeralRecord = new OptionEphemeral();

    /**
     * Only put the record if the versionId matches the current versionId.
     *
     * @param versionId the version ID to be used for the equality condition
     * @return an {@link OptionVersionId.OptionVersionIdEqual} instance representing the version ID
     *     equality condition
     */
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
     * @return the PutOption.
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
     * @param sequenceKeysDeltas a list of sequence numbers to be added to the record key
     * @return the PutOption.
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
     * @return the PutOption.
     */
    static PutOption SecondaryIndex(String indexName, String secondaryKey) {
        return new OptionSecondaryIndex(indexName, secondaryKey);
    }
}
