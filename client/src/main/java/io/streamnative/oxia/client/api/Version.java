/*
 * Copyright © 2022-2023 StreamNative Inc.
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

import static io.streamnative.oxia.client.ProtoUtil.VersionIdNotExists;

import lombok.NonNull;

/**
 * Oxia record metadata.
 *
 * @param versionId The current versionId of the record.
 * @param createdTimestamp The instant at which the record was created. In epoch milliseconds.
 * @param modifiedTimestamp The instant at which the record was last updated. In epoch milliseconds.
 */
public record Version(long versionId, long createdTimestamp, long modifiedTimestamp) {
    /** Represents the state where a versionId of a record (and thus the record) does not exist. */
    public Version {
        requireValidVersionId(versionId);
        requireValidTimestamp(createdTimestamp);
        requireValidTimestamp(modifiedTimestamp);
    }

    /**
     * Checks that the versionId value is either {@link
     * io.streamnative.oxia.client.ProtoUtil#VersionIdNotExists} or positive.
     *
     * @param versionId The versionId to validate.
     */
    public static void requireValidVersionId(long versionId) {
        if (versionId < VersionIdNotExists) {
            throw new IllegalArgumentException("Invalid versionId: " + versionId);
        }
    }

    /**
     * Checks that the timestamp is valid (positive).
     *
     * @param timestamp The timestamp to validate.
     */
    public static void requireValidTimestamp(long timestamp) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
    }

    public static @NonNull Version fromProto(@NonNull io.streamnative.oxia.proto.Version version) {
        return new Version(
                version.getVersionId(), version.getCreatedTimestamp(), version.getModifiedTimestamp());
    }
}
