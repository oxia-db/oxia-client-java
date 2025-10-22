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
package io.oxia.client;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.proto.GetResponse;
import io.oxia.proto.PutResponse;
import java.util.Optional;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ProtoUtil {

    public static long uint32ToLong(int n) {
        if (n >= 0) {
            return n;
        } else {
            // The sign bit is converted into the leading bit
            return (1L << 31) + (n & 0x7FFFFFFF);
        }
    }

    public static @NonNull PutResult getPutResultFromProto(
            @NonNull String originalKey, @NonNull PutResponse response) {
        String key = response.hasKey() ? response.getKey() : originalKey;
        return new PutResult(key, getVersionFromProto(response.getVersion()));
    }

    public static @NonNull GetResult getResultFromProto(
            @NonNull String originalKey, @NonNull GetResponse response) {
        String key = originalKey;
        if (response.hasKey()) {
            key = response.getKey();
        }
        return new GetResult(
                key, response.getValue().toByteArray(), getVersionFromProto(response.getVersion()));
    }

    public static @NonNull Version getVersionFromProto(@NonNull io.oxia.proto.Version version) {
        return new Version(
                version.getVersionId(),
                version.getCreatedTimestamp(),
                version.getModifiedTimestamp(),
                version.getModificationsCount(),
                version.hasSessionId() ? Optional.of(version.getSessionId()) : Optional.empty(),
                version.hasClientIdentity() ? Optional.of(version.getClientIdentity()) : Optional.empty());
    }
}
