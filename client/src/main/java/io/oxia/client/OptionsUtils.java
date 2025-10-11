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

import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.defs.OptionComparisonType;
import io.oxia.client.api.options.defs.OptionEphemeral;
import io.oxia.client.api.options.defs.OptionPartitionKey;
import io.oxia.client.api.options.defs.OptionSecondaryIndex;
import io.oxia.client.api.options.defs.OptionSecondaryIndexName;
import io.oxia.client.api.options.defs.OptionSequenceKeysDeltas;
import io.oxia.client.api.options.defs.OptionVersionId;
import io.oxia.proto.KeyComparisonType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import lombok.experimental.UtilityClass;

@UtilityClass
public class OptionsUtils {

    public static OptionalLong getVersionId(Set<?> options) {
        if (options == null || options.isEmpty()) {
            return OptionalLong.empty();
        }

        OptionalLong versionId = OptionalLong.empty();
        for (var o : options) {
            if (o instanceof OptionVersionId e) {
                if (versionId.isPresent()) {
                    throw new IllegalArgumentException(
                            "VersionId cannot be passed multiple times: " + options);
                }

                versionId = OptionalLong.of(e.versionId());
            }
        }

        return versionId;
    }

    public static boolean isEphemeral(Set<?> options) {
        if (options.isEmpty()) {
            return false;
        }

        for (var option : options) {
            if (option instanceof OptionEphemeral) {
                return true;
            }
        }

        return false;
    }

    public static Optional<String> getPartitionKey(Set<?> options) {
        if (options == null || options.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> partitionKey = Optional.empty();
        for (var o : options) {
            if (o instanceof OptionPartitionKey pk) {
                if (partitionKey.isPresent()) {
                    throw new IllegalArgumentException("PartitionKey can only specified once:  " + options);
                }

                partitionKey = Optional.of(pk.partitionKey());
            }
        }

        return partitionKey;
    }

    public static Optional<List<Long>> getSequenceKeysDeltas(Set<?> options) {
        if (options == null || options.isEmpty()) {
            return Optional.empty();
        }

        Optional<List<Long>> sequenceKeysDeltas = Optional.empty();
        for (var o : options) {
            if (o instanceof OptionSequenceKeysDeltas skd) {
                if (sequenceKeysDeltas.isPresent()) {
                    throw new IllegalArgumentException(
                            "SequencesKeysDeltas can only specified once:  " + options);
                }

                sequenceKeysDeltas = Optional.of(skd.sequenceKeysDeltas());
            }
        }

        return sequenceKeysDeltas;
    }

    public static KeyComparisonType getComparisonType(Set<GetOption> options) {
        if (options == null || options.isEmpty()) {
            return KeyComparisonType.EQUAL;
        }

        boolean alreadyHasComparisonType = false;
        KeyComparisonType comparisonType = KeyComparisonType.EQUAL;
        for (GetOption o : options) {
            if (o instanceof OptionComparisonType e) {

                if (alreadyHasComparisonType) {
                    throw new IllegalArgumentException(
                            "Incompatible " + GetOption.class.getSimpleName() + "s: " + options);
                }

                comparisonType =
                        switch (e.comparisonType()) {
                            case Equal -> KeyComparisonType.EQUAL;
                            case Floor -> KeyComparisonType.FLOOR;
                            case Ceiling -> KeyComparisonType.CEILING;
                            case Lower -> KeyComparisonType.LOWER;
                            case Higher -> KeyComparisonType.HIGHER;
                        };
                alreadyHasComparisonType = true;
            }
        }

        return comparisonType;
    }

    public static List<OptionSecondaryIndex> getSecondaryIndexes(Set<?> options) {
        if (options == null || options.isEmpty()) {
            return Collections.emptyList();
        }

        List<OptionSecondaryIndex> res = null;
        for (var o : options) {
            if (o instanceof OptionSecondaryIndex si) {
                if (res == null) {
                    res = new ArrayList<>();
                }

                res.add(si);
            }
        }

        return res != null ? res : Collections.emptyList();
    }

    public static Optional<String> getSecondaryIndexName(Set<?> options) {
        if (options == null || options.isEmpty()) {
            return Optional.empty();
        }

        Optional<String> secondaryIndexName = Optional.empty();
        for (var o : options) {
            if (o instanceof OptionSecondaryIndexName sin) {
                if (secondaryIndexName.isPresent()) {
                    throw new IllegalArgumentException(
                            "Secondary index can only specified once:  " + options);
                }

                secondaryIndexName = Optional.of(sin.secondaryIndexName());
            }
        }

        return secondaryIndexName;
    }
}
