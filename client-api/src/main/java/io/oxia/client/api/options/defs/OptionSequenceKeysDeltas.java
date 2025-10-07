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
package io.oxia.client.api.options.defs;

import io.oxia.client.api.options.PutOption;
import java.util.List;

/**
 * @hidden
 */
public record OptionSequenceKeysDeltas(List<Long> sequenceKeysDeltas) implements PutOption {
    public OptionSequenceKeysDeltas {
        if (sequenceKeysDeltas.isEmpty()) {
            throw new IllegalArgumentException("Sequence keys deltas cannot be empty");
        }

        if (sequenceKeysDeltas.get(0) <= 0) {
            throw new IllegalArgumentException("The first sequence keys delta must be > 0");
        }

        for (int i = 1; i < sequenceKeysDeltas.size(); i++) {
            if (sequenceKeysDeltas.get(i) < 0) {
                throw new IllegalArgumentException("Sequence keys delta must be >= 0");
            }
        }
    }
}
