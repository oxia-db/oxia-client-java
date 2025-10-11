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
package io.oxia.client.perf.ycsb.generator;

import java.util.concurrent.ThreadLocalRandom;

public class OperationGenerator implements Generator<OperationType> {
    private final double writeThreshold;
    private final double readThreshold;
    private final double scanThreshold;

    public OperationGenerator(OperationGeneratorOptions options) {
        this.writeThreshold = options.writePercentage();
        this.readThreshold = options.readPercentage() + writeThreshold;
        this.scanThreshold =
                options.scanPercentage() > 0 ? options.scanPercentage() + readThreshold : -1;
    }

    @Override
    public OperationType nextValue() {
        final int rand = ThreadLocalRandom.current().nextInt(100);
        if (rand < writeThreshold) {
            return OperationType.WRITE;
        } else if (rand < readThreshold) {
            return OperationType.READ;
        } else if (rand < scanThreshold) {
            return OperationType.SCAN;
        }
        throw new IllegalStateException("out of bound: " + rand);
    }
}
