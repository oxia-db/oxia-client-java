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

import static java.util.Objects.requireNonNull;

public final class Generators {

    public static Generator<String> createKeyGenerator(KeyGeneratorOptions options) {
        return switch (options.type()) {
            case SEQUENTIAL -> new SequentialKeyGenerator(options);
            case UNIFORM -> new UniformKeyGenerator(options);
            case ZIPFIAN -> new ZipfianKeyGenerator(options);
        };
    }

    public static Generator<OperationType> createOperationGenerator(
            OperationGeneratorOptions options) {
        requireNonNull(options);
        if (!options.validate()) {
            throw new IllegalArgumentException(
                    "not validate operation. The probabilities do not sum to 100% ");
        }
        return new OperationGenerator(options);
    }

    public static Generator<byte[]> createFixedLengthValueGenerator(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size can not lower than or equals to 0");
        }
        return new FixedLengthValueGenerator(size);
    }
}
