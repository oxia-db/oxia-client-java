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
package io.oxia.client.batch;

import static lombok.AccessLevel.PACKAGE;

import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.OxiaStubProvider;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = PACKAGE)
abstract class BatchFactory {
    final @NonNull OxiaStubProvider stubProvider;

    @Getter(PACKAGE)
    private final @NonNull ClientConfig config;

    public abstract Batch getBatch(long shardId);
}
