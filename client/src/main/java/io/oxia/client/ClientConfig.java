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

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.Authentication;
import java.time.Duration;
import javax.annotation.Nullable;
import lombok.NonNull;

public record ClientConfig(
        @NonNull String serviceAddress,
        @NonNull Duration requestTimeout,
        @NonNull Duration batchLinger,
        int maxRequestsPerBatch,
        int maxBatchSize,
        @NonNull Duration sessionTimeout,
        @NonNull String clientIdentifier,
        OpenTelemetry openTelemetry,
        @NonNull String namespace,
        @Nullable Authentication authentication,
        boolean enableTls,
        @NonNull Duration connectionBackoffMinDelay,
        @NonNull Duration connectionBackoffMaxDelay,
        Duration connectionKeepAliveTime,
        Duration connectionKeepAliveTimeout,
        int maxConnectionPerNode) {}
