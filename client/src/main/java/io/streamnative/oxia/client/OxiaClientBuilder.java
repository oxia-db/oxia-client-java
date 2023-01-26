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
package io.streamnative.oxia.client;


import io.streamnative.oxia.client.api.AsyncOxiaClient;
import io.streamnative.oxia.client.api.ClientBuilder;
import io.streamnative.oxia.client.api.Notification;
import io.streamnative.oxia.client.api.SyncOxiaClient;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OxiaClientBuilder implements ClientBuilder<OxiaClientBuilder> {

    public static final Duration DefaultBatchLinger = Duration.ofMillis(5);
    public static final int DefaultMaxRequestsPerBatch = 1000;
    public static final Duration DefaultRequestTimeout = Duration.ofSeconds(30);
    public static final int DefaultOperationQueueCapacity = 1000;

    @NonNull private final String serviceAddress;
    private Consumer<Notification> notificationCallback;
    @NonNull private Duration requestTimeout = DefaultRequestTimeout;
    @NonNull private Duration batchLinger = DefaultBatchLinger;
    private int maxRequestsPerBatch = DefaultMaxRequestsPerBatch;
    private int operationQueueCapacity = DefaultOperationQueueCapacity;

    @Override
    public @NonNull OxiaClientBuilder notificationCallback(
            @NonNull Consumer<Notification> notificationCallback) {
        this.notificationCallback = notificationCallback;
        return this;
    }

    public @NonNull OxiaClientBuilder requestTimeout(@NonNull Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public @NonNull OxiaClientBuilder batchLinger(@NonNull Duration batchLinger) {
        this.batchLinger = batchLinger;
        return this;
    }

    public @NonNull OxiaClientBuilder maxRequestsPerBatch(int maxRequestsPerBatch) {
        if (maxRequestsPerBatch < 0) {
            throw new IllegalArgumentException(
                    "MaxRequestsPerBatch must be greater than zero: " + maxRequestsPerBatch);
        }
        this.maxRequestsPerBatch = maxRequestsPerBatch;
        return this;
    }

    public @NonNull OxiaClientBuilder operationQueueCapacity(int operationQueueCapacity) {
        if (operationQueueCapacity < 0) {
            throw new IllegalArgumentException(
                    "operationQueueCapacity must be greater than zero: " + operationQueueCapacity);
        }
        this.operationQueueCapacity = operationQueueCapacity;
        return this;
    }

    public @NonNull AsyncOxiaClient asyncClient() {
        var config =
                new ClientConfig(
                        serviceAddress,
                        notificationCallback,
                        requestTimeout,
                        batchLinger,
                        maxRequestsPerBatch,
                        operationQueueCapacity);
        return new AsyncOxiaClientImpl(config);
    }

    public @NonNull SyncOxiaClient syncClient() {
        return new SyncOxiaClientImpl(asyncClient());
    }
}
