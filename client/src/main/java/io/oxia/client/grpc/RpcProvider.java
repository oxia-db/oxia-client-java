/*
 * Copyright © 2022-2026 The Oxia Authors
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
package io.oxia.client.grpc;

import io.grpc.stub.StreamObserver;
import io.oxia.client.ClientConfig;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.proto.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongFunction;
import lombok.NonNull;

public interface RpcProvider extends AutoCloseable {
    static RpcProvider create(
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService asyncExecutor,
            @NonNull LongFunction<String> shardLeaderProvider) {
        return new GrpcRpcProvider(clientConfig, asyncExecutor, shardLeaderProvider);
    }

    void getShardAssignments(
            @NonNull ShardAssignmentsRequest request, @NonNull StreamObserver<ShardAssignments> observer);

    void getNotifications(
            @NonNull NotificationsRequest request, @NonNull StreamObserver<NotificationBatch> observer);

    CompletableFuture<CreateSessionResponse> createSession(@NonNull CreateSessionRequest request);

    CompletableFuture<KeepAliveResponse> keepAlive(
            @NonNull SessionHeartbeat heartbeat, @NonNull Duration timeout);

    CompletableFuture<CloseSessionResponse> closeSession(@NonNull CloseSessionRequest request);

    void read(@NonNull ReadRequest request, @NonNull StreamObserver<ReadResponse> observer);

    StreamObserver<WriteRequest> writeStream(long shardId, StreamObserver<WriteResponse> responseObserver);

    ManagedWriteStream getWriteStream(long shardId);

    void list(@NonNull ListRequest request, @NonNull CancelableStreamObserver<ListResponse> observer);

    void rangeScan(
            @NonNull RangeScanRequest request,
            @NonNull CancelableStreamObserver<RangeScanResponse> observer);

    void getSequenceUpdates(
            @NonNull GetSequenceUpdatesRequest request,
            @NonNull CancelableStreamObserver<GetSequenceUpdatesResponse> observer);
}
