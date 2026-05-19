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
import io.oxia.proto.CloseSessionRequest;
import io.oxia.proto.CloseSessionResponse;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.ListRequest;
import io.oxia.proto.ListResponse;
import io.oxia.proto.NotificationBatch;
import io.oxia.proto.NotificationsRequest;
import io.oxia.proto.RangeScanRequest;
import io.oxia.proto.RangeScanResponse;
import io.oxia.proto.ReadRequest;
import io.oxia.proto.ReadResponse;
import io.oxia.proto.SessionHeartbeat;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongFunction;
import lombok.NonNull;

public interface RpcProvider extends AutoCloseable {
    static RpcProvider create(
            @NonNull ClientConfig clientConfig,
            @NonNull ScheduledExecutorService executor,
            @NonNull LongFunction<String> shardLeaderProvider) {
        return new GrpcRpcProvider(clientConfig, executor, shardLeaderProvider);
    }

    void getShardAssignments(
            @NonNull ShardAssignmentsRequest request, @NonNull StreamObserver<ShardAssignments> observer);

    void getNotifications(
            @NonNull NotificationsRequest request, @NonNull StreamObserver<NotificationBatch> observer);

    CompletableFuture<CreateSessionResponse> createSession(@NonNull CreateSessionRequest request);

    void keepAlive(
            @NonNull SessionHeartbeat heartbeat, @NonNull StreamObserver<KeepAliveResponse> observer);

    CompletableFuture<CloseSessionResponse> closeSession(@NonNull CloseSessionRequest request);

    void read(@NonNull ReadRequest request, @NonNull StreamObserver<ReadResponse> observer);

    CompletableFuture<WriteResponse> write(@NonNull WriteRequest request);

    void list(@NonNull ListRequest request, @NonNull CancelableStreamObserver<ListResponse> observer);

    void rangeScan(
            @NonNull RangeScanRequest request,
            @NonNull CancelableStreamObserver<RangeScanResponse> observer);

    void getSequenceUpdates(
            @NonNull GetSequenceUpdatesRequest request,
            @NonNull CancelableStreamObserver<GetSequenceUpdatesResponse> observer);
}
