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
package io.oxia.client;

import io.grpc.ClientCall;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.grpc.OxiaStubManager;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import io.oxia.proto.OxiaClientGrpc;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SequenceUpdates implements Closeable, StreamObserver<GetSequenceUpdatesResponse> {

    private final String key;
    private final String partitionKey;

    private final Consumer<String> listener;
    private final OxiaStubManager stubManager;
    private final ShardManager shardManager;
    private final Counter counterSequenceUpdatesReceived;
    private final Function<Void, Boolean> isClientClosed;

    private boolean closed = false;
    private ClientCall<GetSequenceUpdatesRequest, GetSequenceUpdatesResponse> call;

    SequenceUpdates(
            @NonNull String key,
            @NonNull String partitionKey,
            @NonNull Consumer<String> listener,
            @NonNull OxiaStubManager stubManager,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider,
            Function<Void, Boolean> isClientClosed) {
        this.key = key;
        this.partitionKey = partitionKey;
        this.listener = listener;
        this.stubManager = stubManager;
        this.shardManager = shardManager;
        this.isClientClosed = isClientClosed;

        this.counterSequenceUpdatesReceived =
                instrumentProvider.newCounter(
                        "oxia.client.sequence.updates.received",
                        Unit.Events,
                        "The total number of sequence updates received",
                        Attributes.empty());

        createStream();
    }

    private synchronized void createStream() {
        if (closed) {
            return;
        }

        long shardId = shardManager.getShardForKey(partitionKey);
        var leader = shardManager.leader(shardId);
        var stub = stubManager.getStub(leader).async();

        var request = GetSequenceUpdatesRequest.newBuilder().setShard(shardId).setKey(key).build();

        this.call =
                stub.getChannel()
                        .newCall(OxiaClientGrpc.getGetSequenceUpdatesMethod(), stub.getCallOptions());
        io.grpc.stub.ClientCalls.asyncServerStreamingCall(call, request, this);
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        call.cancel("closing streaming", null);
    }

    @Override
    public void onNext(GetSequenceUpdatesResponse value) {
        listener.accept(value.getHighestSequenceKey());
        counterSequenceUpdatesReceived.increment();
    }

    @Override
    public synchronized void onError(Throwable t) {
        if (closed || isClientClosed.apply(null)) {
            return;
        }
        log.warn("Failure while processing sequence updates: {}", t.getMessage(), t);
        createStream();
    }

    @Override
    public synchronized void onCompleted() {
        createStream();
    }
}
