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

import io.github.merlimat.slog.Logger;
import io.opentelemetry.api.common.Attributes;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.client.metrics.Counter;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.metrics.Unit;
import io.oxia.client.shard.ShardManager;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;

public class SequenceUpdates extends CancelableStreamObserver<GetSequenceUpdatesResponse>
        implements Closeable {

    private static final Logger log = Logger.get(SequenceUpdates.class);

    private final String key;
    private final String partitionKey;

    private final Consumer<String> listener;
    private final RpcProvider rpcProvider;
    private final ShardManager shardManager;
    private final Counter counterSequenceUpdatesReceived;
    private final Function<Void, Boolean> isClientClosed;

    private boolean closed = false;

    SequenceUpdates(
            @NonNull String key,
            @NonNull String partitionKey,
            @NonNull Consumer<String> listener,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider,
            Function<Void, Boolean> isClientClosed) {
        this.key = key;
        this.partitionKey = partitionKey;
        this.listener = listener;
        this.rpcProvider = rpcProvider;
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
        var request = new GetSequenceUpdatesRequest();
        request.setShard(shardId).setKey(key);

        rpcProvider.getSequenceUpdates(request, this);
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        cancel();
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
        log.warn().exception(t).log("Failure while processing sequence updates");
        createStream();
    }

    @Override
    public synchronized void onCompleted() {
        createStream();
    }
}
