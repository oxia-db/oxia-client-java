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

import static com.google.common.base.Throwables.getRootCause;

import io.github.merlimat.slog.Logger;
import io.grpc.Status;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import lombok.NonNull;

public class SequenceUpdates implements Closeable {

    private static final Logger log = Logger.get(SequenceUpdates.class);

    private final String key;
    private final String partitionKey;

    private final Consumer<String> listener;
    private final RpcProvider rpcProvider;
    private final ShardManager shardManager;
    private final Counter counterSequenceUpdatesReceived;
    private final BooleanSupplier isClientClosed;
    private final ScheduledExecutorService executor;

    private boolean closed = false;
    private CancelableStreamObserver<?> stream;
    private String lastDeliveredSequenceKey;

    SequenceUpdates(
            @NonNull String key,
            @NonNull String partitionKey,
            @NonNull Consumer<String> listener,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider,
            @NonNull BooleanSupplier isClientClosed,
            @NonNull ScheduledExecutorService executor) {
        this.key = key;
        this.partitionKey = partitionKey;
        this.listener = listener;
        this.rpcProvider = rpcProvider;
        this.shardManager = shardManager;
        this.isClientClosed = isClientClosed;
        this.executor = executor;

        this.counterSequenceUpdatesReceived =
                instrumentProvider.newCounter(
                        "oxia.client.sequence.updates.received",
                        Unit.Events,
                        "The total number of sequence updates received",
                        Attributes.empty());

        createStream();
    }

    private synchronized void createStream() {
        if (closed || isClientClosed.getAsBoolean()) {
            return;
        }

        long shardId = shardManager.getShardForKey(partitionKey);
        var request = new GetSequenceUpdatesRequest();
        request.setShard(shardId).setKey(key);

        var observer =
                new CancelableStreamObserver<GetSequenceUpdatesResponse>() {
                    private boolean firstResponse = true;

                    @Override
                    protected void handleNext(@NonNull GetSequenceUpdatesResponse value) {
                        var replayCandidate = firstResponse;
                        firstResponse = false;
                        SequenceUpdates.this.handleUpdate(value, replayCandidate);
                    }

                    @Override
                    protected void handleError(@NonNull Throwable t) {
                        SequenceUpdates.this.handleError(t);
                    }

                    @Override
                    protected void handleComplete() {
                        SequenceUpdates.this.handleCompleted();
                    }
                };

        stream = observer;
        rpcProvider.getSequenceUpdates(request, observer);
    }

    @Override
    public void close() throws IOException {
        final CancelableStreamObserver<?> currentStream;
        synchronized (this) {
            closed = true;
            currentStream = stream;
        }
        if (currentStream != null) {
            currentStream.cancel();
        }
    }

    private synchronized void handleUpdate(
            @NonNull GetSequenceUpdatesResponse value, boolean replayCandidate) {
        var highestSequenceKey = value.getHighestSequenceKey();
        if (replayCandidate && highestSequenceKey.equals(lastDeliveredSequenceKey)) {
            // A renewed subscription starts with the current key. Suppress that initial snapshot
            // only when it repeats the last callback; later equal or lower keys can be real updates
            // after sequence records are deleted and recreated.
            return;
        }
        lastDeliveredSequenceKey = highestSequenceKey;
        listener.accept(highestSequenceKey);
        counterSequenceUpdatesReceived.increment();
    }

    private synchronized void handleError(@NonNull Throwable t) {
        if (closed || isClientClosed.getAsBoolean()) {
            return;
        }
        if (Status.fromThrowable(getRootCause(t)).getCode() == Status.Code.DEADLINE_EXCEEDED) {
            log.debug("Sequence updates subscription reached its configured maximum age");
        } else {
            log.warn().exception(t).log("Failure while processing sequence updates");
        }
        scheduleRestart();
    }

    private synchronized void handleCompleted() {
        if (closed || isClientClosed.getAsBoolean()) {
            return;
        }
        scheduleRestart();
    }

    private void scheduleRestart() {
        executor.execute(this::createStream);
    }
}
