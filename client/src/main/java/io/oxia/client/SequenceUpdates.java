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
import io.oxia.client.util.Watchdog;
import io.oxia.proto.GetSequenceUpdatesRequest;
import io.oxia.proto.GetSequenceUpdatesResponse;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.NonNull;

public class SequenceUpdates implements Closeable {
    private static final Logger log = Logger.get(SequenceUpdates.class);

    private final String key;
    private final String partitionKey;

    private final Consumer<String> listener;
    private final RpcProvider rpcProvider;
    private final ShardManager shardManager;
    private final Counter counterSequenceUpdatesReceived;
    private final Function<Void, Boolean> isClientClosed;
    private final ScheduledExecutorService executor;
    private final long refreshIntervalMillis;
    private final Watchdog watchdog;

    private boolean closed = false;
    private CancelableStreamObserver<?> stream;
    private volatile String lastDeliveredKey;

    SequenceUpdates(
            @NonNull String key,
            @NonNull String partitionKey,
            @NonNull Consumer<String> listener,
            @NonNull RpcProvider rpcProvider,
            @NonNull ShardManager shardManager,
            @NonNull InstrumentProvider instrumentProvider,
            Function<Void, Boolean> isClientClosed,
            @NonNull ScheduledExecutorService executor,
            @NonNull Duration refreshInterval) {
        this.key = key;
        this.partitionKey = partitionKey;
        this.listener = listener;
        this.rpcProvider = rpcProvider;
        this.shardManager = shardManager;
        this.isClientClosed = isClientClosed;
        this.executor = executor;
        this.refreshIntervalMillis = refreshInterval.toMillis();
        this.watchdog = new Watchdog(executor, refreshInterval);

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

        final var currentStream = stream;
        if (currentStream != null) {
            currentStream.cancel();
        }

        long shardId = shardManager.getShardForKey(partitionKey);
        var request = new GetSequenceUpdatesRequest();
        request.setShard(shardId).setKey(key);

        var observer =
                new CancelableStreamObserver<GetSequenceUpdatesResponse>() {
                    @Override
                    protected void handleNext(@NonNull GetSequenceUpdatesResponse value) {
                        SequenceUpdates.this.handleUpdate(value);
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
        watchdog.start(this::refreshStream);
    }

    @Override
    public void close() throws IOException {
        final CancelableStreamObserver<?> currentStream;
        synchronized (this) {
            closed = true;
            currentStream = stream;
        }
        watchdog.close();
        if (currentStream != null) {
            currentStream.cancel();
        }
    }

    private void handleUpdate(@NonNull GetSequenceUpdatesResponse value) {
        watchdog.pet();
        final var key = value.getHighestSequenceKey();
        if (key.equals(lastDeliveredKey)) {
            // A recreated stream re-sends the current highest sequence key; it was already
            // delivered, so skip the duplicate.
            return;
        }
        lastDeliveredKey = key;
        listener.accept(key);
        counterSequenceUpdatesReceived.increment();
    }

    private synchronized void handleError(@NonNull Throwable t) {
        if (closed || isClientClosed.apply(null)) {
            return;
        }
        watchdog.pet();
        log.warn().exception(t).log("Failure while processing sequence updates");
        createStream();
    }

    private synchronized void handleCompleted() {
        if (closed) {
            return;
        }
        watchdog.pet();
        createStream();
    }

    /**
     * Recreates the stream, invoked by the watchdog when no update has been received for longer than
     * the refresh interval.
     *
     * <p>An idle key is legitimately silent, so recreating the stream re-registers with the current
     * leader and re-reads the current highest sequence key; duplicates are skipped by {@link
     * #lastDeliveredKey}.
     */
    private void refreshStream() {
        if (closed) {
            return;
        }
        log.warn(
                "No sequence updates received for " + refreshIntervalMillis + " ms, recreating the stream");
        createStream();
    }
}
