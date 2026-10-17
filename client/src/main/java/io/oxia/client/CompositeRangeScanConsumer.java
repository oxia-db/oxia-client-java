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
package io.oxia.client;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.grpc.observer.CancelableStreamObserver;
import io.oxia.proto.RangeScanResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Merges the range scans of the shards into one scan in key order: by secondary key, then by
 * primary key, for a scan through an index, as each shard returns its records. It does what the Go
 * client's aggregateAndSortRangeScanAcrossShards does.
 *
 * <p>Each shard stream uses manual flow control: it is paused after each response, and resumed once
 * the records of that response have been delivered, so the merge holds at most one response per
 * shard. To a flow-controlled consumer, the merge is a single stream, which it resumes on the
 * consumer's request.
 */
final class CompositeRangeScanConsumer implements FlowControlledRangeScanConsumer.StreamHandle {

    private final RangeScanConsumer consumer;
    private final FlowControlledRangeScanConsumer flowControl;
    private final List<ShardObserver> shards = new ArrayList<>();
    // Not held while requesting or cancelling a shard stream, which can deliver inline
    private final ReentrantLock lock = new ReentrantLock();
    private boolean done = false;
    private boolean waitingForConsumer = false;
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * @param flowControl the consumer, if it consumes records on demand, or null
     */
    CompositeRangeScanConsumer(
            RangeScanConsumer consumer, FlowControlledRangeScanConsumer flowControl) {
        this.consumer = consumer;
        this.flowControl = flowControl;
    }

    /** Adds a shard stream. All the shards are added before any of their streams starts. */
    CancelableStreamObserver<RangeScanResponse> newShard() {
        var shard = new ShardObserver();
        shards.add(shard);
        return shard;
    }

    @Override
    public void requestNext() {
        List<ShardObserver> toRequest;
        lock.lock();
        try {
            waitingForConsumer = false;
            toRequest = takeShardsToResume();
        } finally {
            lock.unlock();
        }
        toRequest.forEach(ShardObserver::requestNextMessage);
    }

    @Override
    public void cancel() {
        lock.lock();
        try {
            done = true;
        } finally {
            lock.unlock();
        }
        shards.forEach(ShardObserver::cancel);
    }

    /**
     * Delivers the records in order for as long as every shard that hasn't completed has a record
     * pending, and returns what to do once the lock is released.
     */
    private Runnable deliver() {
        while (true) {
            ShardObserver next = null;
            for (var shard : shards) {
                if (!shard.queue.isEmpty()) {
                    if (next == null
                            || CompareWithSlash.GET_RESULTS.compare(shard.queue.peek(), next.queue.peek()) < 0) {
                        next = shard;
                    }
                    continue;
                }
                if (shard.completed) {
                    continue;
                }
                // Resume the paused shards whose records have all been delivered
                if (flowControl == null) {
                    var toRequest = takeShardsToResume();
                    return () -> toRequest.forEach(ShardObserver::requestNextMessage);
                }
                if (waitingForConsumer || shards.stream().noneMatch(ShardObserver::canResume)) {
                    return () -> {};
                }
                waitingForConsumer = true;
                return () -> flowControl.onStreamIdle(this);
            }
            if (next == null) {
                done = true;
                consumer.onCompleted();
                return () -> {};
            }
            if (!consumer.onNext(next.queue.poll())) {
                done = true;
                consumer.onCompleted();
                return () -> shards.forEach(ShardObserver::cancel);
            }
        }
    }

    private List<ShardObserver> takeShardsToResume() {
        var toResume = new ArrayList<ShardObserver>();
        for (var shard : shards) {
            if (shard.canResume()) {
                shard.paused = false;
                toResume.add(shard);
            }
        }
        return toResume;
    }

    private final class ShardObserver extends CancelableStreamObserver<RangeScanResponse> {
        // Guarded by the lock of the merge
        private final ArrayDeque<GetResult> queue = new ArrayDeque<>();
        private boolean paused = false;
        private boolean completed = false;

        ShardObserver() {
            super(true);
        }

        private boolean canResume() {
            return paused && queue.isEmpty() && !completed;
        }

        @Override
        protected void handleNext(RangeScanResponse response) {
            // The merge only waits for the consumer once it has records, so it is registered
            // with the first response. It can cancel the merge right away, so the lock isn't held.
            if (flowControl != null && started.compareAndSet(false, true)) {
                flowControl.onStreamStarted(CompositeRangeScanConsumer.this);
            }
            var results = new ArrayList<GetResult>(response.getRecordsCount());
            for (int i = 0; i < response.getRecordsCount(); i++) {
                results.add(ProtoUtil.getResultFromProto("", response.getRecordAt(i)));
            }
            Runnable after;
            lock.lock();
            try {
                if (done) {
                    after = this::cancel;
                } else {
                    queue.addAll(results);
                    paused = true;
                    after = deliver();
                }
            } finally {
                lock.unlock();
            }
            after.run();
        }

        @Override
        protected void handleError(Throwable t) {
            lock.lock();
            try {
                if (done) {
                    return;
                }
                done = true;
                consumer.onError(t);
            } finally {
                lock.unlock();
            }
            // The paused shards don't deliver anymore, so they would not notice the end of the scan
            shards.forEach(ShardObserver::cancel);
        }

        @Override
        protected void handleComplete() {
            Runnable after;
            lock.lock();
            try {
                if (done) {
                    return;
                }
                completed = true;
                after = deliver();
            } finally {
                lock.unlock();
            }
            after.run();
        }
    }
}
