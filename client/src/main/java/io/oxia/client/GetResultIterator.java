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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import lombok.SneakyThrows;

public class GetResultIterator
        implements Iterator<GetResult>,
                RangeScanConsumer,
                FlowControlledRangeScanConsumer,
                AutoCloseable {

    // Records received but not yet consumed. Bounded through flow control: each underlying
    // stream has at most one response in flight, and idle streams are only resumed once the
    // buffer has been drained.
    private final Deque<GetResult> buffer = new ArrayDeque<>();

    // A scan without a partition key opens one stream per shard, all feeding this iterator.
    private final List<StreamHandle> streams = new ArrayList<>();
    private final List<StreamHandle> idleStreams = new ArrayList<>();

    private Throwable error = null;
    private boolean completed = false;
    private boolean closed = false;

    @Override
    public void onStreamStarted(StreamHandle handle) {
        boolean cancelNow;
        synchronized (this) {
            cancelNow = closed;
            if (!cancelNow) {
                streams.add(handle);
            }
        }
        if (cancelNow) {
            handle.cancel();
        }
    }

    @Override
    public void onStreamIdle(StreamHandle handle) {
        boolean requestNow;
        synchronized (this) {
            if (closed) {
                return;
            }
            requestNow = buffer.isEmpty();
            if (!requestNow) {
                idleStreams.add(handle);
            }
        }
        // Invoked outside the monitor: with a direct executor gRPC can deliver the next
        // response inline, re-entering onNext()/onError() from within this call.
        if (requestNow) {
            handle.requestNext();
        }
    }

    @Override
    public synchronized boolean onNext(GetResult result) {
        if (closed) {
            return false;
        }
        buffer.add(result);
        notifyAll();
        return true;
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        // Don't cancel the other streams here: onError can be invoked while holding another
        // stream observer's lock, and cancelling a stream takes its observer's lock. The
        // streams are cancelled by close().
        this.error = new Exception("Range scan error", throwable);
        notifyAll();
    }

    @Override
    public synchronized void onCompleted() {
        this.completed = true;
        notifyAll();
    }

    @Override
    @SneakyThrows
    public synchronized boolean hasNext() {
        while (error == null && !completed && buffer.isEmpty() && !closed) {
            wait();
        }

        if (error != null) {
            throw new RuntimeException(error);
        }

        return !buffer.isEmpty();
    }

    @Override
    @SneakyThrows
    public GetResult next() {
        GetResult res;
        List<StreamHandle> toRequest = null;
        synchronized (this) {
            while (error == null && !completed && buffer.isEmpty() && !closed) {
                wait();
            }

            if (error != null) {
                throw new RuntimeException(error);
            }

            res = buffer.poll();
            if (res == null) {
                throw new NoSuchElementException();
            }

            if (buffer.isEmpty() && !idleStreams.isEmpty()) {
                toRequest = new ArrayList<>(idleStreams);
                idleStreams.clear();
            }
        }
        // Outside the monitor: see onStreamIdle().
        if (toRequest != null) {
            toRequest.forEach(StreamHandle::requestNext);
        }
        return res;
    }

    @Override
    public void close() {
        List<StreamHandle> toCancel;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toCancel = new ArrayList<>(streams);
            streams.clear();
            idleStreams.clear();
            buffer.clear();
            notifyAll();
        }
        toCancel.forEach(StreamHandle::cancel);
    }
}
