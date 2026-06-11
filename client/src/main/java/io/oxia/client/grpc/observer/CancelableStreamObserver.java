/*
 * Copyright © 2026 The Oxia Authors
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
package io.oxia.client.grpc.observer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;

@ThreadSafe
public abstract class CancelableStreamObserver<T> implements StreamObserver<T> {
    private static final String CANCELED = "canceled";

    private final ReentrantLock lock;
    private final boolean manualFlowControl;

    @GuardedBy("lock")
    private boolean terminated;

    @GuardedBy("lock")
    private ClientCallStreamObserver<?> requestStream;

    public CancelableStreamObserver() {
        this(false);
    }

    /**
     * @param manualFlowControl when true, the stream is started with automatic inbound flow control
     *     disabled: after one initial message, further messages are only delivered when {@link
     *     #requestNextMessage()} is invoked.
     */
    public CancelableStreamObserver(boolean manualFlowControl) {
        this.lock = new ReentrantLock();
        this.manualFlowControl = manualFlowControl;
        this.terminated = false;
        this.requestStream = null;
    }

    public void cancel() {
        final ClientCallStreamObserver<?> stream;
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            terminated = true;
            stream = requestStream;
        } finally {
            lock.unlock();
        }
        if (stream != null) {
            stream.cancel(CANCELED, null);
        }
    }

    public void cancelAndComplete() {
        final ClientCallStreamObserver<?> stream;
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            terminated = true;
            stream = requestStream;
        } finally {
            lock.unlock();
        }
        if (stream != null) {
            stream.cancel(CANCELED, null);
        }
        lock.lock();
        try {
            handleComplete();
        } finally {
            lock.unlock();
        }
    }

    void injectRequestStream(ClientCallStreamObserver<?> requestStream) {
        final ClientCallStreamObserver<?> previousStream;
        lock.lock();
        try {
            if (terminated) {
                previousStream = requestStream;
            } else {
                if (manualFlowControl) {
                    // Must happen before the call is started: injectRequestStream is invoked
                    // from ClientResponseObserver.beforeStart()
                    requestStream.disableAutoRequestWithInitial(1);
                }
                previousStream = this.requestStream;
                this.requestStream = requestStream;
            }
        } finally {
            lock.unlock();
        }

        if (previousStream != null) {
            previousStream.cancel(CANCELED, null);
        }
    }

    /** Requests the next message on a stream using manual flow control. No-op once terminated. */
    public final void requestNextMessage() {
        final ClientCallStreamObserver<?> stream;
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            stream = requestStream;
        } finally {
            lock.unlock();
        }
        if (stream != null) {
            stream.request(1);
        }
    }

    @Override
    public final void onNext(@NonNull T value) {
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            handleNext(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void onError(@NonNull Throwable t) {
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            terminated = true;
            handleError(t);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void onCompleted() {
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            terminated = true;
            handleComplete();
        } finally {
            lock.unlock();
        }
    }

    protected abstract void handleNext(T value);

    protected abstract void handleError(Throwable t);

    protected abstract void handleComplete();
}
