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

    @GuardedBy("lock")
    private boolean terminated;

    @GuardedBy("lock")
    private ClientCallStreamObserver<?> requestStream;

    public CancelableStreamObserver() {
        this.lock = new ReentrantLock();
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
