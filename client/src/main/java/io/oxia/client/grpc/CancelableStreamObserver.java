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
package io.oxia.client.grpc;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class CancelableStreamObserver<T> implements StreamObserver<T> {
    private final Lock lock = new ReentrantLock();
    private Runnable cancelHandler;
    private boolean canceled;

    public void cancel() {
        Runnable handler;
        lock.lock();
        try {
            if (canceled) {
                return;
            }
            canceled = true;
            if (cancelHandler == null) {
                return;
            }
            handler = cancelHandler;
        } finally {
            lock.unlock();
        }
        handler.run();
    }

    void setCancelHandler(Runnable cancelHandler) {
        boolean shouldCancel;
        lock.lock();
        try {
            this.cancelHandler = cancelHandler;
            shouldCancel = canceled;
        } finally {
            lock.unlock();
        }
        if (shouldCancel) {
            cancelHandler.run();
        }
    }
}
