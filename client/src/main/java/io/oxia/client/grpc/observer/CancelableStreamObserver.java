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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class CancelableStreamObserver<T> implements StreamObserver<T> {
    private static final String CANCELED = "canceled";

    private final Lock lock = new ReentrantLock();
    private ClientCallStreamObserver<?> requestStream;
    private boolean canceled;

    public void cancel() {
        ClientCallStreamObserver<?> stream;
        lock.lock();
        try {
            if (canceled) {
                return;
            }
            canceled = true;
            stream = requestStream;
        } finally {
            lock.unlock();
        }
        if (stream != null) {
            stream.cancel(CANCELED, null);
        }
    }

    void setRequestStream(ClientCallStreamObserver<?> requestStream) {
        boolean shouldCancel;
        lock.lock();
        try {
            this.requestStream = requestStream;
            shouldCancel = canceled;
        } finally {
            lock.unlock();
        }
        if (shouldCancel) {
            requestStream.cancel(CANCELED, null);
        }
    }
}
