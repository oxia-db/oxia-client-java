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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;

public abstract class CancelableStreamObserver<T> implements StreamObserver<T> {
    private static final String CANCELED = "canceled";

    private final AtomicBoolean canceled = new AtomicBoolean();
    private final AtomicReference<ClientCallStreamObserver<?>> requestStream =
            new AtomicReference<>();

    public void cancel() {
        if (!canceled.compareAndSet(false, true)) {
            return;
        }
        final var stream = requestStream.getAndSet(null);
        if (stream != null) {
            stream.cancel(CANCELED, null);
        }
    }

    protected boolean isCanceled() {
        return canceled.get();
    }

    void setRequestStream(ClientCallStreamObserver<?> requestStream) {
        if (!this.requestStream.compareAndSet(null, requestStream)) {
            throw new IllegalStateException("Request stream was already initialized");
        }
        if (canceled.get() && this.requestStream.compareAndSet(requestStream, null)) {
            requestStream.cancel(CANCELED, null);
        }
    }

    @Override
    public final void onNext(@NonNull T value) {
        if (!isCanceled()) {
            onNextValue(value);
        }
    }

    @Override
    public final void onError(@NonNull Throwable t) {
        if (!isCanceled()) {
            onErrorValue(t);
        }
    }

    @Override
    public final void onCompleted() {
        if (!isCanceled()) {
            onCompletedValue();
        }
    }

    protected abstract void onNextValue(@NonNull T value);

    protected abstract void onErrorValue(@NonNull Throwable t);

    protected abstract void onCompletedValue();
}
