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

import io.grpc.stub.StreamObserver;
import io.oxia.client.grpc.OxiaStatusException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;

public final class GuardedStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> streamObserver;
    private final AtomicBoolean terminated = new AtomicBoolean();

    GuardedStreamObserver(@NonNull StreamObserver<T> streamObserver) {
        this.streamObserver = streamObserver;
    }

    @Override
    public void onNext(@NonNull T response) {
        streamObserver.onNext(response);
    }

    @Override
    public void onError(@NonNull Throwable error) {
        if (terminated.compareAndSet(false, true)) {
            streamObserver.onError(OxiaStatusException.toException(error));
        }
    }

    @Override
    public void onCompleted() {
        if (terminated.compareAndSet(false, true)) {
            streamObserver.onCompleted();
        }
    }
}
