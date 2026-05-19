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
import lombok.NonNull;

public final class ManagedStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> observer;

    public ManagedStreamObserver(@NonNull StreamObserver<T> observer) {
        this.observer = observer;
    }

    @Override
    public void onNext(@NonNull T value) {
        observer.onNext(value);
    }

    @Override
    public void onError(@NonNull Throwable error) {
        observer.onError(OxiaStatusException.toException(error));
    }

    @Override
    public void onCompleted() {
        observer.onCompleted();
    }
}
