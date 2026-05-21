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
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

public final class BarrierStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> streamObserver;
    private final CompletableFuture<Void> barrierFuture;

    BarrierStreamObserver(
            @NonNull StreamObserver<T> streamObserver, @NonNull CompletableFuture<Void> barrierFuture) {
        this.streamObserver = streamObserver;
        this.barrierFuture = barrierFuture;
    }

    @Override
    public void onNext(@NonNull T response) {
        if (!barrierFuture.isDone()) {
            barrierFuture.complete(null);
        }
        streamObserver.onNext(response);
    }

    @Override
    public void onError(@NonNull Throwable error) {
        final var translated = OxiaStatusException.toException(error);
        if (!barrierFuture.isDone()) {
            barrierFuture.completeExceptionally(translated);
            return;
        }
        streamObserver.onError(translated);
    }

    @Override
    public void onCompleted() {
        if (!barrierFuture.isDone()) {
            barrierFuture.complete(null);
        }
        streamObserver.onCompleted();
    }
}
