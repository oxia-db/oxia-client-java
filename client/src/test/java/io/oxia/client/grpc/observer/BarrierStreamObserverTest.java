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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class BarrierStreamObserverTest {

    @Test
    @SuppressWarnings("unchecked")
    void completesBarrierWhenStreamCompletesWithoutResponses() {
        StreamObserver<String> delegate = mock(StreamObserver.class);
        var barrier = new CompletableFuture<Void>();

        new BarrierStreamObserver<>(delegate, barrier).onCompleted();

        assertThat(barrier).isCompleted();
        verify(delegate).onCompleted();
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectsNullArguments() {
        StreamObserver<String> delegate = mock(StreamObserver.class);
        var barrier = new CompletableFuture<Void>();
        var observer = new BarrierStreamObserver<>(delegate, barrier);
        List<ThrowingCallable> calls =
                List.of(
                        () -> new BarrierStreamObserver<>(null, barrier),
                        () -> new BarrierStreamObserver<>(delegate, null),
                        () -> observer.onNext(null),
                        () -> observer.onError(null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }
}
