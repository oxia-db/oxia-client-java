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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.oxia.client.grpc.OxiaStatusCode;
import io.oxia.client.grpc.OxiaStatusException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class ManagedObserversTest {

    @Test
    void completableFutureObserverCompletesWithLastResponse() {
        var future = new CompletableFuture<String>();
        var observer = ManagedObservers.toCompletableFuture(future);

        observer.onNext("first");
        observer.onNext("second");
        assertThat(future).isNotDone();
        observer.onCompleted();

        assertThat(future).isCompletedWithValue("second");
    }

    @Test
    void completableFutureObserverTranslatesError() {
        var future = new CompletableFuture<String>();

        ManagedObservers.toCompletableFuture(future).onError(Status.UNAVAILABLE.asRuntimeException());

        assertThat(future)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .havingCause()
                .isInstanceOfSatisfying(
                        OxiaStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(OxiaStatusCode.RESOURCE_UNAVAILABLE));
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectsNullArguments() {
        StreamObserver<String> observer = mock(StreamObserver.class);
        CancelableStreamObserver<String> cancelable = mock(CancelableStreamObserver.class);
        var future = new CompletableFuture<Void>();
        var completable = ManagedObservers.toCompletableFuture(new CompletableFuture<String>());
        List<ThrowingCallable> calls =
                List.of(
                        () -> ManagedObservers.toCompletableFuture(null),
                        () -> completable.onNext(null),
                        () -> completable.onError(null),
                        () -> ManagedObservers.toGuardedStreamObserver(null),
                        () -> ManagedObservers.toBarrierStreamObserver(null, future),
                        () -> ManagedObservers.toBarrierStreamObserver(observer, null),
                        () -> ManagedObservers.toBarrierClientResponseObserver(null, future),
                        () -> ManagedObservers.toBarrierClientResponseObserver(cancelable, null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }
}
