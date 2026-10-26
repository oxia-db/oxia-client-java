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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class BarrierClientResponseObserverTest {

    @Test
    @SuppressWarnings("unchecked")
    void rejectsNullArguments() {
        CancelableStreamObserver<String> delegate = mock(CancelableStreamObserver.class);
        var barrier = new CompletableFuture<Void>();
        var observer = new BarrierClientResponseObserver<String, String>(delegate, barrier);
        List<ThrowingCallable> calls =
                List.of(
                        () -> new BarrierClientResponseObserver<>(null, barrier),
                        () -> new BarrierClientResponseObserver<>(delegate, null),
                        () -> observer.beforeStart(null),
                        () -> observer.onNext(null),
                        () -> observer.onError(null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }
}
