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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.grpc.stub.StreamObserver;
import java.util.List;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

class GuardedStreamObserverTest {

    @Test
    @SuppressWarnings("unchecked")
    void ignoresCallbacksAfterCompletion() {
        StreamObserver<String> delegate = mock(StreamObserver.class);
        var observer = new GuardedStreamObserver<>(delegate);

        observer.onNext("value");
        observer.onCompleted();
        observer.onNext("late");
        observer.onCompleted();
        observer.onError(new RuntimeException("late"));

        verify(delegate).onNext("value");
        verify(delegate).onCompleted();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void ignoresCompletionAfterError() {
        StreamObserver<String> delegate = mock(StreamObserver.class);
        var observer = new GuardedStreamObserver<>(delegate);

        observer.onError(new RuntimeException("failed"));
        observer.onCompleted();

        verify(delegate).onError(any());
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectsNullArguments() {
        var observer = new GuardedStreamObserver<String>(mock(StreamObserver.class));
        List<ThrowingCallable> calls =
                List.of(
                        () -> new GuardedStreamObserver<>(null),
                        () -> observer.onNext(null),
                        () -> observer.onError(null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }
}
