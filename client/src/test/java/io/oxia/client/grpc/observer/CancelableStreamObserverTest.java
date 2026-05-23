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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.grpc.stub.ClientCallStreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CancelableStreamObserverTest {

    @Test
    void ignoresCallbacksAfterTerminalSignal() {
        var observer = new RecordingStreamObserver();

        observer.onCompleted();
        observer.onCompleted();
        observer.onError(new RuntimeException("late"));
        observer.onNext("late");

        assertThat(observer.nextCount).hasValue(0);
        assertThat(observer.errorCount).hasValue(0);
        assertThat(observer.completedCount).hasValue(1);
    }

    @Test
    void cancelSuppressesCallbacks() {
        var observer = new RecordingStreamObserver();
        @SuppressWarnings("unchecked")
        ClientCallStreamObserver<String> requestStream = mock(ClientCallStreamObserver.class);
        observer.setRequestStream(requestStream);

        observer.cancel();
        observer.onNext("late");
        observer.onError(new RuntimeException("late"));
        observer.onCompleted();

        verify(requestStream).cancel(eq("canceled"), isNull());
        assertThat(observer.nextCount).hasValue(0);
        assertThat(observer.errorCount).hasValue(0);
        assertThat(observer.completedCount).hasValue(0);
    }

    @Test
    void cancelAfterCompletedDoesNotCancelCompletedRequestStream() {
        var observer = new RecordingStreamObserver();
        @SuppressWarnings("unchecked")
        ClientCallStreamObserver<String> requestStream = mock(ClientCallStreamObserver.class);
        observer.setRequestStream(requestStream);

        observer.onCompleted();
        observer.cancel();

        verify(requestStream, never()).cancel(eq("canceled"), isNull());
        assertThat(observer.completedCount).hasValue(1);
    }

    @Test
    void setRequestStreamAfterCancelCancelsImmediately() {
        var observer = new RecordingStreamObserver();
        @SuppressWarnings("unchecked")
        ClientCallStreamObserver<String> requestStream = mock(ClientCallStreamObserver.class);

        observer.cancel();
        observer.setRequestStream(requestStream);

        verify(requestStream).cancel(eq("canceled"), isNull());
    }

    private static final class RecordingStreamObserver extends CancelableStreamObserver<String> {
        private final AtomicInteger nextCount = new AtomicInteger();
        private final AtomicInteger errorCount = new AtomicInteger();
        private final AtomicInteger completedCount = new AtomicInteger();

        @Override
        protected void onNextValue(String value) {
            nextCount.incrementAndGet();
        }

        @Override
        protected void onErrorValue(Throwable t) {
            errorCount.incrementAndGet();
        }

        @Override
        protected void onCompletedValue() {
            completedCount.incrementAndGet();
        }
    }
}
