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
package io.oxia.client.grpc;

import io.grpc.stub.StreamObserver;
import java.util.function.BiConsumer;

public abstract class CancelableStreamObserver<T> implements StreamObserver<T> {
    private final Object lock = new Object();
    private BiConsumer<String, Throwable> cancelHandler;
    private boolean canceled;
    private CancelRequest cancelRequest;

    public void cancel(String message, Throwable cause) {
        BiConsumer<String, Throwable> handler;
        synchronized (lock) {
            canceled = true;
            cancelRequest = new CancelRequest(message, cause);
            if (cancelHandler == null) {
                return;
            }
            handler = cancelHandler;
        }
        handler.accept(message, cause);
    }

    void setCancelHandler(BiConsumer<String, Throwable> cancelHandler) {
        CancelRequest cancel;
        synchronized (lock) {
            this.cancelHandler = cancelHandler;
            cancel = canceled ? cancelRequest : null;
        }
        if (cancel != null) {
            cancelHandler.accept(cancel.message(), cancel.cause());
        }
    }

    private record CancelRequest(String message, Throwable cause) {}
}
