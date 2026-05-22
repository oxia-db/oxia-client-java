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
package io.oxia.client.operation.rangescan;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import java.util.ArrayList;
import java.util.List;

public class CompositeRangeScanConsumer implements RangeScanConsumer {
    private final Object callbackLock = new Object();
    private final RangeScanConsumer delegate;
    private final List<Runnable> cancelHandlers = new ArrayList<>();

    private int pendingCompletedRequests;
    private boolean completed = false;
    private Throwable completedException = null;

    public CompositeRangeScanConsumer(int shards, RangeScanConsumer delegate) {
        this.pendingCompletedRequests = shards;
        this.delegate = delegate;
    }

    public void registerCancelHandler(Runnable handler) {
        synchronized (this) {
            if (!completed) {
                cancelHandlers.add(handler);
                return;
            }
        }
        handler.run();
    }

    @Override
    public boolean onNext(GetResult result) {
        synchronized (callbackLock) {
            synchronized (this) {
                if (completed) {
                    return false;
                }
            }
            if (delegate.onNext(result)) {
                return true;
            }

            final List<Runnable> handlersToCancel = completeAndTakeCancelHandlers();
            if (handlersToCancel != null) {
                cancelHandlers(handlersToCancel);
                delegate.onCompleted();
            }
            return false;
        }
    }

    private synchronized List<Runnable> completeAndTakeCancelHandlers() {
        if (completed) {
            return null;
        }
        completed = true;
        var handlersToCancel = new ArrayList<>(cancelHandlers);
        cancelHandlers.clear();
        return handlersToCancel;
    }

    private void cancelHandlers(List<Runnable> handlersToCancel) {
        for (Runnable h : handlersToCancel) {
            try {
                h.run();
            } catch (Throwable ignored) {
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        final boolean notifyDelegate;
        synchronized (this) {
            if (completedException == null) {
                completedException = throwable;
            } else {
                completedException.addSuppressed(throwable);
            }
            if (completed) {
                return;
            }
            completed = true;
            notifyDelegate = true;
        }
        if (notifyDelegate) {
            synchronized (callbackLock) {
                delegate.onError(throwable);
            }
        }
    }

    @Override
    public void onCompleted() {
        final boolean notifyDelegate;
        synchronized (this) {
            if (completed) {
                return;
            }
            pendingCompletedRequests -= 1;
            notifyDelegate = pendingCompletedRequests == 0;
            if (notifyDelegate) {
                completed = true;
            }
        }
        if (notifyDelegate) {
            synchronized (callbackLock) {
                delegate.onCompleted();
            }
        }
    }
}
