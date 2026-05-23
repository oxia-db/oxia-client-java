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
import java.util.concurrent.locks.ReentrantLock;

public class CompositeRangeScanConsumer implements RangeScanConsumer {
    private final RangeScanConsumer delegate;
    private final ReentrantLock lock;
    private int pendingCompletedRequests;
    private boolean completed;

    public CompositeRangeScanConsumer(int shards, RangeScanConsumer delegate) {
        this.pendingCompletedRequests = shards;
        this.delegate = delegate;
        this.completed = false;
        this.lock = new ReentrantLock();
    }

    @Override
    public boolean onNext(GetResult result) {
        lock.lock();
        try {
            if (completed) {
                return false; // ignore the dirty data
            }
            final boolean wantNext = delegate.onNext(result);
            if (!wantNext) {
                completed = true;
                delegate.onCompleted();
            }
            return wantNext;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        lock.lock();
        try {
            if (completed) {
                return;
            }
            delegate.onError(throwable);
            completed = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onCompleted() {
        lock.lock();
        try {
            if (completed) {
                return;
            }
            pendingCompletedRequests -= 1;
            if (pendingCompletedRequests == 0) {
                delegate.onCompleted();
                completed = true;
            }
        } finally {
            lock.unlock();
        }
    }
}
