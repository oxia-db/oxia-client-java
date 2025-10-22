/*
 * Copyright © 2022-2025 The Oxia Authors
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
package io.oxia.client;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.RangeScanConsumer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.SneakyThrows;

public class GetResultIterator implements Iterator<GetResult>, RangeScanConsumer {

    private GetResult pendingResult;
    private Throwable error = null;
    private boolean completed = false;

    @Override
    @SneakyThrows
    public synchronized void onNext(GetResult result) {
        while (pendingResult != null) {
            wait();
        }

        pendingResult = result;
        notifyAll();
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        this.error = new Exception("Range scan error", throwable);
        notifyAll();
    }

    @Override
    public synchronized void onCompleted() {
        this.completed = true;
        notifyAll();
    }

    @Override
    @SneakyThrows
    public synchronized boolean hasNext() {
        while (error == null && !completed && pendingResult == null) {
            wait();
        }

        if (error != null) {
            throw new RuntimeException(error);
        }

        return pendingResult != null;
    }

    @Override
    @SneakyThrows
    public synchronized GetResult next() {
        while (error == null && !completed && pendingResult == null) {
            wait();
        }

        if (error != null) {
            throw new RuntimeException(error);
        }

        if (pendingResult != null) {
            GetResult res = pendingResult;
            this.pendingResult = null;
            notifyAll();
            return res;
        }

        throw new NoSuchElementException();
    }
}
