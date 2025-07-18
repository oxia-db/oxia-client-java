/*
 * Copyright © 2022-2025 StreamNative Inc.
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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class CompositeConsumer<T> implements Consumer<T> {

    final Set<Consumer<T>> callbacks = ConcurrentHashMap.newKeySet();

    public void add(Consumer<T> callback) {
        callbacks.add(callback);
    }

    @Override
    public void accept(T t) {
        callbacks.parallelStream().forEach(c -> c.accept(t));
    }
}
