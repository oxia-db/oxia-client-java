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

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.Notification;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.options.RangeScanOption;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SyncOxiaClientImpl implements SyncOxiaClient {
    private final AsyncOxiaClient asyncClient;

    @Override
    public PutResult put(@NonNull String key, byte @NonNull [] value) {
        return put(key, value, Collections.emptySet());
    }

    @SneakyThrows
    @Override
    public @NonNull PutResult put(
            @NonNull String key, byte @NonNull [] value, @NonNull Set<PutOption> options) {
        try {
            return asyncClient.put(key, value, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public boolean delete(@NonNull String key) {
        return delete(key, Collections.emptySet());
    }

    @SneakyThrows
    @Override
    public boolean delete(@NonNull String key, @NonNull Set<DeleteOption> options)
            throws UnexpectedVersionIdException {
        try {
            return asyncClient.delete(key, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public void deleteRange(@NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        deleteRange(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @SneakyThrows
    @Override
    public void deleteRange(
            @NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive,
            @NonNull Set<DeleteRangeOption> options) {
        try {
            asyncClient.deleteRange(startKeyInclusive, endKeyExclusive, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public GetResult get(@NonNull String key) {
        return get(key, Collections.emptySet());
    }

    @SneakyThrows
    @Override
    public GetResult get(@NonNull String key, @NonNull Set<GetOption> options) {
        try {
            return asyncClient.get(key, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @SneakyThrows
    @Override
    public @NonNull List<String> list(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        return list(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @SneakyThrows
    @Override
    public @NonNull List<String> list(
            @NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive,
            @NonNull Set<ListOption> options) {
        try {
            return asyncClient.list(startKeyInclusive, endKeyExclusive, options).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        asyncClient.notifications(notificationCallback);
    }

    @Override
    public Closeable getSequenceUpdates(
            @NonNull String key,
            @NonNull Consumer<String> listener,
            @NonNull Set<GetSequenceUpdatesOption> options) {
        return asyncClient.getSequenceUpdates(key, listener, options);
    }

    @Override
    public Iterable<GetResult> rangeScan(
            @NonNull String startKeyInclusive, @NonNull String endKeyExclusive) {
        return rangeScan(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public Iterable<GetResult> rangeScan(
            @NonNull String startKeyInclusive,
            @NonNull String endKeyExclusive,
            Set<RangeScanOption> options) {
        return () -> {
            GetResultIterator gri = new GetResultIterator();
            asyncClient.rangeScan(startKeyInclusive, endKeyExclusive, gri, options);
            return gri;
        };
    }

    @Override
    public void close() throws Exception {
        asyncClient.close();
    }
}
