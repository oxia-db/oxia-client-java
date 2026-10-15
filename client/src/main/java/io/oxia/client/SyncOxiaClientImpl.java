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

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.OxiaClientException;
import io.oxia.client.api.exceptions.OxiaException;
import io.oxia.client.api.exceptions.OxiaUncheckedException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.DeleteRangeOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SyncOxiaClientImpl implements SyncOxiaClient {
    private final AsyncOxiaClient asyncClient;

    @Override
    public PutResult put(String key, byte[] value) {
        try {
            return put(key, value, Collections.emptySet());
        } catch (UnexpectedVersionIdException | KeyAlreadyExistsException e) {
            // Only a put with options can fail with them
            throw new OxiaUncheckedException(e);
        }
    }

    @Override
    public @NonNull PutResult put(String key, byte[] value, @NonNull Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        try {
            return awaitDeclared(asyncClient.put(key, value, options));
        } catch (UnexpectedVersionIdException | KeyAlreadyExistsException e) {
            throw e;
        } catch (OxiaException e) {
            throw new OxiaUncheckedException(e);
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            return delete(key, Collections.emptySet());
        } catch (UnexpectedVersionIdException e) {
            // Only a delete with options can fail with it
            throw new OxiaUncheckedException(e);
        }
    }

    @Override
    public boolean delete(String key, @NonNull Set<DeleteOption> options)
            throws UnexpectedVersionIdException {
        try {
            return awaitDeclared(asyncClient.delete(key, options));
        } catch (UnexpectedVersionIdException e) {
            throw e;
        } catch (OxiaException e) {
            throw new OxiaUncheckedException(e);
        }
    }

    @Override
    public void deleteRange(String startKeyInclusive, String endKeyExclusive) {
        deleteRange(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public void deleteRange(
            String startKeyInclusive, String endKeyExclusive, @NonNull Set<DeleteRangeOption> options) {
        await(asyncClient.deleteRange(startKeyInclusive, endKeyExclusive, options));
    }

    @Override
    public GetResult get(String key) {
        return get(key, Collections.emptySet());
    }

    @Override
    public GetResult get(String key, @NonNull Set<GetOption> options) {
        return await(asyncClient.get(key, options));
    }

    @Override
    public @NonNull List<String> list(String startKeyInclusive, String endKeyExclusive) {
        return list(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public @NonNull List<String> list(
            String startKeyInclusive, String endKeyExclusive, @NonNull Set<ListOption> options) {
        return await(asyncClient.list(startKeyInclusive, endKeyExclusive, options));
    }

    @Override
    public void notifications(@NonNull Consumer<Notification> notificationCallback) {
        asyncClient.notifications(notificationCallback);
    }

    @Override
    public Closeable getSequenceUpdates(
            String key,
            @NonNull Consumer<String> listener,
            @NonNull Set<GetSequenceUpdatesOption> options) {
        return asyncClient.getSequenceUpdates(key, listener, options);
    }

    @Override
    public CloseableIterable<GetResult> rangeScan(String startKeyInclusive, String endKeyExclusive) {
        return rangeScan(startKeyInclusive, endKeyExclusive, Collections.emptySet());
    }

    @Override
    public CloseableIterable<GetResult> rangeScan(
            String startKeyInclusive, String endKeyExclusive, Set<RangeScanOption> options) {
        return new CloseableIterable<>() {
            private final List<GetResultIterator> active = new ArrayList<>();
            private boolean closed = false;

            @Override
            public synchronized Iterator<GetResult> iterator() {
                if (closed) {
                    throw new IllegalStateException("Range scan iterable is closed");
                }
                GetResultIterator gri = new GetResultIterator();
                active.add(gri);
                asyncClient.rangeScan(startKeyInclusive, endKeyExclusive, gri, options);
                return gri;
            }

            @Override
            public synchronized void close() {
                if (closed) {
                    return;
                }
                closed = true;
                for (GetResultIterator gri : active) {
                    gri.close();
                }
                active.clear();
            }
        };
    }

    @Override
    public void close() throws Exception {
        asyncClient.close();
    }

    /**
     * Waits for an operation that declares no {@link OxiaException}: its {@link OxiaException} is
     * thrown in an {@link OxiaUncheckedException}.
     */
    private static <T> T await(CompletableFuture<T> future) {
        try {
            return awaitDeclared(future);
        } catch (OxiaException e) {
            throw new OxiaUncheckedException(e);
        }
    }

    /** Waits for an operation, and throws its {@link OxiaException} as is. */
    private static <T> T awaitDeclared(CompletableFuture<T> future) throws OxiaException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OxiaClientException("Interrupted while waiting for the operation", e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof OxiaException oxiaException) {
                throw oxiaException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new OxiaClientException("Unexpected error: " + cause, cause);
        }
    }
}
