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
package io.oxia.client.batch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import io.oxia.client.grpc.OxiaStubProvider;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.WriteRequest;
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;

final class WriteBatch extends BatchBase implements Batch {

    private final WriteBatchFactory factory;

    @VisibleForTesting final List<Operation.WriteOperation.PutOperation> puts = new ArrayList<>();

    @VisibleForTesting
    final List<Operation.WriteOperation.DeleteOperation> deletes = new ArrayList<>();

    @VisibleForTesting
    final List<Operation.WriteOperation.DeleteRangeOperation> deleteRanges = new ArrayList<>();

    private final SessionManager sessionManager;
    private final int maxBatchSize;
    private int byteSize;
    private long bytes;
    private long startSendTimeNanos;

    WriteBatch(
            @NonNull WriteBatchFactory factory,
            @NonNull OxiaStubProvider stubProvider,
            @NonNull SessionManager sessionManager,
            long shardId,
            int maxBatchSize) {
        super(stubProvider, shardId);
        this.factory = factory;
        this.sessionManager = sessionManager;
        this.byteSize = 0;
        this.maxBatchSize = maxBatchSize;
    }

    int sizeOf(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.WriteOperation.PutOperation p) {
            return p.key().getBytes(UTF_8).length + p.value().length;
        } else if (operation instanceof Operation.WriteOperation.DeleteOperation d) {
            return d.key().getBytes(UTF_8).length;
        } else if (operation instanceof Operation.WriteOperation.DeleteRangeOperation r) {
            return r.startKeyInclusive().getBytes(UTF_8).length
                    + r.endKeyExclusive().getBytes(UTF_8).length;
        }
        return 0;
    }

    public void add(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.WriteOperation.PutOperation p) {
            puts.add(p);
            bytes += p.value().length;
        } else if (operation instanceof Operation.WriteOperation.DeleteOperation d) {
            deletes.add(d);
        } else if (operation instanceof Operation.WriteOperation.DeleteRangeOperation r) {
            deleteRanges.add(r);
        }
        byteSize += sizeOf(operation);
    }

    @Override
    public boolean canAdd(@NonNull Operation<?> operation) {
        int size = sizeOf(operation);
        return byteSize + size <= maxBatchSize;
    }

    @Override
    public int size() {
        return puts.size() + deletes.size() + deleteRanges.size();
    }

    @Override
    public void send() {
        startSendTimeNanos = System.nanoTime();
        try {
            getWriteStream()
                    .send(toProto())
                    .thenAccept(
                            response -> {
                                factory.writeRequestLatencyHistogram.recordSuccess(
                                        System.nanoTime() - startSendTimeNanos);

                                for (var i = 0; i < deletes.size(); i++) {
                                    deletes.get(i).complete(response.getDeletes(i));
                                }
                                for (var i = 0; i < deleteRanges.size(); i++) {
                                    deleteRanges.get(i).complete(response.getDeleteRanges(i));
                                }
                                for (var i = 0; i < puts.size(); i++) {
                                    puts.get(i).complete(response.getPuts(i));
                                }
                            })
                    .exceptionally(
                            ex -> {
                                handleError(ex);
                                return null;
                            });
        } catch (Throwable t) {
            handleError(t);
        }
    }

    public void handleError(Throwable batchError) {
        factory.writeRequestLatencyHistogram.recordFailure(System.nanoTime() - startSendTimeNanos);
        deletes.forEach(d -> d.fail(batchError));
        deleteRanges.forEach(f -> f.fail(batchError));
        puts.forEach(p -> p.fail(batchError));
    }

    @NonNull
    WriteRequest toProto() {
        return WriteRequest.newBuilder()
                .setShard(getShardId())
                .addAllPuts(
                        puts.stream().map(Operation.WriteOperation.PutOperation::toProto).collect(toList()))
                .addAllDeletes(
                        deletes.stream()
                                .map(Operation.WriteOperation.DeleteOperation::toProto)
                                .collect(toList()))
                .addAllDeleteRanges(
                        deleteRanges.stream()
                                .map(Operation.WriteOperation.DeleteRangeOperation::toProto)
                                .collect(toList()))
                .build();
    }
}
