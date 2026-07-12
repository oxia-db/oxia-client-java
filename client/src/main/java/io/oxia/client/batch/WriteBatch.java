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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufUtil;
import io.oxia.client.grpc.ManagedWriteStream;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
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
    private final DispatchWindow window;
    private final int maxBatchSize;
    private int byteSize;
    private long bytes;
    private long startSendTimeNanos;

    WriteBatch(
            @NonNull WriteBatchFactory factory,
            @NonNull RpcProvider rpcProvider,
            @NonNull SessionManager sessionManager,
            long shardId,
            int maxBatchSize) {
        super(rpcProvider, shardId);
        this.factory = factory;
        this.sessionManager = sessionManager;
        this.window = factory.getDispatchWindow(shardId);
        this.byteSize = 0;
        this.maxBatchSize = maxBatchSize;
    }

    // ByteBufUtil.utf8Bytes() computes the UTF-8 encoded length without materializing the bytes
    int sizeOf(@NonNull Operation<?> operation) {
        if (operation instanceof Operation.WriteOperation.PutOperation p) {
            return ByteBufUtil.utf8Bytes(p.key()) + p.value().length;
        } else if (operation instanceof Operation.WriteOperation.DeleteOperation d) {
            return ByteBufUtil.utf8Bytes(d.key());
        } else if (operation instanceof Operation.WriteOperation.DeleteRangeOperation r) {
            return ByteBufUtil.utf8Bytes(r.startKeyInclusive())
                    + ByteBufUtil.utf8Bytes(r.endKeyExclusive());
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
            final ManagedWriteStream writeStream = rpcProvider.getWriteStream(getShardId());
            writeStream
                    .send(this::toProto)
                    .whenComplete(
                            (response, ex) -> {
                                // Free the window slot first, so the next batch is dispatched
                                // before the operation callbacks below run.
                                window.release();
                                if (ex != null) {
                                    handleError(ex);
                                } else {
                                    handleResponse(response);
                                }
                            });
        } catch (Throwable t) {
            window.release();
            handleError(t);
        }
    }

    private void handleResponse(WriteResponse response) {
        factory.writeRequestLatencyHistogram.recordSuccess(System.nanoTime() - startSendTimeNanos);

        for (var i = 0; i < deletes.size(); i++) {
            deletes.get(i).complete(response.getDeleteAt(i));
        }
        for (var i = 0; i < deleteRanges.size(); i++) {
            deleteRanges.get(i).complete(response.getDeleteRangeAt(i));
        }
        for (var i = 0; i < puts.size(); i++) {
            puts.get(i).complete(response.getPutAt(i));
        }
    }

    public void handleError(Throwable batchError) {
        factory.writeRequestLatencyHistogram.recordFailure(System.nanoTime() - startSendTimeNanos);
        fail(batchError);
    }

    @Override
    public void fail(@NonNull Throwable batchError) {
        deletes.forEach(d -> d.fail(batchError));
        deleteRanges.forEach(f -> f.fail(batchError));
        puts.forEach(p -> p.fail(batchError));
    }

    @NonNull
    WriteRequest toProto() {
        var req = new WriteRequest();
        req.setShard(getShardId());
        for (var p : puts) {
            p.toProto(req.addPut());
        }
        for (var d : deletes) {
            d.toProto(req.addDelete());
        }
        for (var dr : deleteRanges) {
            dr.toProto(req.addDeleteRange());
        }
        return req;
    }
}
