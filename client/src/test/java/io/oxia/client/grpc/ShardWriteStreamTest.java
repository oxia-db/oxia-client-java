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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.stub.StreamObserver;
import io.oxia.proto.OxiaClientGrpc;
import io.oxia.proto.WriteRequest;
import io.oxia.proto.WriteResponse;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ShardWriteStreamTest {
    @Test
    void sendCompletesPendingWriteWithNextResponse() throws Exception {
        var testStream = newTestStream();
        var request = new WriteRequest().setShard(1);
        var response = new WriteResponse();

        var future = testStream.stream().send(request);
        testStream.responseStream().get().onNext(response);

        assertThat(future.get(5, TimeUnit.SECONDS)).isSameAs(response);
        verify(testStream.requestStream()).onNext(request);
    }

    @Test
    void onErrorFailsPendingWritesAndRejectsNewWrites() throws Exception {
        var testStream = newTestStream();
        var request = new WriteRequest().setShard(1);
        var pendingWrite = testStream.stream().send(request);
        var error = new RuntimeException("write stream failed");

        testStream.responseStream().get().onError(error);

        assertThat(failure(pendingWrite)).isSameAs(error);
        assertThat(testStream.stream().isValid()).isFalse();
        assertThat(failure(testStream.stream().send(new WriteRequest().setShard(1)))).isSameAs(error);
        verify(testStream.requestStream(), times(1)).onNext(any(WriteRequest.class));
    }

    @Test
    void closeCancelsPendingWritesAndClosesRequestStream() throws Exception {
        var testStream = newTestStream();
        var pendingWrite = testStream.stream().send(new WriteRequest().setShard(1));

        testStream.stream().close();

        assertThat(failure(pendingWrite)).isInstanceOf(CancellationException.class);
        verify(testStream.requestStream()).onCompleted();
    }

    private static Throwable failure(CompletableFuture<?> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            throw new AssertionError("Expected future to fail");
        } catch (CancellationException error) {
            return error;
        } catch (ExecutionException error) {
            return error.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private static TestStream newTestStream() {
        var stub = mock(OxiaClientGrpc.OxiaClientStub.class);
        StreamObserver<WriteRequest> requestStream = mock(StreamObserver.class);
        var responseStream = new AtomicReference<StreamObserver<WriteResponse>>();
        when(stub.writeStream(any()))
                .thenAnswer(
                        invocation -> {
                            responseStream.set(invocation.getArgument(0));
                            return requestStream;
                        });
        return new TestStream(new ShardWriteStream(stub, 1), requestStream, responseStream);
    }

    private record TestStream(
            ShardWriteStream stream,
            StreamObserver<WriteRequest> requestStream,
            AtomicReference<StreamObserver<WriteResponse>> responseStream) {}
}
