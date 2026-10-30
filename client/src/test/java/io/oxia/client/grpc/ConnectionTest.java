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
package io.oxia.client.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.InsecureChannelCredentials;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConnectionTest {
    private static final String ADDRESS = "localhost:6648";
    private final ClientConfig config = new OxiaClientBuilderImpl(ADDRESS).getClientConfig();
    private final CompletableFuture<Runnable> healthCheck = new CompletableFuture<>();
    private final ScheduledFuture<?> healthCheckTask = mock(ScheduledFuture.class);
    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    private final HealthCheckFailureCallback callback = mock(HealthCheckFailureCallback.class);
    private final LegacyServiceInterceptor interceptor = new LegacyServiceInterceptor();

    @Test
    void closeIsIdempotent() {
        var connection = newConnection();

        connection.close();
        connection.close();

        verify(healthCheckTask).cancel(false);
    }

    @Test
    void closeKeepsInterruptStatus() {
        var connection = newConnection();

        Thread.currentThread().interrupt();
        connection.close();

        assertThat(Thread.interrupted()).isTrue();
    }

    @Test
    void skipsHealthCheckAfterClose() throws Exception {
        var connection = newConnection();

        connection.close();
        healthCheck.get().run();

        verify(callback, never()).onFailure(any());
    }

    @Test
    void ignoresHealthCheckResultsAfterClose() {
        var connection = newConnection();

        connection.close();
        connection.onNext(
                HealthCheckResponse.newBuilder().setStatus(ServingStatus.NOT_SERVING).build());
        connection.onError(Status.UNAVAILABLE.asRuntimeException());

        verify(callback, never()).onFailure(any());
    }

    @Test
    void rejectsNullArguments() {
        List<ThrowingCallable> calls =
                List.of(
                        () -> new Connection(null, config, executor, interceptor, callback),
                        () -> new Connection(ADDRESS, null, executor, interceptor, callback),
                        () -> new Connection(ADDRESS, config, null, interceptor, callback),
                        () -> new Connection(ADDRESS, config, executor, null, callback),
                        () -> new Connection(ADDRESS, config, executor, interceptor, null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }

    private Connection newConnection() {
        when(executor.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            healthCheck.complete(invocation.getArgument(0));
                            return healthCheckTask;
                        });
        return new Connection(ADDRESS, config, executor, interceptor, callback);
    }

    @Test
    public void testAddressTrim() {
        final var tlsAddress = "tls://localhost:6648";
        Assertions.assertEquals("localhost:6648", Connection.getAddress(tlsAddress));

        final var planTxtAddress = "localhost:6648";
        Assertions.assertEquals("localhost:6648", Connection.getAddress(planTxtAddress));
    }

    @Test
    public void testTlsCredential() {
        final var tlsAddress = "tls://localhost:6648";
        var channelCredential = Connection.getChannelCredential(tlsAddress, false);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);

        channelCredential = Connection.getChannelCredential(tlsAddress, true);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);

        final var planTxtAddress = "localhost:6648";
        channelCredential = Connection.getChannelCredential(planTxtAddress, false);
        Assertions.assertInstanceOf(InsecureChannelCredentials.class, channelCredential);

        channelCredential = Connection.getChannelCredential(planTxtAddress, true);
        Assertions.assertInstanceOf(TlsChannelCredentials.class, channelCredential);
    }
}
