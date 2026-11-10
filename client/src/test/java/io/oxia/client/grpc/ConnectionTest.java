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
import io.grpc.stub.StreamObserver;
import io.netty.channel.ConnectTimeoutException;
import io.oxia.client.ClientConfig;
import io.oxia.client.OxiaClientBuilderImpl;
import io.oxia.proto.ShardAssignments;
import io.oxia.proto.ShardAssignmentsRequest;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
                        () -> new Connection(null, config, executor, callback),
                        () -> new Connection(ADDRESS, null, executor, callback),
                        () -> new Connection(ADDRESS, config, null, callback),
                        () -> new Connection(ADDRESS, config, executor, null));

        calls.forEach(call -> assertThatThrownBy(call).isInstanceOf(NullPointerException.class));
    }

    @Test
    void connectionAttemptToHangingAddressFailsAfterConnectTimeout() throws Exception {
        // On Linux, grpc-java sets TCP_USER_TIMEOUT to the keepalive timeout (3 s by default), which
        // would race the connect timeout: move it out of the time the test waits.
        var builder = new OxiaClientBuilderImpl(ADDRESS);
        builder.connectionKeepAliveTimeout(Duration.ofSeconds(30));
        var longKeepAliveConfig = builder.getClientConfig();
        // A listener whose accept queue is full drops the new connection attempts, like the address of
        // a deleted pod: nobody refuses or answers them.
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            var filler = new ArrayList<Socket>();
            try {
                for (int i = 0; i < 4; i++) {
                    var socket = new Socket();
                    try {
                        socket.connect(server.getLocalSocketAddress(), 500);
                    } catch (java.net.SocketTimeoutException expected) {
                        // the queue is full
                    }
                    filler.add(socket);
                }
                when(executor.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any()))
                        .thenReturn((ScheduledFuture) healthCheckTask);
                var failed = new CompletableFuture<Status>();
                var connection =
                        new Connection(
                                "127.0.0.1:" + server.getLocalPort(), longKeepAliveConfig, executor, callback);
                var start = System.nanoTime();
                connection
                        .stub()
                        .withDeadlineAfter(15, TimeUnit.SECONDS)
                        .getShardAssignments(
                                new ShardAssignmentsRequest(),
                                new StreamObserver<ShardAssignments>() {
                                    @Override
                                    public void onNext(ShardAssignments value) {}

                                    @Override
                                    public void onError(Throwable t) {
                                        failed.complete(Status.fromThrowable(t));
                                    }

                                    @Override
                                    public void onCompleted() {}
                                });

                var status = failed.get(12, TimeUnit.SECONDS);
                var elapsed = Duration.ofNanos(System.nanoTime() - start);
                connection.close();

                assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                // Netty's connect timeout, not one of the operating system's (30 s or more here)
                assertThat(status.getCause()).isInstanceOf(ConnectTimeoutException.class);
                assertThat(elapsed).isLessThan(Duration.ofSeconds(8));
            } finally {
                for (var socket : filler) {
                    socket.close();
                }
            }
        }
    }

    private Connection newConnection() {
        when(executor.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any()))
                .thenAnswer(
                        invocation -> {
                            healthCheck.complete(invocation.getArgument(0));
                            return healthCheckTask;
                        });
        return new Connection(ADDRESS, config, executor, callback);
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
