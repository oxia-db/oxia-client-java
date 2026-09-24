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
package io.oxia.client.batch;

import static io.oxia.client.OxiaClientBuilderImpl.DefaultNamespace;
import static io.oxia.proto.Status.SESSION_DOES_NOT_EXIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.oxia.client.ClientConfig;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.SessionDoesNotExistException;
import io.oxia.client.grpc.ManagedWriteStream;
import io.oxia.client.grpc.RpcProvider;
import io.oxia.client.metrics.InstrumentProvider;
import io.oxia.client.session.SessionManager;
import io.oxia.proto.CreateSessionRequest;
import io.oxia.proto.CreateSessionResponse;
import io.oxia.proto.KeepAliveResponse;
import io.oxia.proto.SessionHeartbeat;
import io.oxia.proto.WriteResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * End-to-end coverage of the write-path session recovery: a batch whose ephemeral put is rejected
 * with SESSION_DOES_NOT_EXIST must judge the session it carried dead (exactly once) so the next
 * ephemeral operation re-establishes a fresh session.
 */
@ExtendWith(MockitoExtension.class)
class WriteBatchSessionInvalidationTest {

    private static final long SHARD_ID = 1L;

    @Mock RpcProvider rpcProvider;
    @Mock ManagedWriteStream writeStream;

    ScheduledExecutorService executor;
    SessionManager sessionManager;

    @BeforeEach
    void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        var config =
                new ClientConfig(
                        "address",
                        Duration.ofSeconds(1),
                        10,
                        1024 * 1024,
                        256L * 1024 * 1024,
                        4,
                        4,
                        1,
                        Duration.ofSeconds(10),
                        "client",
                        null,
                        DefaultNamespace,
                        null,
                        false,
                        Duration.ofMillis(10),
                        Duration.ofMillis(100),
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(3),
                        1);
        sessionManager = new SessionManager(executor, config, rpcProvider, InstrumentProvider.NOOP);
        lenient()
                .when(rpcProvider.keepAlive(any(SessionHeartbeat.class), any(Duration.class)))
                .thenReturn(CompletableFuture.completedFuture(new KeepAliveResponse()));
    }

    @AfterEach
    void cleanup() {
        executor.shutdownNow();
    }

    @Test
    void rejectedEphemeralPutInvalidatesSessionAndNextOperationRecreates() {
        // Server session ids are monotonic per term: after a restart the id counter can be
        // reused once, then every recreated session moves past the rejected id.
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(sessionResponse(14L), sessionResponse(14L), sessionResponse(15L));
        when(rpcProvider.getWriteStream(SHARD_ID)).thenReturn(writeStream);
        when(writeStream.send(any()))
                .thenReturn(CompletableFuture.completedFuture(rejectedPutsResponse(1)));

        var expired = sessionManager.getSession(SHARD_ID).join();

        // Incident preamble: the session expires locally (e.g. during a server outage) —
        // abandoned without a CloseSession RPC — and the server hands out the same id again.
        sessionManager.onSessionExpired(expired);
        assertThat(sessionManager.getSession(SHARD_ID).join().getSessionId()).isEqualTo(14L);

        // The late write for the old session is rejected: the same-numbered recreated
        // session is judged dead, exactly once, through the write path.
        var putCallback = new CompletableFuture<PutResult>();
        batchWith(ephemeralPut(14L, putCallback)).send();

        // The rejected operation keeps its failure semantics for the caller.
        assertThatThrownBy(putCallback::get)
                .hasCauseExactlyInstanceOf(SessionDoesNotExistException.class);

        // The next ephemeral operation converges on a fresh session, and no close RPC was
        // ever sent for either 14.
        assertThat(sessionManager.getSession(SHARD_ID).join().getSessionId()).isEqualTo(15L);
        verify(rpcProvider, times(3)).createSession(any(CreateSessionRequest.class));
        verify(rpcProvider, never()).closeSession(any());
    }

    @Test
    void multipleRejectedEphemeralPutsInvalidateExactlyOnce() {
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(sessionResponse(14L), sessionResponse(15L));
        when(rpcProvider.getWriteStream(SHARD_ID)).thenReturn(writeStream);
        when(writeStream.send(any()))
                .thenReturn(CompletableFuture.completedFuture(rejectedPutsResponse(3)));
        sessionManager.getSession(SHARD_ID).join();

        var batch =
                batchWith(
                        ephemeralPut(14L, new CompletableFuture<>()),
                        ephemeralPut(14L, new CompletableFuture<>()),
                        ephemeralPut(14L, new CompletableFuture<>()));
        batch.send();

        // Several in-flight operations of the same session are rejected together: the
        // compute() guard still yields a single verdict, so the session is recreated once.
        assertThat(sessionManager.getSession(SHARD_ID).join().getSessionId()).isEqualTo(15L);
        verify(rpcProvider, times(2)).createSession(any(CreateSessionRequest.class));
    }

    @Test
    void rejectedPutForForeignSessionDoesNotInvalidateLiveSession() {
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(sessionResponse(14L), sessionResponse(15L));
        when(rpcProvider.getWriteStream(SHARD_ID)).thenReturn(writeStream);
        when(writeStream.send(any()))
                .thenReturn(CompletableFuture.completedFuture(rejectedPutsResponse(2)));
        sessionManager.getSession(SHARD_ID).join();

        // A batch may carry puts of different sessions: only the session this manager
        // actually holds under the rejected id is invalidated.
        batchWith(
                        ephemeralPut(27L, new CompletableFuture<>()),
                        ephemeralPut(14L, new CompletableFuture<>()))
                .send();
        assertThat(sessionManager.getSession(SHARD_ID).join().getSessionId()).isEqualTo(15L);
    }

    @Test
    void sessionlessOperationsNeverInvalidateTheSession() {
        when(rpcProvider.createSession(any(CreateSessionRequest.class)))
                .thenReturn(sessionResponse(14L));
        when(rpcProvider.getWriteStream(SHARD_ID)).thenReturn(writeStream);
        var response = new WriteResponse();
        response.addPut().setStatus(SESSION_DOES_NOT_EXIST);
        response.addDelete().setStatus(SESSION_DOES_NOT_EXIST);
        when(writeStream.send(any())).thenReturn(CompletableFuture.completedFuture(response));
        sessionManager.getSession(SHARD_ID).join();

        var putCallback = new CompletableFuture<PutResult>();
        var deleteCallback = new CompletableFuture<Boolean>();
        batchWith(
                        plainPut(putCallback),
                        new Operation.WriteOperation.DeleteOperation(SHARD_ID, deleteCallback, "plain-key"))
                .send();

        // Even a SESSION_DOES_NOT_EXIST status on ops that carry no session (a plain put
        // overwriting an ephemeral key, or any delete) is only an operation failure.
        assertThatThrownBy(putCallback::get)
                .hasCauseExactlyInstanceOf(SessionDoesNotExistException.class);
        assertThat(deleteCallback).isCompletedExceptionally();
        assertThat(sessionManager.getSession(SHARD_ID).join().getSessionId()).isEqualTo(14L);
        verify(rpcProvider, times(1)).createSession(any(CreateSessionRequest.class));
    }

    private static CompletableFuture<CreateSessionResponse> sessionResponse(long sessionId) {
        return CompletableFuture.completedFuture(new CreateSessionResponse().setSessionId(sessionId));
    }

    private static WriteResponse rejectedPutsResponse(int count) {
        var response = new WriteResponse();
        for (var i = 0; i < count; i++) {
            response.addPut().setStatus(SESSION_DOES_NOT_EXIST);
        }
        return response;
    }

    private WriteBatch batchWith(Operation<?>... operations) {
        var factory =
                new WriteBatchFactory(rpcProvider, sessionManager, config(), InstrumentProvider.NOOP);
        var batch = new WriteBatch(factory, rpcProvider, sessionManager, SHARD_ID, 1024 * 1024);
        for (var operation : operations) {
            batch.add(operation);
        }
        return batch;
    }

    private ClientConfig config() {
        return new ClientConfig(
                "address",
                Duration.ofSeconds(1),
                10,
                1024 * 1024,
                256L * 1024 * 1024,
                4,
                4,
                1,
                Duration.ofSeconds(10),
                "client",
                null,
                DefaultNamespace,
                null,
                false,
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                Duration.ofSeconds(10),
                Duration.ofSeconds(3),
                1);
    }

    private static Operation.WriteOperation.PutOperation ephemeralPut(
            long sessionId, CompletableFuture<PutResult> callback) {
        return new Operation.WriteOperation.PutOperation(
                SHARD_ID,
                callback,
                "ephemeral-key",
                Optional.empty(),
                Optional.empty(),
                new byte[0],
                OptionalLong.empty(),
                OptionalLong.of(sessionId),
                Optional.of("client"),
                Collections.emptyList(),
                OptionalLong.empty(),
                OptionalLong.empty());
    }

    private static Operation.WriteOperation.PutOperation plainPut(
            CompletableFuture<PutResult> callback) {
        return new Operation.WriteOperation.PutOperation(
                SHARD_ID,
                callback,
                "plain-key",
                Optional.empty(),
                Optional.empty(),
                new byte[0],
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Collections.emptyList(),
                OptionalLong.empty(),
                OptionalLong.empty());
    }
}
