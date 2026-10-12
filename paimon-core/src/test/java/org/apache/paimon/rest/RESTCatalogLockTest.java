/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.rest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.responses.CommitLockResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests renewal, expiry and scoped execution of REST catalog locks. */
class RESTCatalogLockTest {
    private final RESTApi api = mock(RESTApi.class);
    private final ScheduledExecutorService renewer = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final Identifier identifier = new Identifier("db", "table", "dev");
    private final AtomicLong clock = new AtomicLong();
    private final RESTCatalogLock lock =
            new RESTCatalogLock(
                    api, identifier, "table-id", "morax-job", 0, () -> renewer, clock::get);

    @Test
    void callbackIsProtectedAndLeaseEndsOnReturn() throws Exception {
        grant("morax-job", 60000);
        assertThat(
                        run(
                                () -> {
                                    lock.ensureValid();
                                    return "published";
                                }))
                .isEqualTo("published");
        verify(future).cancel(true);
        assertThatThrownBy(lock::ensureValid).hasMessageContaining("No commit lease");
    }

    @Test
    void callbackFailureStopsRenewal() {
        grant("morax-job", 60000);
        RuntimeException failure = new RuntimeException("Preparation failed");
        assertThatThrownBy(
                        () ->
                                run(
                                        () -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        verify(future).cancel(true);
    }

    @Test
    void renewalUsesExactOwnerAndStopsAfterClose() throws Exception {
        grant("morax-job", 60000);
        when(api.renewCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(new CommitLockResponse(true, "morax-job", 100000, 60000, null));
        Runnable callback =
                run(
                        () -> {
                            Runnable renewal = renewal();
                            renewal.run();
                            lock.ensureValid();
                            return renewal;
                        });
        callback.run();
        verify(api).renewCommitLock(identifier, "table-id", "morax-job");
        verify(future).cancel(true);
    }

    @Test
    void rejectedRenewalInvalidatesLease() throws Exception {
        grant("morax-job", 60000);
        when(api.renewCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(CommitLockResponse.unavailable());
        run(
                () -> {
                    renewal().run();
                    renewal().run();
                    assertThatThrownBy(lock::ensureValid)
                            .hasCauseInstanceOf(IllegalStateException.class);
                    return null;
                });
        verify(api).renewCommitLock(identifier, "table-id", "morax-job");
    }

    @Test
    void renewalExceptionPreservesCause() throws Exception {
        grant("morax-job", 60000);
        RuntimeException failure = new RuntimeException("Disconnected");
        when(api.renewCommitLock(identifier, "table-id", "morax-job")).thenThrow(failure);
        run(
                () -> {
                    renewal().run();
                    assertThatThrownBy(lock::ensureValid).hasCause(failure);
                    return null;
                });
    }

    @Test
    void busyTimeoutDoesNotExecuteCallbackOrStartRenewal() {
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(CommitLockResponse.unavailable());
        assertThatThrownBy(
                        () ->
                                run(
                                        () -> {
                                            throw new AssertionError();
                                        }))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasRootCauseMessage("Timed out acquiring commit lock for " + identifier);
        verifyNoInteractions(renewer);
    }

    @Test
    void invalidGrantDoesNotStartRenewal() {
        grant("other-job", 60000);
        assertThatThrownBy(() -> run(() -> null)).isInstanceOf(Lock.AcquisitionException.class);
        grant("morax-job", 0);
        assertThatThrownBy(() -> run(() -> null)).isInstanceOf(Lock.AcquisitionException.class);
        verifyNoInteractions(renewer);
    }

    @Test
    void missingIdentityAndGeneralLockAreRejectedBeforeRequest() {
        assertThatThrownBy(() -> new RESTCatalogLock(api, identifier, "", "morax-job", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RESTCatalogLock(api, identifier, "table-id", "", 0))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(api, renewer);
    }

    @Test
    void expiryIsDetectedEvenWhenRenewerHasNotRun() throws Exception {
        grant("morax-job", 60000);
        run(
                () -> {
                    clock.set(TimeUnit.MILLISECONDS.toNanos(60000));
                    assertThatThrownBy(lock::ensureValid).hasMessageContaining("no longer usable");
                    renewal().run();
                    verify(api, never()).renewCommitLock(identifier, "table-id", "morax-job");
                    return null;
                });
    }

    @Test
    void renewalExtendsLocalLifetimeAndIncludesResponseTime() throws Exception {
        grant("morax-job", 60000);
        when(api.renewCommitLock(identifier, "table-id", "morax-job"))
                .thenAnswer(
                        invocation -> {
                            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10000));
                            return new CommitLockResponse(true, "morax-job", 100000, 60000, null);
                        });
        run(
                () -> {
                    clock.set(TimeUnit.MILLISECONDS.toNanos(20000));
                    renewal().run();
                    clock.set(TimeUnit.MILLISECONDS.toNanos(79999));
                    lock.ensureValid();
                    clock.set(TimeUnit.MILLISECONDS.toNanos(80000));
                    assertThatThrownBy(lock::ensureValid).isInstanceOf(IllegalStateException.class);
                    return null;
                });
    }

    @Test
    void acquirePreservesCatalogPermissionAndMissingTableErrors() {
        ForbiddenException denied = new ForbiddenException("Denied");
        when(api.acquireCommitLock(identifier, "table-id", "morax-job")).thenThrow(denied);
        assertThatThrownBy(() -> run(() -> null))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasCauseInstanceOf(Catalog.TableNoPermissionException.class)
                .hasRootCause(denied);
        NoSuchResourceException missing = new NoSuchResourceException("table", "table", "Missing");
        doThrow(missing).when(api).acquireCommitLock(identifier, "table-id", "morax-job");
        assertThatThrownBy(() -> run(() -> null))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasCauseInstanceOf(Catalog.TableNotExistException.class)
                .hasRootCause(missing);
        verifyNoInteractions(renewer);
    }

    @Test
    void slowAcquisitionCannotStartProtectedOperation() {
        grant("morax-job", 60000);
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenAnswer(
                        invocation -> {
                            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(60000));
                            return new CommitLockResponse(true, "morax-job", 100000, 60000, null);
                        });
        assertThatThrownBy(
                        () ->
                                run(
                                        () -> {
                                            throw new AssertionError();
                                        }))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasRootCauseMessage("The commit lease has expired locally.");
        verify(future).cancel(true);
        verify(renewer).shutdownNow();
        assertThatThrownBy(lock::ensureValid).hasMessageContaining("No commit lease");
    }

    @Test
    void schedulerFailureStopsWorkerAndAllowsFreshAcquisition() throws Exception {
        grant("morax-job", 60000);
        RejectedExecutionException failure = new RejectedExecutionException("Stopped worker");
        when(renewer.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenThrow(failure);
        assertThatThrownBy(
                        () ->
                                run(
                                        () -> {
                                            throw new AssertionError();
                                        }))
                .isInstanceOf(Lock.AcquisitionException.class)
                .hasCause(failure);
        verify(renewer).shutdownNow();
        assertThatThrownBy(lock::ensureValid).hasMessageContaining("No commit lease");
        doReturn(future)
                .when(renewer)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any());
        assertThat(run(() -> "published")).isEqualTo("published");
        verify(renewer, times(2)).shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void workerShutdownFailurePreservesOperationOutcome(boolean failOperation) throws Exception {
        grant("morax-job", 60000);
        RuntimeException releaseFailure = new RuntimeException("Worker shutdown failed");
        RuntimeException operationFailure = new RuntimeException("Preparation failed");
        doThrow(releaseFailure).when(renewer).shutdownNow();
        Callable<String> operation =
                () -> {
                    if (failOperation) {
                        throw operationFailure;
                    }
                    return "published";
                };
        if (failOperation) {
            assertThatThrownBy(() -> run(operation))
                    .isSameAs(operationFailure)
                    .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(releaseFailure));
        } else {
            assertThat(run(operation)).isEqualTo("published");
        }
        verify(future).cancel(true);
        assertThatThrownBy(lock::ensureValid).hasMessageContaining("No commit lease");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidRenewalGrantCannotExtendLease(boolean differentOwner) throws Exception {
        grant("morax-job", 60000);
        when(api.renewCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(
                        new CommitLockResponse(
                                true,
                                differentOwner ? "other-job" : "morax-job",
                                100000,
                                differentOwner ? 60000 : 120000,
                                null));
        run(
                () -> {
                    renewal().run();
                    assertThatThrownBy(lock::ensureValid)
                            .isInstanceOf(IllegalStateException.class)
                            .hasRootCauseMessage(
                                    differentOwner
                                            ? "The server returned an invalid commit lease."
                                            : "The commit lease duration changed during renewal.");
                    return null;
                });
    }

    @Test
    void eachScopeOwnsAndStopsItsRenewalWorker() throws Exception {
        grant("morax-job", 60000);
        ScheduledExecutorService secondWorker = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> secondRenewal = mock(ScheduledFuture.class);
        doReturn(secondRenewal)
                .when(secondWorker)
                .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any());
        AtomicInteger workers = new AtomicInteger();
        RESTCatalogLock scopes =
                new RESTCatalogLock(
                        api,
                        identifier,
                        "table-id",
                        "morax-job",
                        0,
                        () -> workers.getAndIncrement() == 0 ? renewer : secondWorker,
                        clock::get);
        assertThat(scopes.runWithLock(() -> "first")).isEqualTo("first");
        assertThat(scopes.runWithLock(() -> "second")).isEqualTo("second");
        assertThat(workers.get()).isEqualTo(2);
        verify(future).cancel(true);
        verify(secondRenewal).cancel(true);
        verify(renewer).shutdownNow();
        verify(secondWorker).shutdownNow();
    }

    @Test
    void nestedPublicationReusesLeaseAndRechecksValidity() throws Exception {
        grant("morax-job", 60000);
        Lock publication = Lock.reentrant(lock);
        assertThat(publication.runWithLock(() -> publication.runWithLock(() -> "published")))
                .isEqualTo("published");
        verify(api).acquireCommitLock(identifier, "table-id", "morax-job");
        verify(future).cancel(true);
        publication.runWithLock(
                () -> {
                    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(60000));
                    assertThatThrownBy(
                                    () ->
                                            publication.runWithLock(
                                                    () -> {
                                                        throw new AssertionError();
                                                    }))
                            .hasMessageContaining("no longer usable");
                    return null;
                });
        verify(api, times(2)).acquireCommitLock(identifier, "table-id", "morax-job");
    }

    @Test
    void closingAnOldScopeTwiceCannotInvalidateTheNewLease() throws Exception {
        grant("morax-job", 60000);
        AutoCloseable first = lock.acquire();
        first.close();
        try (AutoCloseable second = lock.acquire()) {
            first.close();
            lock.ensureValid();
        }
        verify(api, times(2)).acquireCommitLock(identifier, "table-id", "morax-job");
        verify(future, times(2)).cancel(true);
    }

    @Test
    void interruptedContentionStopsAcquisition() {
        grant("morax-job", 60000);
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(CommitLockResponse.unavailable());
        RESTCatalogLock waiting =
                new RESTCatalogLock(
                        api, identifier, "table-id", "morax-job", 1000, () -> renewer, clock::get);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(
                            () ->
                                    waiting.runWithLock(
                                            () -> {
                                                throw new AssertionError();
                                            }))
                    .isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        verifyNoInteractions(renewer);
    }

    private <T> T run(Callable<T> callable) throws Exception {
        return lock.runWithLock(callable);
    }

    private void grant(String owner, long leaseMillis) {
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(new CommitLockResponse(true, owner, 100000, leaseMillis, null));
        when(renewer.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
                .thenAnswer(invocation -> future);
    }

    private Runnable renewal() {
        ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
        verify(renewer)
                .scheduleWithFixedDelay(
                        callback.capture(), eq(20000L), eq(20000L), eq(TimeUnit.MILLISECONDS));
        return callback.getValue();
    }
}
