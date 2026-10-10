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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogCommitLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.responses.CommitLockResponse;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Tests REST lease renewal and scope lifetime independently of snapshot publication. */
class RESTCatalogLockTest {
    private final RESTApi api = mock(RESTApi.class);
    private final ScheduledExecutorService renewer = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> future = mock(ScheduledFuture.class);
    private final Identifier identifier = new Identifier("db", "table", "dev");
    private final AtomicLong clock = new AtomicLong();
    private final RESTCatalogLock lock = new RESTCatalogLock(api, renewer, clock::get);

    @Test
    void grantedHeadIsAuthoritativeAndCloseIsIdempotent() throws Exception {
        Snapshot head = mock(Snapshot.class);
        grant("morax-job", 60000, head);
        CatalogCommitLock lease = lock.acquireCommitLock(identifier, "table-id", "morax-job").get();
        assertThat(lease.snapshot()).isSameAs(head);
        lease.ensureValid();
        lease.close();
        lease.close();
        verify(future).cancel(false);
        assertThatThrownBy(lease::ensureValid).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void renewalUsesExactOwnerAndStopsAfterClose() throws Exception {
        grant("morax-job", 60000, null);
        when(api.renewCommitLock(identifier, "table-id", "morax-job")).thenReturn(true);
        CatalogCommitLock lease = lock.acquireCommitLock(identifier, "table-id", "morax-job").get();
        Runnable renewal = renewal();
        renewal.run();
        lease.ensureValid();
        lease.close();
        renewal.run();
        verify(api, times(1)).renewCommitLock(identifier, "table-id", "morax-job");
        verify(future).cancel(false);
    }

    @Test
    void rejectedRenewalInvalidatesLease() throws Exception {
        grant("morax-job", 60000, null);
        when(api.renewCommitLock(identifier, "table-id", "morax-job")).thenReturn(false);
        try (CatalogCommitLock lease =
                lock.acquireCommitLock(identifier, "table-id", "morax-job").get()) {
            Runnable renewal = renewal();
            renewal.run();
            renewal.run();
            assertThatThrownBy(lease::ensureValid)
                    .isInstanceOf(IllegalStateException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
            verify(api, times(1)).renewCommitLock(identifier, "table-id", "morax-job");
        }
    }

    @Test
    void renewalExceptionPreservesCause() throws Exception {
        grant("morax-job", 60000, null);
        RuntimeException failure = new RuntimeException("Disconnected");
        when(api.renewCommitLock(identifier, "table-id", "morax-job")).thenThrow(failure);
        try (CatalogCommitLock lease =
                lock.acquireCommitLock(identifier, "table-id", "morax-job").get()) {
            renewal().run();
            assertThatThrownBy(lease::ensureValid).hasCause(failure);
        }
    }

    @Test
    void busyLeaseDoesNotStartRenewal() throws Exception {
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(CommitLockResponse.unavailable());
        assertThat(lock.acquireCommitLock(identifier, "table-id", "morax-job")).isEmpty();
        verifyNoInteractions(renewer);
    }

    @Test
    void invalidGrantDoesNotStartRenewal() {
        grant("other-job", 60000, null);
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "table-id", "morax-job"))
                .isInstanceOf(IllegalStateException.class);
        grant("morax-job", 0, null);
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "table-id", "morax-job"))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(renewer);
    }

    @Test
    void missingIdentityAndGeneralLockAreRejectedBeforeRequest() {
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "", "morax-job"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "table-id", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> lock.runWithLock("db", "table", () -> null))
                .isInstanceOf(UnsupportedOperationException.class);
        verifyNoInteractions(api, renewer);
    }

    @Test
    void expiryIsDetectedEvenWhenRenewerHasNotRun() throws Exception {
        grant("morax-job", 60000, null);
        try (CatalogCommitLock lease =
                lock.acquireCommitLock(identifier, "table-id", "morax-job").get()) {
            clock.set(TimeUnit.MILLISECONDS.toNanos(60000));
            assertThatThrownBy(lease::ensureValid).hasMessageContaining("no longer usable");
            renewal().run();
            verify(api, never()).renewCommitLock(identifier, "table-id", "morax-job");
        }
    }

    @Test
    void renewalExtendsLocalLifetimeAndIncludesResponseTime() throws Exception {
        grant("morax-job", 60000, null);
        when(api.renewCommitLock(identifier, "table-id", "morax-job"))
                .thenAnswer(
                        invocation -> {
                            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(10000));
                            return true;
                        });
        try (CatalogCommitLock lease =
                lock.acquireCommitLock(identifier, "table-id", "morax-job").get()) {
            clock.set(TimeUnit.MILLISECONDS.toNanos(20000));
            renewal().run();
            clock.set(TimeUnit.MILLISECONDS.toNanos(79999));
            lease.ensureValid();
            clock.set(TimeUnit.MILLISECONDS.toNanos(80000));
            assertThatThrownBy(lease::ensureValid).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void acquirePreservesCatalogPermissionAndMissingTableErrors() {
        ForbiddenException denied = new ForbiddenException("Denied");
        when(api.acquireCommitLock(identifier, "table-id", "morax-job")).thenThrow(denied);
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "table-id", "morax-job"))
                .isInstanceOf(Catalog.TableNoPermissionException.class)
                .hasCause(denied);
        NoSuchResourceException missing = new NoSuchResourceException("table", "table", "Missing");
        doThrow(missing).when(api).acquireCommitLock(identifier, "table-id", "morax-job");
        assertThatThrownBy(() -> lock.acquireCommitLock(identifier, "table-id", "morax-job"))
                .isInstanceOf(Catalog.TableNotExistException.class)
                .hasCause(missing);
        verifyNoInteractions(renewer);
    }

    private void grant(String owner, long leaseMillis, Snapshot head) {
        when(api.acquireCommitLock(identifier, "table-id", "morax-job"))
                .thenReturn(new CommitLockResponse(true, owner, 100000, leaseMillis, head));
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
