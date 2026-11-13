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
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.responses.CommitLockResponse;
import org.apache.paimon.utils.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A renewable REST commit lease bound to a table, branch and writer. */
public class RESTCatalogLock implements Lock {

    private static final ScheduledExecutorService RENEWAL_SCHEDULER = createRenewalScheduler();
    private static final ExecutorService RENEWAL_WORKERS =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("rest-commit-lock-renew"));

    private final RESTApi api;
    private final Identifier identifier;
    private final String tableUuid;
    private final String commitUser;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService workers;
    private final LongSupplier nanoTime;
    private final long acquireTimeoutMillis;
    private final ThreadLocal<Lease> currentLease = new ThreadLocal<>();

    public RESTCatalogLock(
            RESTApi api,
            Identifier identifier,
            String tableUuid,
            String commitUser,
            long acquireTimeoutMillis) {
        this(
                api,
                identifier,
                tableUuid,
                commitUser,
                acquireTimeoutMillis,
                RENEWAL_SCHEDULER,
                RENEWAL_WORKERS,
                System::nanoTime);
    }

    RESTCatalogLock(
            RESTApi api,
            Identifier identifier,
            String tableUuid,
            String commitUser,
            long acquireTimeoutMillis,
            ScheduledExecutorService scheduler,
            ExecutorService workers,
            LongSupplier nanoTime) {
        checkArgument(
                tableUuid != null && !tableUuid.isEmpty(), "A commit lease requires a table UUID.");
        checkArgument(
                commitUser != null && !commitUser.isEmpty(),
                "A commit lease requires a commit user.");
        checkArgument(
                acquireTimeoutMillis >= 0, "The lock acquisition timeout cannot be negative.");
        this.api = checkNotNull(api);
        this.identifier = checkNotNull(identifier);
        this.tableUuid = tableUuid;
        this.commitUser = commitUser;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.scheduler = checkNotNull(scheduler);
        this.workers = checkNotNull(workers);
        this.nanoTime = nanoTime;
    }

    static ScheduledThreadPoolExecutor createRenewalScheduler() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(
                        1, new ExecutorThreadFactory("rest-commit-lock-timer"));
        executor.setRemoveOnCancelPolicy(true);
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    @Override
    public AutoCloseable acquire() throws Exception {
        checkArgument(currentLease.get() == null, "This thread already holds a commit lease.");
        long startedAt = nanoTime.getAsLong();
        while (true) {
            long requestedAt = nanoTime.getAsLong();
            CommitLockResponse response = acquireLease();
            if (response.isAcquired()) {
                validateGrant(response);
                Lease lease = new Lease(response.getLeaseMillis(), requestedAt);
                try {
                    lease.checkValid();
                    currentLease.set(lease);
                    return () -> {
                        if (currentLease.get() == lease) {
                            currentLease.remove();
                        }
                        lease.close();
                    };
                } catch (Exception | Error e) {
                    Lock.release(lease, e);
                    throw e;
                }
            }
            if (TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - startedAt)
                    >= acquireTimeoutMillis) {
                throw new IllegalStateException(
                        "Timed out acquiring commit lock for " + identifier);
            }
            Thread.sleep(Math.min(100, acquireTimeoutMillis));
        }
    }

    private CommitLockResponse acquireLease() throws Catalog.TableNotExistException {
        try {
            return api.acquireCommitLock(identifier, tableUuid, commitUser);
        } catch (NoSuchResourceException e) {
            throw new Catalog.TableNotExistException(identifier, e);
        } catch (ForbiddenException e) {
            throw new Catalog.TableNoPermissionException(identifier, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private void validateGrant(CommitLockResponse response) {
        if (!commitUser.equals(response.getCommitUser()) || response.getLeaseMillis() <= 0) {
            throw new IllegalStateException("The server returned an invalid commit lease.");
        }
    }

    @Override
    public void ensureValid() {
        Lease lease = currentLease.get();
        if (lease == null) {
            throw new IllegalStateException("No commit lease is held by this thread.");
        }
        lease.checkValid();
    }

    private class Lease implements AutoCloseable {
        private final ScheduledFuture<?> renewal;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean renewing = new AtomicBoolean();
        private final long leaseMillis;
        private volatile long renewedAt;
        @Nullable private volatile Throwable renewalFailure;
        @Nullable private volatile FutureTask<Void> renewalRequest;

        private Lease(long leaseMillis, long requestedAt) {
            this.leaseMillis = leaseMillis;
            this.renewedAt = requestedAt;
            long interval = Math.max(1, leaseMillis / 3);
            this.renewal =
                    scheduler.scheduleWithFixedDelay(
                            this::submitRenewal, interval, interval, TimeUnit.MILLISECONDS);
        }

        private void submitRenewal() {
            if (closed.get() || renewalFailure != null || !renewing.compareAndSet(false, true)) {
                return;
            }
            FutureTask<Void> request =
                    new FutureTask<>(
                            () -> {
                                try {
                                    renew();
                                } finally {
                                    renewing.set(false);
                                }
                            },
                            null);
            renewalRequest = request;
            // Closing can race with dispatch. Publish the request before rechecking closed so
            // either close or this dispatcher cancels it before it can start another RPC.
            if (closed.get()) {
                request.cancel(true);
                renewing.set(false);
                return;
            }
            try {
                workers.execute(request);
            } catch (RuntimeException | Error e) {
                renewalFailure = e;
                request.cancel(false);
                renewing.set(false);
            }
        }

        private void renew() {
            if (closed.get() || renewalFailure != null) {
                return;
            }
            try {
                // Renewal workers validate this lease without the acquiring thread's context.
                checkValid();
                long requestedAt = nanoTime.getAsLong();
                CommitLockResponse response =
                        api.renewCommitLock(identifier, tableUuid, commitUser);
                if (!response.isAcquired()) {
                    throw new IllegalStateException("The commit lease has expired.");
                }
                validateGrant(response);
                if (response.getLeaseMillis() != leaseMillis) {
                    throw new IllegalStateException(
                            "The commit lease duration changed during renewal.");
                }
                renewedAt = requestedAt;
            } catch (Exception | Error e) {
                renewalFailure = e;
            }
        }

        private void checkValid() {
            // Include response time conservatively without relying on the renewal worker running.
            if (TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - renewedAt) >= leaseMillis
                    && renewalFailure == null) {
                renewalFailure = new IllegalStateException("The commit lease has expired locally.");
            }
            if (closed.get() || renewalFailure != null) {
                throw new IllegalStateException(
                        "The commit lease is no longer usable.", renewalFailure);
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                try {
                    renewal.cancel(false);
                } finally {
                    FutureTask<Void> request = renewalRequest;
                    if (request != null) {
                        request.cancel(true);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        // Lease scopes cancel their own tasks; shared executors outlive individual locks.
    }
}
