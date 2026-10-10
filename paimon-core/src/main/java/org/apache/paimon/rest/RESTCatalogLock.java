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
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.responses.CommitLockResponse;
import org.apache.paimon.utils.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * REST commit leases. General catalog operations are not protected by the commit lease protocol.
 */
public class RESTCatalogLock implements CatalogLock {

    private static final ScheduledThreadPoolExecutor RENEWER = createRenewer();

    private final RESTApi api;
    private final ScheduledExecutorService renewer;
    private final LongSupplier nanoTime;

    public RESTCatalogLock(RESTApi api) {
        this(api, RENEWER);
    }

    RESTCatalogLock(RESTApi api, ScheduledExecutorService renewer) {
        this(api, renewer, System::nanoTime);
    }

    RESTCatalogLock(RESTApi api, ScheduledExecutorService renewer, LongSupplier nanoTime) {
        this.api = api;
        this.renewer = renewer;
        this.nanoTime = nanoTime;
    }

    private static ScheduledThreadPoolExecutor createRenewer() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(2, new ExecutorThreadFactory("commit-lock-renew"));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) {
        throw new UnsupportedOperationException(
                "REST commit leases require a table UUID, branch and commit user.");
    }

    @Override
    public Optional<CatalogCommitLock> acquireCommitLock(
            Identifier identifier, String tableUuid, String commitUser)
            throws Catalog.TableNotExistException {
        if (tableUuid == null
                || tableUuid.isEmpty()
                || commitUser == null
                || commitUser.isEmpty()) {
            throw new IllegalArgumentException(
                    "A commit lease requires a table UUID and commit user.");
        }
        try {
            long requestedAt = nanoTime.getAsLong();
            CommitLockResponse response = api.acquireCommitLock(identifier, tableUuid, commitUser);
            if (!response.isAcquired()) {
                return Optional.empty();
            }
            if (!commitUser.equals(response.getCommitUser()) || response.getLeaseMillis() <= 0) {
                throw new IllegalStateException("The server returned an invalid commit lease.");
            }
            return Optional.of(new Lease(identifier, tableUuid, commitUser, response, requestedAt));
        } catch (NoSuchResourceException e) {
            throw new Catalog.TableNotExistException(identifier, e);
        } catch (ForbiddenException e) {
            throw new Catalog.TableNoPermissionException(identifier, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private class Lease implements CatalogCommitLock {
        private final Identifier identifier;
        private final String tableUuid;
        private final String commitUser;
        @Nullable private final Snapshot snapshot;
        private final ScheduledFuture<?> renewal;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final long leaseNanos;
        private volatile long renewedAt;
        @Nullable private volatile Exception renewalFailure;

        private Lease(
                Identifier identifier,
                String tableUuid,
                String commitUser,
                CommitLockResponse response,
                long requestedAt) {
            this.identifier = identifier;
            this.tableUuid = tableUuid;
            this.commitUser = commitUser;
            this.snapshot = response.getSnapshot();
            this.leaseNanos = TimeUnit.MILLISECONDS.toNanos(response.getLeaseMillis());
            this.renewedAt = requestedAt;
            long interval = Math.max(1, response.getLeaseMillis() / 3);
            this.renewal =
                    renewer.scheduleWithFixedDelay(
                            this::renew, interval, interval, TimeUnit.MILLISECONDS);
        }

        private void renew() {
            if (closed.get() || renewalFailure != null) {
                return;
            }
            try {
                ensureValid();
                long requestedAt = nanoTime.getAsLong();
                if (!api.renewCommitLock(identifier, tableUuid, commitUser)) {
                    renewalFailure = new IllegalStateException("The commit lease has expired.");
                } else {
                    renewedAt = requestedAt;
                }
            } catch (Exception e) {
                renewalFailure = e;
            }
        }

        @Override
        public Snapshot snapshot() {
            return snapshot;
        }

        @Override
        public void ensureValid() {
            // Count response time conservatively and do not depend on the renewer being scheduled.
            if (nanoTime.getAsLong() - renewedAt >= leaseNanos && renewalFailure == null) {
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
                renewal.cancel(false);
            }
        }
    }

    @Override
    public void close() {
        // RESTApi has no owned closeable resources; individual lease scopes stop renewal.
    }
}
