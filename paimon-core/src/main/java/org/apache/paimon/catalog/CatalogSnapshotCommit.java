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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.utils.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** A {@link SnapshotCommit} using {@link Catalog} to commit. */
public class CatalogSnapshotCommit implements SnapshotCommit {

    private static final ScheduledThreadPoolExecutor RENEWER = createRenewer();

    private final Catalog catalog;
    private final ScheduledExecutorService renewer;
    private final Identifier identifier;
    @Nullable private final String uuid;

    public CatalogSnapshotCommit(Catalog catalog, Identifier identifier, @Nullable String uuid) {
        this(catalog, identifier, uuid, RENEWER);
    }

    CatalogSnapshotCommit(
            Catalog catalog,
            Identifier identifier,
            @Nullable String uuid,
            ScheduledExecutorService renewer) {
        this.catalog = catalog;
        this.identifier = identifier;
        this.uuid = uuid;
        this.renewer = renewer;
    }

    private static ScheduledThreadPoolExecutor createRenewer() {
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(2, new ExecutorThreadFactory("commit-lock-renew"));
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public Optional<CommitAttempt> beginCommit(
            String branch, String commitUser, boolean acquireLock) throws Exception {
        if (!acquireLock) {
            return SnapshotCommit.super.beginCommit(branch, commitUser, false);
        }
        if (uuid == null) {
            throw new IllegalArgumentException("A commit lease requires a stable table UUID.");
        }
        Identifier branchIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch);
        Optional<CatalogCommitLock> lease =
                catalog.acquireCommitLock(branchIdentifier, uuid, commitUser);
        if (!lease.isPresent()) {
            return Optional.empty();
        }
        if (!commitUser.equals(lease.get().commitUser())) {
            throw new IllegalStateException("The catalog granted a lease for another commit user.");
        }
        return Optional.of(new LockedAttempt(branchIdentifier, lease.get()));
    }

    private class LockedAttempt extends CommitAttempt {
        private final Identifier branchIdentifier;
        private final CatalogCommitLock lease;
        private final ScheduledFuture<?> renewal;
        private final AtomicBoolean closed = new AtomicBoolean();
        @Nullable private volatile Exception renewalFailure;

        private LockedAttempt(Identifier branchIdentifier, CatalogCommitLock lease) {
            this.branchIdentifier = branchIdentifier;
            this.lease = lease;
            long interval = Math.max(1, lease.leaseMillis() / 3);
            this.renewal =
                    renewer.scheduleWithFixedDelay(
                            this::renew, interval, interval, TimeUnit.MILLISECONDS);
        }

        private void renew() {
            if (closed.get() || renewalFailure != null) {
                return;
            }
            try {
                if (!catalog.renewCommitLock(branchIdentifier, uuid, lease.commitUser())) {
                    renewalFailure = new IllegalStateException("The commit lease has expired.");
                }
            } catch (Exception e) {
                renewalFailure = e;
            }
        }

        @Override
        public Snapshot latestSnapshot(Supplier<Snapshot> loader) {
            return lease.snapshot();
        }

        @Override
        public boolean commit(
                String baseSnapshotUuid,
                Snapshot snapshot,
                String branch,
                List<PartitionStatistics> statistics)
                throws Exception {
            if (closed.get() || renewalFailure != null) {
                throw new IllegalStateException(
                        "The commit lease is no longer usable.", renewalFailure);
            }
            if (!branchIdentifier.equals(
                    new Identifier(
                            identifier.getDatabaseName(), identifier.getTableName(), branch))) {
                throw new IllegalArgumentException(
                        "A commit lease cannot be used for another branch.");
            }
            if (!lease.commitUser().equals(snapshot.commitUser())) {
                throw new IllegalArgumentException(
                        "A commit lease belongs to its exact commit user.");
            }
            return catalog.commitSnapshot(
                    branchIdentifier, uuid, baseSnapshotUuid, snapshot, statistics);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            renewal.cancel(false);
        }
    }

    @Override
    public boolean commit(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception {
        Identifier newIdentifier =
                new Identifier(identifier.getDatabaseName(), identifier.getTableName(), branch);
        return catalog.commitSnapshot(newIdentifier, uuid, baseSnapshotUuid, snapshot, statistics);
    }

    @Override
    public void close() throws Exception {
        catalog.close();
    }
}
