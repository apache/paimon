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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.utils.RetryWaiter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** A single snapshot preparation and publication, optionally protected by a commit lock. */
public abstract class CommitAttempt implements AutoCloseable {

    /** Use the granted head for a locked attempt, or load the head for an optimistic attempt. */
    @Nullable
    public abstract Snapshot latestSnapshot(Supplier<Snapshot> loader);

    public abstract boolean commit(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception;

    /**
     * End this attempt idempotently. Catalog lease implementations stop renewal and let the server
     * release the lease on publication or expiry.
     */
    @Override
    public abstract void close();

    /** Wait for a commit lock separately from the number of snapshot publication attempts. */
    public static CommitAttempt begin(
            SnapshotCommit commit,
            CoreOptions options,
            String commitUser,
            int retryCount,
            long startedMillis) {
        boolean locked = options.commitLockOnRetry() && retryCount > 0;
        if (!locked) {
            return unlocked(commit);
        }
        RetryWaiter waiter =
                new RetryWaiter(options.commitMinRetryWait(), options.commitMaxRetryWait());
        int lockRetry = 0;
        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new RuntimeException("Interrupted while acquiring the commit lock.");
            }
            try {
                Optional<CommitAttempt> attempt =
                        commit.beginCommit(options.branch(), commitUser, locked);
                if (attempt.isPresent()) {
                    return attempt.get();
                }
                if (System.currentTimeMillis() - startedMillis >= options.commitTimeout()) {
                    throw new RuntimeException("Timed out waiting for the commit lock.");
                }
                waiter.retryWait(lockRetry++);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while acquiring the commit lock.", e);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Could not acquire the commit lock.", e);
            }
        }
    }

    public static CommitAttempt unlocked(SnapshotCommit commit) {
        return new CommitAttempt() {
            @Override
            public Snapshot latestSnapshot(Supplier<Snapshot> loader) {
                return loader.get();
            }

            @Override
            public boolean commit(
                    String baseSnapshotUuid,
                    Snapshot snapshot,
                    String branch,
                    List<PartitionStatistics> statistics)
                    throws Exception {
                return commit.commit(baseSnapshotUuid, snapshot, branch, statistics);
            }

            @Override
            public void close() {}
        };
    }

    public static CommitAttempt locked(
            SnapshotCommit commit, CatalogCommitLock lease, String branch, String commitUser) {
        return new CommitAttempt() {
            private final AtomicBoolean closed = new AtomicBoolean();

            @Override
            public Snapshot latestSnapshot(Supplier<Snapshot> loader) {
                return lease.snapshot();
            }

            @Override
            public boolean commit(
                    String baseSnapshotUuid,
                    Snapshot snapshot,
                    String commitBranch,
                    List<PartitionStatistics> statistics)
                    throws Exception {
                if (closed.get()) {
                    throw new IllegalStateException("The commit attempt is closed.");
                }
                lease.ensureValid();
                if (!branch.equals(commitBranch) || !commitUser.equals(snapshot.commitUser())) {
                    throw new IllegalArgumentException(
                            "A commit lease belongs to its exact branch and commit user.");
                }
                return commit.commit(baseSnapshotUuid, snapshot, commitBranch, statistics);
            }

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    lease.close();
                }
            }
        };
    }
}
