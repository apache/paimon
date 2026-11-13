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
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.RESTCatalogLock;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Callable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A snapshot publisher bound to the lock protecting its preparation and publication. */
public abstract class SnapshotCommit implements Lock {

    private final Lock lock;
    public final boolean lockOnRetry;

    protected SnapshotCommit(Lock lock) {
        this.lock =
                Lock.reentrant(checkNotNull(lock, "Snapshot publication requires a bound lock."));
        this.lockOnRetry = this instanceof CatalogSnapshotCommit && lock instanceof RESTCatalogLock;
    }

    /** Publish using the bound lock or the REST catalog's expected-head check. */
    public final boolean commit(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception {
        if (lockOnRetry && !((Lock.ReentrantLock) lock).isHeldByCurrentThread()) {
            return commitImpl(baseSnapshotUuid, snapshot, branch, statistics);
        }
        return runWithLock(() -> commitImpl(baseSnapshotUuid, snapshot, branch, statistics));
    }

    @Override
    public final AutoCloseable acquire() throws Exception {
        return lock.acquire();
    }

    @Override
    public final <T> T runWithLock(Callable<T> callable) throws Exception {
        return lock.runWithLock(callable);
    }

    @Override
    public final void ensureValid() {
        lock.ensureValid();
    }

    @Override
    public final void close() throws Exception {
        try {
            lock.close();
        } finally {
            closeImpl();
        }
    }

    /** Backend publication, after any required lock validation. */
    protected abstract boolean commitImpl(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception;

    /** Release backend resources after closing the lock, including when lock closure fails. */
    protected void closeImpl() throws Exception {}
}
