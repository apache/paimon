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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * A {@link SnapshotCommit} using file renaming to commit.
 *
 * <p>Note that when the file system is local or HDFS, rename is atomic. But if the file system is
 * object storage, we need additional lock protection unless the storage supports native conditional
 * writes.
 */
public class RenamingSnapshotCommit implements SnapshotCommit {

    private final SnapshotManager snapshotManager;
    private final FileIO fileIO;
    private final Lock lock;

    public RenamingSnapshotCommit(SnapshotManager snapshotManager, Lock lock) {
        this.snapshotManager = snapshotManager;
        this.fileIO = snapshotManager.fileIO();
        this.lock = lock;
    }

    @Override
    public boolean commit(Snapshot snapshot, String branch, List<PartitionStatistics> statistics)
            throws Exception {
        Path newSnapshotPath =
                snapshotManager.branch().equals(branch)
                        ? snapshotManager.snapshotPath(snapshot.id())
                        : snapshotManager.copyWithBranch(branch).snapshotPath(snapshot.id());

        // Use native conditional writes if supported
        // This eliminates the need for external locking on object stores like S3
        if (fileIO.supportsConditionalWrite()) {
            boolean committed = fileIO.tryToWriteAtomicIfAbsent(newSnapshotPath, snapshot.toJson());
            if (committed) {
                snapshotManager.commitLatestHint(snapshot.id());
            }
            return committed;
        }

        // Fall back to lock-based approach for filesystems without conditional write support
        Callable<Boolean> callable =
                () -> {
                    boolean committed = fileIO.tryToWriteAtomic(newSnapshotPath, snapshot.toJson());
                    if (committed) {
                        snapshotManager.commitLatestHint(snapshot.id());
                    }
                    return committed;
                };
        return lock.runWithLock(
                () ->
                        // fs.rename may not returns false if target file
                        // already exists, or even not atomic
                        // as we're relying on external locking, we can first
                        // check if file exist then rename to work around this
                        // case
                        !fileIO.exists(newSnapshotPath) && callable.call());
    }

    @Override
    public void close() throws Exception {
        this.lock.close();
    }
}
