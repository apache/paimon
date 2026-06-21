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

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A {@link SnapshotCommit} using file renaming to commit.
 *
 * <p>Note that when the file system is local or HDFS, rename is atomic. But if the file system is
 * object storage, we need additional lock protection.
 */
public class RenamingSnapshotCommit implements SnapshotCommit {

    private static final String HADOOP_FILE_ALREADY_EXISTS_EXCEPTION =
            "org.apache.hadoop.fs.FileAlreadyExistsException";

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
        SnapshotManager targetSnapshotManager =
                snapshotManager.branch().equals(branch)
                        ? snapshotManager
                        : snapshotManager.copyWithBranch(branch);
        Path newSnapshotPath = targetSnapshotManager.snapshotPath(snapshot.id());
        boolean supportsNoListCommit =
                fileIO.isObjectStore()
                        && fileIO.supportsAtomicCreateWithoutOverwrite(newSnapshotPath);

        Callable<Boolean> callable =
                () -> {
                    boolean committed =
                            supportsNoListCommit
                                    ? writeNoOverwriteOrRecover(snapshot, newSnapshotPath)
                                    : writeAtomicOrRecover(snapshot, newSnapshotPath);

                    if (committed) {
                        targetSnapshotManager.commitLatestHint(snapshot.id());
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
                        (supportsNoListCommit || !fileIO.exists(newSnapshotPath))
                                && callable.call());
    }

    @Override
    public void close() throws Exception {
        this.lock.close();
    }

    private boolean writeNoOverwriteOrRecover(Snapshot snapshot, Path newSnapshotPath)
            throws IOException {
        try {
            fileIO.writeFile(newSnapshotPath, snapshot.toJson(), false);
            return true;
        } catch (IOException e) {
            if (!isFileAlreadyExists(e)) {
                throw e;
            }
            return recoverExistingSnapshot(snapshot, newSnapshotPath);
        }
    }

    private boolean writeAtomicOrRecover(Snapshot snapshot, Path newSnapshotPath)
            throws IOException {
        boolean committed = fileIO.tryToWriteAtomic(newSnapshotPath, snapshot.toJson());
        if (!committed) {
            if (!fileIO.exists(newSnapshotPath)) {
                throw new IOException(
                        "Commit snapshot "
                                + snapshot.id()
                                + " failed and "
                                + newSnapshotPath
                                + " not found");
            }
            committed = snapshot.equals(Snapshot.fromJson(fileIO.readFileUtf8(newSnapshotPath)));
        }
        return committed;
    }

    private boolean recoverExistingSnapshot(Snapshot snapshot, Path newSnapshotPath)
            throws IOException {
        String existingSnapshotJson;
        try {
            existingSnapshotJson = fileIO.readFileUtf8(newSnapshotPath);
        } catch (IOException | RuntimeException e) {
            throw recoveryRequired(
                    snapshot, newSnapshotPath, "already exists but cannot be read", e);
        }

        Snapshot existingSnapshot;
        try {
            existingSnapshot = Snapshot.fromJson(existingSnapshotJson);
        } catch (RuntimeException e) {
            throw recoveryRequired(snapshot, newSnapshotPath, "already exists but is malformed", e);
        }

        if (snapshot.equals(existingSnapshot)) {
            return true;
        }

        throw recoveryRequired(
                snapshot, newSnapshotPath, "already exists with different content", null);
    }

    private static boolean isFileAlreadyExists(Throwable throwable) {
        while (throwable != null) {
            if (throwable instanceof FileAlreadyExistsException
                    || HADOOP_FILE_ALREADY_EXISTS_EXCEPTION.equals(
                            throwable.getClass().getName())) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private static SnapshotCommitConflictRequiresListRecoveryException recoveryRequired(
            Snapshot snapshot, Path newSnapshotPath, String reason, Throwable cause) {
        return new SnapshotCommitConflictRequiresListRecoveryException(
                "Cannot safely commit snapshot "
                        + snapshot.id()
                        + " because "
                        + newSnapshotPath
                        + " "
                        + reason
                        + "; recovery requires listing to discover the latest committed snapshot.",
                cause);
    }

    /** Signals that snapshot commit conflict recovery requires listing snapshot metadata. */
    public static class SnapshotCommitConflictRequiresListRecoveryException
            extends RuntimeException {

        public SnapshotCommitConflictRequiresListRecoveryException(
                String message, Throwable cause) {
            super(message, cause);
        }
    }
}
