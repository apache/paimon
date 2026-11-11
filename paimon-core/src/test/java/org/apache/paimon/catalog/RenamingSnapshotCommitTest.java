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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link RenamingSnapshotCommit} to verify robustness when object storage rename returns
 * false while the target file is actually created, and to verify failure when target is missing.
 */
public class RenamingSnapshotCommitTest {

    /**
     * Simulate the object storage scenario where rename actually succeeds but returns false, which
     * causes {@link FileIO#tryToWriteAtomic(Path, String)} to return false while the target file
     * exists with correct content.
     */
    @Test
    public void testCommitSucceedsAndRenameReturnsFalse(@TempDir java.nio.file.Path tmp)
            throws Exception {
        FileIO fileIO = new AlwaysFalseRenameLocalFileIO(true);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(1L);

        boolean committed = commit.commit(null, snapshot, "main", Collections.emptyList());

        assertThat(committed).isTrue();

        // snapshot file should exist and match content
        Path snapshotPath = snapshotManager.snapshotPath(snapshot.id());
        assertThat(fileIO.exists(snapshotPath)).isTrue();
        Snapshot loaded = Snapshot.fromJson(fileIO.readFileUtf8(snapshotPath));
        assertThat(loaded).isEqualTo(snapshot);

        // LATEST hint should be committed
        Path latestHint = new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST);
        assertThat(fileIO.readOverwrittenFileUtf8(latestHint).orElse(null))
                .isEqualTo(String.valueOf(snapshot.id()));
    }

    /**
     * Simulate the scenario where rename fails and the target snapshot file is not created; the
     * commit should throw IOException according to the new check.
     */
    @Test
    public void testCommitTargetSnapshotMissing(@TempDir java.nio.file.Path tmp) throws Exception {
        FileIO fileIO = new AlwaysFalseRenameLocalFileIO(false);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(2L);

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> commit.commit(null, snapshot, "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("Commit snapshot " + snapshot.id() + " failed")
                .hasMessageContaining("not found");

        // ensure snapshot file is not accidentally created
        assertThat(fileIO.exists(snapshotManager.snapshotPath(snapshot.id()))).isFalse();
    }

    /**
     * Simulate the object storage scenario where rename actually succeeds but throws, for example
     * when the response of a successful copy cannot be parsed. The commit should succeed and write
     * the LATEST hint.
     */
    @Test
    public void testCommitSucceedsAndRenameThrows(@TempDir java.nio.file.Path tmp)
            throws Exception {
        FileIO fileIO = new ThrowingRenameLocalFileIO(true);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(3L);

        boolean committed = commit.commit(null, snapshot, "main", Collections.emptyList());

        assertThat(committed).isTrue();
        Path latestHint = new Path(snapshotManager.snapshotDirectory(), HintFileUtils.LATEST);
        assertThat(fileIO.readOverwrittenFileUtf8(latestHint).orElse(null))
                .isEqualTo(String.valueOf(snapshot.id()));
    }

    /** When rename throws and the target snapshot file is not created, the error is rethrown. */
    @Test
    public void testCommitRenameThrowsAndTargetMissing(@TempDir java.nio.file.Path tmp)
            throws Exception {
        FileIO fileIO = new ThrowingRenameLocalFileIO(false);
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        Snapshot snapshot = createSnapshot(4L);

        IOException ex =
                assertThrows(
                        IOException.class,
                        () -> commit.commit(null, snapshot, "main", Collections.emptyList()));

        assertThat(ex).hasMessageContaining(ThrowingRenameLocalFileIO.MESSAGE);
        assertThat(fileIO.exists(snapshotManager.snapshotPath(snapshot.id()))).isFalse();
    }

    /**
     * The lease of the commit lock expires while the commit checks whether the snapshot file
     * already exists. The commit must not publish the snapshot after that.
     */
    @Test
    public void testCommitRechecksLeaseAfterCheckingSnapshot(@TempDir java.nio.file.Path tmp)
            throws Exception {
        AtomicBoolean expired = new AtomicBoolean();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager =
                new SnapshotManager(new LocalFileIO(), tablePath, null, null, null);
        Path snapshotPath = snapshotManager.snapshotPath(5L);
        FileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public boolean exists(Path path) throws IOException {
                        boolean exists = super.exists(path);
                        if (path.equals(snapshotPath)) {
                            expired.set(true);
                        }
                        return exists;
                    }
                };
        snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        Lock lock =
                new Lock() {
                    @Override
                    public AutoCloseable acquire() {
                        return () -> {};
                    }

                    @Override
                    public void ensureValid() {
                        if (expired.get()) {
                            throw new IllegalStateException("Lease expired");
                        }
                    }

                    @Override
                    public void close() {}
                };

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, lock);
        Snapshot snapshot = createSnapshot(5L);
        // thrown by the check in commitImpl, not wrapped like a failure to acquire the lock
        assertThatThrownBy(() -> commit.commit(null, snapshot, "main", Collections.emptyList()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Lease expired");

        assertThat(new LocalFileIO().exists(snapshotPath)).isFalse();
    }

    /**
     * Two writers share a lease lock on a file system whose rename overwrites the target, as some
     * object stores do. Writer A's lease expires while it checks whether snapshot 2 exists, and
     * writer B, which gets the lock then, commits snapshot 2. Writer A must not overwrite it.
     */
    @Test
    public void testExpiredLeaseDoesNotOverwriteAnotherWritersSnapshot(
            @TempDir java.nio.file.Path tmp) throws Exception {
        Path tablePath = new Path(tmp.toUri());
        OverwritingRenameLocalFileIO fileIO = new OverwritingRenameLocalFileIO();
        new FileSystemSchemaManager(fileIO, tablePath)
                .createTable(Schema.newBuilder().column("id", DataTypes.INT()).build());
        LeaseLockFactory locks = new LeaseLockFactory();
        FileStoreTable writerA = createTable(fileIO, tablePath, locks);
        FileStoreTable writerB = createTable(fileIO, tablePath, locks);
        commitRow(writerA, 1);

        fileIO.onCommitExists(
                writerA.snapshotManager().snapshotPath(2),
                () -> {
                    locks.expireHolder();
                    Thread writer = new Thread(() -> commitRow(writerB, 2));
                    writer.start();
                    writer.join();
                });
        commitRow(writerA, 3);

        assertThat(readIds(FileStoreTableFactory.create(fileIO, tablePath)))
                .containsExactlyInAnyOrder(1, 2, 3);
    }

    private static FileStoreTable createTable(
            FileIO fileIO, Path tablePath, CatalogLockFactory lockFactory) {
        CatalogEnvironment env =
                new CatalogEnvironment(
                        Identifier.create("db", "t"),
                        null,
                        null,
                        lockFactory,
                        null,
                        null,
                        false,
                        false);
        return FileStoreTableFactory.create(
                fileIO,
                tablePath,
                new FileSystemSchemaManager(fileIO, tablePath).latest().get(),
                env);
    }

    private static void commitRow(FileStoreTable table, int id) {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(id));
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Integer> readIds(FileStoreTable table) throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }
        return ids;
    }

    private static Snapshot createSnapshot(long id) throws IOException {
        long schemaId = 1L;
        String baseManifestList = "manifest-list-base";
        String deltaManifestList = "manifest-list-delta";
        String changelogManifestList = null;
        Long baseManifestListSize = 10L;
        Long deltaManifestListSize = 20L;
        Long changelogManifestListSize = null;
        String indexManifest = null;
        String commitUser = "user";
        long commitIdentifier = id;
        Snapshot.CommitKind commitKind = Snapshot.CommitKind.APPEND;
        long timeMillis = System.currentTimeMillis();
        Long totalRecordCount = 100L;
        Long deltaRecordCount = 100L;

        return new Snapshot(
                id,
                schemaId,
                baseManifestList,
                baseManifestListSize,
                deltaManifestList,
                deltaManifestListSize,
                changelogManifestList,
                changelogManifestListSize,
                indexManifest,
                commitUser,
                null,
                commitIdentifier,
                commitKind,
                timeMillis,
                totalRecordCount,
                deltaRecordCount,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * A FileIO based on LocalFileIO that always returns false from rename().
     *
     * <p>When {@code actuallyMove} is true, it performs the move then returns false; otherwise it
     * returns false without moving, simulating a failed rename.
     */
    private static class AlwaysFalseRenameLocalFileIO extends LocalFileIO {

        private final boolean actuallyMove;

        AlwaysFalseRenameLocalFileIO(boolean actuallyMove) {
            this.actuallyMove = actuallyMove;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            if (actuallyMove) {
                // perform actual move to create target, but report failure
                boolean ignored = super.rename(src, dst);
                return false;
            } else {
                // do not move, report failure
                // ensure parent directory exists to avoid side effects in tests
                java.nio.file.Path parent = java.nio.file.Paths.get(dst.toUri());
                Files.createDirectories(parent.getParent());
                return false;
            }
        }
    }

    /**
     * A LocalFileIO whose rename replaces an existing target, and that can run an action once when
     * a snapshot commit checks whether a file exists.
     */
    private static class OverwritingRenameLocalFileIO extends LocalFileIO {

        private volatile Path trigger;
        private volatile ThrowingRunnable action;

        void onCommitExists(Path path, ThrowingRunnable action) {
            this.action = action;
            this.trigger = path;
        }

        @Override
        public boolean exists(Path path) throws IOException {
            boolean exists = super.exists(path);
            if (path.equals(trigger) && calledFromCommitImpl()) {
                trigger = null;
                try {
                    action.run();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            return exists;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            java.nio.file.Path target = java.nio.file.Paths.get(dst.toUri());
            Files.createDirectories(target.getParent());
            Files.move(
                    java.nio.file.Paths.get(src.toUri()),
                    target,
                    StandardCopyOption.REPLACE_EXISTING);
            return true;
        }

        private static boolean calledFromCommitImpl() {
            for (StackTraceElement e : Thread.currentThread().getStackTrace()) {
                if (e.getClassName().startsWith(RenamingSnapshotCommit.class.getName())
                        && e.getMethodName().equals("commitImpl")) {
                    return true;
                }
            }
            return false;
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Hands out lease locks that exclude each other. {@link #expireHolder} ends the current
     * holder's lease, so another lock can be acquired and the holder fails {@code ensureValid}.
     */
    private static class LeaseLockFactory implements CatalogLockFactory {

        private final AtomicReference<Lock> holder = new AtomicReference<>();

        void expireHolder() {
            holder.set(null);
        }

        @Override
        public String identifier() {
            return "lease";
        }

        @Override
        public Lock createLock(
                CatalogLockContext context,
                Identifier identifier,
                @Nullable String tableUuid,
                @Nullable String commitUser,
                Options tableOptions) {
            return new Lock() {
                @Override
                public AutoCloseable acquire() {
                    if (!holder.compareAndSet(null, this)) {
                        throw new IllegalStateException("The lock is held by another writer");
                    }
                    return () -> holder.compareAndSet(this, null);
                }

                @Override
                public void ensureValid() {
                    if (holder.get() != this) {
                        throw new IllegalStateException("Lease expired");
                    }
                }

                @Override
                public void close() {}
            };
        }
    }

    /**
     * A FileIO based on LocalFileIO that always throws from rename().
     *
     * <p>When {@code actuallyMove} is true, it performs the move then throws; otherwise it throws
     * without moving.
     */
    private static class ThrowingRenameLocalFileIO extends LocalFileIO {

        private static final String MESSAGE = "Failed to parse the rename response";

        private final boolean actuallyMove;

        ThrowingRenameLocalFileIO(boolean actuallyMove) {
            this.actuallyMove = actuallyMove;
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            if (actuallyMove) {
                super.rename(src, dst);
            }
            throw new IOException(MESSAGE);
        }
    }
}
