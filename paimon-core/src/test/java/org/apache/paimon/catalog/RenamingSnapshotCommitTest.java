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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
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

        boolean committed = commit.commit(snapshot, "main", Collections.emptyList());

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
                        () -> commit.commit(snapshot, "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("Commit snapshot " + snapshot.id() + " failed")
                .hasMessageContaining("not found");

        // ensure snapshot file is not accidentally created
        assertThat(fileIO.exists(snapshotManager.snapshotPath(snapshot.id()))).isFalse();
    }

    @Test
    public void testObjectStoreCommitDoesNotProbeMissingSnapshot(@TempDir java.nio.file.Path tmp)
            throws Exception {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());
        Snapshot snapshot = createSnapshot(1L);

        boolean committed = commit.commit(snapshot, "main", Collections.emptyList());

        assertThat(committed).isTrue();
        assertThat(fileIO.snapshotExistsCalls).isZero();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.noOverwriteWrites).isEqualTo(1);
        assertThat(Snapshot.fromJson(fileIO.readFileUtf8(snapshotManager.snapshotPath(1L))))
                .isEqualTo(snapshot);
        assertThat(
                        fileIO.readFileUtf8(
                                new Path(
                                        snapshotManager.snapshotDirectory(), HintFileUtils.LATEST)))
                .isEqualTo("1");
    }

    @Test
    public void testObjectStoreCommitConflictRequiresListRecovery(@TempDir java.nio.file.Path tmp)
            throws Exception {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        Snapshot committedSnapshot = createSnapshot(1L, "committed", 1L);
        Snapshot staleSnapshot = createSnapshot(1L, "stale", 2L);
        Path snapshotPath = snapshotManager.snapshotPath(1L);
        fileIO.writeFile(snapshotPath, committedSnapshot.toJson(), false);
        fileIO.resetCounters();

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException ex =
                assertThrows(
                        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException
                                .class,
                        () -> commit.commit(staleSnapshot, "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("Cannot safely commit snapshot 1")
                .hasMessageContaining("already exists with different content")
                .hasMessageContaining("recovery requires listing");
        assertThat(fileIO.snapshotExistsCalls).isZero();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(Snapshot.fromJson(fileIO.readFileUtf8(snapshotPath)))
                .isEqualTo(committedSnapshot);
    }

    @Test
    public void testObjectStoreSameSnapshotContentIsIdempotent(@TempDir java.nio.file.Path tmp)
            throws Exception {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        Snapshot snapshot = createSnapshot(1L);
        Path snapshotPath = snapshotManager.snapshotPath(1L);
        fileIO.writeFile(snapshotPath, snapshot.toJson(), false);
        fileIO.resetCounters();

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        boolean committed = commit.commit(snapshot, "main", Collections.emptyList());

        assertThat(committed).isTrue();
        assertThat(fileIO.snapshotExistsCalls).isZero();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.noOverwriteWrites).isEqualTo(1);
        assertThat(Snapshot.fromJson(fileIO.readFileUtf8(snapshotPath))).isEqualTo(snapshot);
    }

    @Test
    public void testObjectStoreCommitConflictFailsClosedForMalformedExistingSnapshot(
            @TempDir java.nio.file.Path tmp) throws Exception {
        NoListObjectStoreFileIO fileIO = new NoListObjectStoreFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        Path snapshotPath = snapshotManager.snapshotPath(1L);
        fileIO.writeFile(snapshotPath, "not-json", false);
        fileIO.resetCounters();

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException ex =
                assertThrows(
                        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException
                                .class,
                        () -> commit.commit(createSnapshot(1L), "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("already exists but is malformed")
                .hasMessageContaining("recovery requires listing");
        assertThat(fileIO.snapshotExistsCalls).isZero();
        assertThat(fileIO.listStatusCalls).isZero();
        assertThat(fileIO.readFileUtf8(snapshotPath)).isEqualTo("not-json");
    }

    @Test
    public void testObjectStoreCommitConflictFailsClosedForUnreadableExistingSnapshot(
            @TempDir java.nio.file.Path tmp) throws Exception {
        UnreadableSnapshotObjectStoreFileIO fileIO = new UnreadableSnapshotObjectStoreFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        Snapshot committedSnapshot = createSnapshot(1L, "committed", 1L);
        Snapshot staleSnapshot = createSnapshot(1L, "stale", 2L);
        Path snapshotPath = snapshotManager.snapshotPath(1L);
        fileIO.writeFile(snapshotPath, committedSnapshot.toJson(), false);
        fileIO.resetCounters();
        fileIO.denySnapshotReads = true;

        RenamingSnapshotCommit commit = new RenamingSnapshotCommit(snapshotManager, Lock.empty());

        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException ex =
                assertThrows(
                        RenamingSnapshotCommit.SnapshotCommitConflictRequiresListRecoveryException
                                .class,
                        () -> commit.commit(staleSnapshot, "main", Collections.emptyList()));

        assertThat(ex)
                .hasMessageContaining("already exists but cannot be read")
                .hasMessageContaining("recovery requires listing");
        assertThat(fileIO.snapshotExistsCalls).isZero();
        assertThat(fileIO.listStatusCalls).isZero();
        fileIO.denySnapshotReads = false;
        assertThat(Snapshot.fromJson(fileIO.readFileUtf8(snapshotPath)))
                .isEqualTo(committedSnapshot);
    }

    @Test
    public void testBranchCommitUpdatesBranchLatestHint(@TempDir java.nio.file.Path tmp)
            throws Exception {
        FileIO fileIO = new LocalFileIO();
        Path tablePath = new Path(tmp.toUri());
        SnapshotManager mainSnapshotManager =
                new SnapshotManager(fileIO, tablePath, null, null, null);
        SnapshotManager branchSnapshotManager = mainSnapshotManager.copyWithBranch("dev");

        RenamingSnapshotCommit commit =
                new RenamingSnapshotCommit(mainSnapshotManager, Lock.empty());
        Snapshot snapshot = createSnapshot(1L);

        assertThat(commit.commit(snapshot, "dev", Collections.emptyList())).isTrue();

        assertThat(fileIO.exists(branchSnapshotManager.snapshotPath(1L))).isTrue();
        assertThat(fileIO.exists(mainSnapshotManager.snapshotPath(1L))).isFalse();
        assertThat(
                        fileIO.readOverwrittenFileUtf8(
                                new Path(
                                        branchSnapshotManager.snapshotDirectory(),
                                        HintFileUtils.LATEST)))
                .contains("1");
        assertThat(
                        fileIO.readOverwrittenFileUtf8(
                                new Path(
                                        mainSnapshotManager.snapshotDirectory(),
                                        HintFileUtils.LATEST)))
                .isEmpty();
    }

    private static Snapshot createSnapshot(long id) throws IOException {
        return createSnapshot(id, "user", id);
    }

    private static Snapshot createSnapshot(long id, String commitUser, long commitIdentifier)
            throws IOException {
        long schemaId = 1L;
        String baseManifestList = "manifest-list-base";
        String deltaManifestList = "manifest-list-delta";
        String changelogManifestList = null;
        Long baseManifestListSize = 10L;
        Long deltaManifestListSize = 20L;
        Long changelogManifestListSize = null;
        String indexManifest = null;
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
                commitIdentifier,
                commitKind,
                timeMillis,
                totalRecordCount,
                deltaRecordCount,
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

    private static class NoListObjectStoreFileIO extends LocalFileIO {

        int snapshotExistsCalls;
        int listStatusCalls;
        int noOverwriteWrites;

        @Override
        public boolean isObjectStore() {
            return true;
        }

        @Override
        public boolean supportsAtomicCreateWithoutOverwrite(Path path) {
            return isSnapshotFile(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            if (isSnapshotFile(path)) {
                snapshotExistsCalls++;
                throw new AccessDeniedException(path.toString());
            }
            return super.exists(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            listStatusCalls++;
            throw new AccessDeniedException(path.toString());
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            if (!isSnapshotFile(path)) {
                return super.newOutputStream(path, overwrite);
            }

            if (!overwrite) {
                noOverwriteWrites++;
            }

            java.nio.file.Path localPath = java.nio.file.Paths.get(path.toUri());
            java.nio.file.Path parent = localPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            OutputStream out =
                    overwrite
                            ? Files.newOutputStream(
                                    localPath,
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.TRUNCATE_EXISTING,
                                    StandardOpenOption.WRITE)
                            : Files.newOutputStream(
                                    localPath,
                                    StandardOpenOption.CREATE_NEW,
                                    StandardOpenOption.WRITE);
            return new OutputStreamPositionOutputStream(out);
        }

        void resetCounters() {
            snapshotExistsCalls = 0;
            listStatusCalls = 0;
            noOverwriteWrites = 0;
        }

        protected boolean isSnapshotFile(Path path) {
            Path parent = path.getParent();
            if (parent == null || !"snapshot".equals(parent.getName())) {
                return false;
            }

            String fileName = path.getName();
            return fileName.startsWith("snapshot-")
                    || HintFileUtils.LATEST.equals(fileName)
                    || HintFileUtils.EARLIEST.equals(fileName);
        }
    }

    private static class UnreadableSnapshotObjectStoreFileIO extends NoListObjectStoreFileIO {

        boolean denySnapshotReads;

        @Override
        public String readFileUtf8(Path path) throws IOException {
            if (denySnapshotReads
                    && isSnapshotFile(path)
                    && path.getName().startsWith("snapshot-")) {
                throw new AccessDeniedException(path.toString());
            }
            return super.readFileUtf8(path);
        }
    }

    private static class OutputStreamPositionOutputStream extends PositionOutputStream {

        private final OutputStream out;
        private long pos;

        OutputStreamPositionOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            pos += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
