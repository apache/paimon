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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the Frankfurt eu-central-1 regression: when a Paimon table has branches and the REST
 * catalog (Bennett) returns the <b>main</b> table's snapshot for a branch query, {@link
 * SnapshotManager#latestSnapshotId()} returns a snapshot ID that does not exist in the branch's
 * snapshot directory. Any caller that compares {@code latestSnapshotId()} against the actual
 * snapshots on disk (e.g. Morax orphan-clean validation) will fail with "safelyGetAllSnapshots did
 * not include latest snapshot".
 *
 * <p>Root cause: the REST server may not correctly resolve branch-qualified table identifiers
 * (e.g., {@code table$branch_cluster}) and instead returns the main table's snapshot. Paimon's
 * {@link SnapshotManager#latestSnapshotId()} trusts the {@link SnapshotLoader} result without
 * cross-checking the filesystem.
 *
 * <p>Workaround: for non-main branches, use {@link
 * SnapshotManager#latestSnapshotIdFromFileSystem()} to bypass the REST catalog entirely.
 */
public class SnapshotManagerBranchLoaderTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path tablePath;
    private FileStoreTable table;
    private TableWriteImpl<?> write;
    private TableCommitImpl commit;
    private long incrementalIdentifier;

    @BeforeEach
    public void setUp() throws Exception {
        fileIO = LocalFileIO.create();
        tablePath = new Path(tempDir.toString());

        RowType rowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"pk", "part", "value"});

        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        options.set(CoreOptions.BUCKET, 1);

        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(fileIO, tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("part"),
                                Arrays.asList("pk", "part"),
                                options.toMap(),
                                ""));
        table = FileStoreTableFactory.create(fileIO, tablePath, schema);

        String commitUser = UUID.randomUUID().toString();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
        incrementalIdentifier = 0;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (write != null) {
            write.close();
        }
        if (commit != null) {
            commit.close();
        }
    }

    /**
     * Reproduces the Frankfurt regression.
     *
     * <p>Setup: main branch has snapshots 1..10, branch "cluster" was created at snapshot 3 (so the
     * branch directory contains snapshots 1..3).
     *
     * <p>A mock {@link SnapshotLoader} simulates a REST catalog that always returns the main
     * table's latest snapshot (ID=10) regardless of the branch identifier. This is what happens
     * when Bennett does not correctly resolve {@code table$branch_cluster} and falls back to the
     * main table.
     *
     * <p>Expected (bug): {@code latestSnapshotId()} returns 10 (main's), but the branch directory
     * only has snapshots up to 3. The 166944-snapshot gap from Frankfurt (main=166971, branch=27)
     * is the same class of bug at production scale.
     */
    @Test
    public void testLatestSnapshotIdMismatchWithBuggyRestLoader() throws Exception {
        // P0: Write 3 snapshots to main branch
        for (int i = 0; i < 3; i++) {
            writeRow(i, 0, "value_" + i);
        }
        SnapshotManager mainSnapshotManager = table.snapshotManager();
        long branchPointId = mainSnapshotManager.latestSnapshotId();
        assertThat(branchPointId).isGreaterThanOrEqualTo(3L);

        // P1: Create branch "cluster" at the current snapshot via tag
        table.createTag("tag_for_branch", branchPointId);
        table.createBranch("cluster", "tag_for_branch");

        // P2: Write 7 more snapshots to main branch
        for (int i = 3; i < 10; i++) {
            writeRow(i, 0, "value_" + i);
        }
        long mainLatestId = mainSnapshotManager.latestSnapshotId();
        assertThat(mainLatestId).isGreaterThan(branchPointId);

        // P3: Verify branch filesystem has snapshots only up to branchPointId
        SnapshotManager branchFsManager =
                new SnapshotManager(fileIO, tablePath, "cluster", null, null);
        Long branchLatestFromFs = branchFsManager.latestSnapshotIdFromFileSystem();
        assertThat(branchLatestFromFs).isEqualTo(branchPointId);

        List<Snapshot> branchSnapshots = branchFsManager.safelyGetAllSnapshots();
        long maxBranchSnapshotId =
                branchSnapshots.stream().mapToLong(Snapshot::id).max().orElse(-1);
        assertThat(maxBranchSnapshotId).isEqualTo(branchPointId);

        // P4: Simulate the REST catalog bug — create a SnapshotLoader that returns
        // the MAIN table's latest snapshot when asked for the branch's snapshot.
        // This is exactly what happens when Bennett receives
        //   GET /databases/dwd/tables/dwd_flow_sdk_log_i_s%24branch_cluster/snapshot
        // but resolves it to the main table instead of the branch entry.
        Snapshot mainLatestSnapshot = mainSnapshotManager.latestSnapshot();
        assertThat(mainLatestSnapshot.id()).isEqualTo(mainLatestId);

        SnapshotLoader buggyLoader = new BuggyRestSnapshotLoader(mainLatestSnapshot);
        SnapshotManager branchWithBuggyLoader =
                new SnapshotManager(fileIO, tablePath, "cluster", buggyLoader, null);

        // P5: Demonstrate the bug — latestSnapshotId() returns main's ID instead of branch's
        Long latestIdFromLoader = branchWithBuggyLoader.latestSnapshotId();
        assertThat(latestIdFromLoader)
                .as(
                        "Bug: latestSnapshotId() via REST loader returns main table's snapshot ID "
                                + "instead of branch's. This is the root cause of Frankfurt's "
                                + "'safelyGetAllSnapshots did not include latest snapshot' error. "
                                + "main=%d, branch=%d",
                        mainLatestId, branchPointId)
                .isEqualTo(mainLatestId);

        // P6: safelyGetAllSnapshots() reads from the correct branch directory (filesystem)
        List<Snapshot> branchSnapshotsFromBuggyManager =
                branchWithBuggyLoader.safelyGetAllSnapshots();
        long maxIdFromBranchDir =
                branchSnapshotsFromBuggyManager.stream().mapToLong(Snapshot::id).max().orElse(-1);
        assertThat(maxIdFromBranchDir).isEqualTo(branchPointId);

        // P7: The mismatch — latestSnapshotId()=mainLatestId but
        // max(safelyGetAllSnapshots)=branchPointId.
        // Any code that validates "latestSnapshotId must be in safelyGetAllSnapshots" will fail.
        // At Frankfurt scale this was main=166971 vs branch=27.
        assertThat(latestIdFromLoader).isNotEqualTo(maxIdFromBranchDir);

        // P8: Verify the workaround — latestSnapshotIdFromFileSystem() returns the correct value
        Long latestFromFs = branchWithBuggyLoader.latestSnapshotIdFromFileSystem();
        assertThat(latestFromFs)
                .as("Workaround: latestSnapshotIdFromFileSystem() bypasses the REST loader")
                .isEqualTo(branchPointId);
        assertThat(latestFromFs).isEqualTo(maxIdFromBranchDir);
    }

    /**
     * Verifies that when the SnapshotLoader correctly returns the branch's snapshot, there is no
     * mismatch.
     */
    @Test
    public void testLatestSnapshotIdCorrectWithProperBranchLoader() throws Exception {
        // Write 3 snapshots to main, create branch, write 7 more to main
        for (int i = 0; i < 3; i++) {
            writeRow(i, 0, "value_" + i);
        }
        long branchPointId = table.snapshotManager().latestSnapshotId();
        table.createTag("tag_for_branch", branchPointId);
        table.createBranch("cluster", "tag_for_branch");
        for (int i = 3; i < 10; i++) {
            writeRow(i, 0, "value_" + i);
        }

        // Correct loader: returns the branch's actual latest snapshot
        SnapshotManager branchFsManager =
                new SnapshotManager(fileIO, tablePath, "cluster", null, null);
        Snapshot branchLatestSnapshot = branchFsManager.latestSnapshot();
        assertThat(branchLatestSnapshot.id()).isEqualTo(branchPointId);

        SnapshotLoader correctLoader = new BuggyRestSnapshotLoader(branchLatestSnapshot);
        SnapshotManager branchWithCorrectLoader =
                new SnapshotManager(fileIO, tablePath, "cluster", correctLoader, null);

        // No mismatch — latestSnapshotId() matches safelyGetAllSnapshots()
        Long latestIdFromLoader = branchWithCorrectLoader.latestSnapshotId();
        assertThat(latestIdFromLoader).isEqualTo(branchPointId);

        List<Snapshot> branchSnapshots = branchWithCorrectLoader.safelyGetAllSnapshots();
        long maxIdFromBranch = branchSnapshots.stream().mapToLong(Snapshot::id).max().orElse(-1);
        assertThat(latestIdFromLoader).isEqualTo(maxIdFromBranch);
    }

    /**
     * Verifies that copyWithBranch propagates the SnapshotLoader correctly — the new
     * SnapshotLoader's copyWithBranch is called with the target branch name.
     */
    @Test
    public void testCopyWithBranchPropagatesLoader() throws Exception {
        for (int i = 0; i < 3; i++) {
            writeRow(i, 0, "value_" + i);
        }

        SnapshotManager mainManager = table.snapshotManager();
        Snapshot mainSnapshot = mainManager.latestSnapshot();

        // Create a loader that tracks copyWithBranch calls
        TrackingSnapshotLoader trackingLoader = new TrackingSnapshotLoader(mainSnapshot);
        SnapshotManager withLoader =
                new SnapshotManager(fileIO, tablePath, "main", trackingLoader, null);

        // copyWithBranch should create a new SnapshotManager with the loader's copyWithBranch
        // result
        SnapshotManager copied = withLoader.copyWithBranch("cluster");

        // The copied manager should use the new loader (which is a TrackingSnapshotLoader
        // that was created by copyWithBranch with branch="cluster")
        assertThat(trackingLoader.lastCopyBranch).isEqualTo("cluster");
    }

    private void writeRow(int pk, int part, String value) throws Exception {
        InternalRow row =
                GenericRow.ofKind(RowKind.INSERT, pk, part, BinaryString.fromString(value));
        write.write(row);
        List<CommitMessage> messages = write.prepareCommit(false, incrementalIdentifier);
        commit.commit(incrementalIdentifier, messages);
        incrementalIdentifier++;
    }

    /**
     * A SnapshotLoader that always returns a fixed snapshot, simulating a REST catalog (Bennett)
     * that returns the main table's snapshot when queried for a branch's snapshot.
     */
    private static class BuggyRestSnapshotLoader implements SnapshotLoader {
        private static final long serialVersionUID = 1L;
        private final Snapshot fixedSnapshot;

        BuggyRestSnapshotLoader(Snapshot fixedSnapshot) {
            this.fixedSnapshot = fixedSnapshot;
        }

        @Override
        public Optional<Snapshot> load() {
            return Optional.of(fixedSnapshot);
        }

        @Override
        public void rollback(Instant instant) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SnapshotLoader copyWithBranch(String branch) {
            // In the buggy scenario, even after copyWithBranch, the REST catalog
            // still returns the main table's snapshot.
            return new BuggyRestSnapshotLoader(fixedSnapshot);
        }
    }

    /** A SnapshotLoader that tracks copyWithBranch calls for verification. */
    private static class TrackingSnapshotLoader implements SnapshotLoader {
        private static final long serialVersionUID = 1L;
        private final Snapshot snapshot;
        String lastCopyBranch;

        TrackingSnapshotLoader(Snapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public Optional<Snapshot> load() {
            return Optional.of(snapshot);
        }

        @Override
        public void rollback(Instant instant) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SnapshotLoader copyWithBranch(String branch) {
            lastCopyBranch = branch;
            return new TrackingSnapshotLoader(snapshot);
        }
    }
}
