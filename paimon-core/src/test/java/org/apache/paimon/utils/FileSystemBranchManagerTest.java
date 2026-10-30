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
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link FileSystemBranchManager}. */
class FileSystemBranchManagerTest {

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;
    private FileIO fileIO;
    private SnapshotManager snapshotManager;
    private TagManager tagManager;
    private SchemaManager schemaManager;
    private FileSystemBranchManager branchManager;

    @BeforeEach
    void before() throws Exception {
        tablePath = new Path(tempDir.toUri().toString());
        fileIO = FileIOFinder.find(tablePath);

        // Create schema
        Schema schema =
                Schema.newBuilder()
                        .column("product_id", DataTypes.INT())
                        .column("price", DataTypes.INT())
                        .column("sales", DataTypes.INT())
                        .primaryKey("product_id")
                        .build();

        // Create schema manager and initialize schema
        schemaManager = new FileSystemSchemaManager(fileIO, tablePath);
        schemaManager.createTable(schema);

        // Create snapshot, tag, and branch managers
        snapshotManager = new SnapshotManager(fileIO, tablePath, null, null, null);
        tagManager = new TagManager(fileIO, tablePath);
        branchManager =
                new FileSystemBranchManager(
                        fileIO, tablePath, snapshotManager, tagManager, schemaManager, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeleteMainBranchPreservesTable(boolean useTableApi) throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        FileStoreTable table = FileStoreTableFactory.create(fileIO, tablePath);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, 10, 100));
            commit.commit(write.prepareCommit());
        }
        table.createTag("tag1", 1);
        table.createBranch("branch1", "tag1");
        Set<java.nio.file.Path> retainedFiles;
        try (Stream<java.nio.file.Path> files = Files.walk(tempDir)) {
            retainedFiles = files.filter(Files::isRegularFile).collect(Collectors.toSet());
        }

        assertThatThrownBy(
                        () -> {
                            if (useTableApi) {
                                table.deleteBranch(DEFAULT_MAIN_BRANCH);
                            } else {
                                branchManager.dropBranch(DEFAULT_MAIN_BRANCH);
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The main branch cannot be deleted.");

        try (Stream<java.nio.file.Path> files = Files.walk(tempDir)) {
            assertThat(files.filter(Files::isRegularFile).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrderElementsOf(retainedFiles);
        }
        FileStoreTable reloaded = FileStoreTableFactory.create(fileIO, tablePath);
        assertThat(reloaded.snapshotManager().snapshotExists(1)).isTrue();
        assertThat(reloaded.tagManager().tagExists("tag1")).isTrue();
        assertThat(reloaded.branchManager().branchExists("branch1")).isTrue();
        assertThat(
                        getResult(
                                reloaded.newRead(),
                                reloaded.newReadBuilder().newScan().plan().splits(),
                                row -> row.getInt(0) + "," + row.getInt(1) + "," + row.getInt(2)))
                .containsExactly("1,10,100");
    }

    @Test
    void testRenameBranchBasic() {
        // Create a branch
        branchManager.createBranch("old_branch");
        assertThat(branchManager.branchExists("old_branch")).isTrue();

        // Rename the branch
        branchManager.renameBranch("old_branch", "new_branch");

        // Verify old branch no longer exists
        assertThat(branchManager.branchExists("old_branch")).isFalse();

        // Verify new branch exists
        assertThat(branchManager.branchExists("new_branch")).isTrue();

        // Verify branches list contains new branch
        List<String> branches = branchManager.branches();
        assertThat(branches).contains("new_branch");
        assertThat(branches).doesNotContain("old_branch");
    }

    @Test
    void testRenameNonExistentBranch() {
        // Try to rename non-existent branch should throw exception
        assertThatThrownBy(() -> branchManager.renameBranch("non_existent", "new_branch"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("doesn't exist");
    }

    @Test
    void testRenameToExistingBranch() {
        // Create two branches
        branchManager.createBranch("branch1");
        branchManager.createBranch("branch2");

        // Try to rename to existing branch should throw exception
        assertThatThrownBy(() -> branchManager.renameBranch("branch1", "branch2"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void testRenameMainBranchShouldFail() {
        // Try to rename main branch should throw exception
        assertThatThrownBy(() -> branchManager.renameBranch("main", "renamed_main"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("main");
    }

    @Test
    void testRenameBranchFromTag() {
        // Create a tag
        branchManager.createBranch("old_branch");

        // Rename branch created from tag
        branchManager.renameBranch("old_branch", "new_branch");

        // Verify new branch exists
        assertThat(branchManager.branchExists("new_branch")).isTrue();
        assertThat(branchManager.branchExists("old_branch")).isFalse();
    }

    @Test
    void testRenameBranchPreservesData() {
        // Create a branch
        branchManager.createBranch("test_branch");
        assertThat(branchManager.branchExists("test_branch")).isTrue();

        // Rename the branch
        branchManager.renameBranch("test_branch", "renamed_branch");

        // Verify the renamed branch exists and the original does not
        assertThat(branchManager.branchExists("test_branch")).isFalse();
        assertThat(branchManager.branchExists("renamed_branch")).isTrue();
    }

    @Test
    void testRenameBranchMultipleTimes() {
        // Create a branch
        branchManager.createBranch("branch1");

        // Rename multiple times
        branchManager.renameBranch("branch1", "branch2");
        branchManager.renameBranch("branch2", "branch3");

        // Verify final state
        assertThat(branchManager.branchExists("branch1")).isFalse();
        assertThat(branchManager.branchExists("branch2")).isFalse();
        assertThat(branchManager.branchExists("branch3")).isTrue();

        List<String> branches = branchManager.branches();
        assertThat(branches).contains("branch3");
        assertThat(branches).doesNotContain("branch1", "branch2");
    }

    @Test
    void testRenameBranchValidatesBranchNames() {
        // Try to rename with invalid target branch name
        branchManager.createBranch("valid_branch");

        // Test numeric branch name
        assertThatThrownBy(() -> branchManager.renameBranch("valid_branch", "123"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("pure numeric");

        // Test blank branch name
        assertThatThrownBy(() -> branchManager.renameBranch("valid_branch", ""))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void testRenameBranchToSameName() {
        // Create a branch
        branchManager.createBranch("same_name");

        // Try to rename to the same name should throw exception
        assertThatThrownBy(() -> branchManager.renameBranch("same_name", "same_name"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("already exists");
    }

    @Test
    void testFailedFastForwardKeepsMainSnapshots() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);

        io.failSnapshotWrite(table.snapshotManager().snapshotDirectory(), 0);
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Injected write failure");

        // Snapshot 2 was committed to main before the branch existed; a failed fast-forward must
        // not take it away.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        assertThat(readRows(reloaded)).containsExactlyInAnyOrder("1,10,100", "2,20,200");

        io.watch(table.snapshotManager().snapshotDirectory(), "snapshot-");
        table.fastForward("b1");
        assertThat(io.changes).containsExactly("write snapshot-3", "write snapshot-4");
        assertThat(readRows(FileStoreTableFactory.create(io, tablePath)))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    @Test
    void testFastForwardResumesAfterPartialFailure() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);

        io.failSnapshotWrite(table.snapshotManager().snapshotDirectory(), 1);
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Injected write failure");

        // Only the first of the branch's snapshots reached main, and main reads it consistently.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(3L);
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300");

        // The retry continues where the failed run stopped instead of replacing main.
        io.watch(table.snapshotManager().snapshotDirectory(), "snapshot-");
        table.fastForward("b1");
        assertThat(io.changes).containsExactly("write snapshot-4");
        reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(4L);
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    @Test
    void testFastForwardAfterMainDivergedReplacesMain() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        commitRow(table, 5);

        table.fastForward("b1");

        // Main moved on after the branch was created, so fast-forward resets main to the branch
        // as it always has, including main's own snapshot 3.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().snapshot(3))
                .isEqualTo(table.switchToBranch("b1").snapshotManager().snapshot(3));
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    @Test
    void testFailedReplaceKeepsMainSchemas() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        commitRow(table, 5);

        io.failSnapshotWrite(table.snapshotManager().snapshotDirectory(), 0);
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Injected write failure");

        // Replacing main dropped its snapshots from 2 on, but snapshot 1 keeps its schema.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.schemaManager().listAllIds()).containsExactlyInAnyOrder(0L, 1L);
        assertThat(readRows(reloaded)).containsExactly("1,10,100");

        table.fastForward("b1");
        assertThat(readRows(FileStoreTableFactory.create(io, tablePath)))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    @Test
    void testFailedSchemaReplaceKeepsMainReadable() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        // Both sides create a schema 2, so replacing main rewrites main's schema 2.
        table.schemaManager().commitChanges(SchemaChange.addColumn("m_col", DataTypes.INT()));
        table.switchToBranch("b1")
                .schemaManager()
                .commitChanges(SchemaChange.addColumn("b_col", DataTypes.INT()));
        commitWideRow(table.switchToBranch("b1"), 5);

        io.failWrite(table.schemaManager().schemaDirectory(), "schema-", 0);
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Injected write failure");

        // Snapshot 1, which main keeps, still has its schema.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.schemaManager().listAllIds()).contains(0L, 1L);
        assertThat(readRows(reloaded)).containsExactly("1,10,100");

        table.fastForward("b1");
        reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.schema().fieldNames()).contains("b_col").doesNotContain("m_col");
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder(
                        "1,10,100", "2,20,200", "3,30,300", "4,40,400", "5,50,500");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReplaceMovesEarliestHintBack(boolean failFirstCopy) throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        commitRow(table, 5);
        expireAllButLatest(table);
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(3L);

        if (failFirstCopy) {
            // Replacing main copies the branch's snapshot 2, then fails on snapshot 3. The retry
            // sees main as a prefix of the branch and only appends.
            io.failSnapshotWrite(table.snapshotManager().snapshotDirectory(), 1);
            assertThatThrownBy(() -> table.fastForward("b1"))
                    .hasStackTraceContaining("Injected write failure");
            io.watch(table.snapshotManager().snapshotDirectory(), "snapshot-");
        }
        table.fastForward("b1");
        if (failFirstCopy) {
            assertThat(io.changes).containsExactly("write snapshot-3", "write snapshot-4");
        }

        // Main is the branch from snapshot 2 on, so 2 is its earliest snapshot again; otherwise
        // expiration would never reach snapshot 2.
        assertThat(
                        FileStoreTableFactory.create(io, tablePath)
                                .snapshotManager()
                                .earliestSnapshotId())
                .isEqualTo(2L);
    }

    @ParameterizedTest
    @ValueSource(strings = {"latest-missing", "gap", "truncated-latest", "truncated-middle"})
    void testFastForwardRepairsInterruptedReplace(String state) throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        SnapshotManager main = table.snapshotManager();
        SnapshotManager branch = table.switchToBranch("b1").snapshotManager();
        // What an older version's replace leaves in main's snapshot directory when it fails: the
        // branch's LATEST hint copied first, then some of the branch's snapshot files in listing
        // order, which is not id order once ids reach 10. Only the snapshot directory is set up.
        main.commitLatestHint(4);
        copySnapshot(io, branch.snapshotPath(3), main.snapshotPath(3), state, "middle", "gap");
        copySnapshot(io, branch.snapshotPath(4), main.snapshotPath(4), state, "latest", "missing");

        table.fastForward("b1");

        for (long id = 2; id <= 4; id++) {
            assertThat(readBytes(io, main.snapshotPath(id)))
                    .isEqualTo(readBytes(io, branch.snapshotPath(id)));
        }
        assertThat(readRows(FileStoreTableFactory.create(io, tablePath)))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    /**
     * Copy a snapshot file byte for byte, as an older replace did, unless {@code state} names it
     * truncated (then write its first half) or absent.
     */
    private static void copySnapshot(
            FileIO io, Path source, Path target, String state, String truncatedAs, String absentAs)
            throws IOException {
        if (state.endsWith(absentAs)) {
            return;
        }
        byte[] bytes = readBytes(io, source);
        int length = state.equals("truncated-" + truncatedAs) ? bytes.length / 2 : bytes.length;
        try (PositionOutputStream out = io.newOutputStream(target, false)) {
            out.write(bytes, 0, length);
        }
    }

    private static byte[] readBytes(FileIO io, Path path) throws IOException {
        return IOUtils.readFully(io.newInputStream(path), true);
    }

    @Test
    void testFastForwardCopiesBranchSchemas() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        table.switchToBranch("b1")
                .schemaManager()
                .commitChanges(SchemaChange.addColumn("b_col", DataTypes.INT()));
        commitWideRow(table.switchToBranch("b1"), 5);

        table.fastForward("b1");

        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.schema().fieldNames()).contains("b_col");
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder(
                        "1,10,100", "2,20,200", "3,30,300", "4,40,400", "5,50,500");
    }

    @Test
    void testFastForwardAfterMainSchemaChangeReplacesMain() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        // Both sides create a schema with the same id after the branch was created.
        table.schemaManager().commitChanges(SchemaChange.addColumn("m_col", DataTypes.INT()));
        table.switchToBranch("b1")
                .schemaManager()
                .commitChanges(SchemaChange.addColumn("b_col", DataTypes.INT()));
        commitWideRow(table.switchToBranch("b1"), 5);

        table.fastForward("b1");

        // Main's own schema must not be kept under the id the branch's snapshots refer to.
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.schema().fieldNames()).contains("b_col").doesNotContain("m_col");
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder(
                        "1,10,100", "2,20,200", "3,30,300", "4,40,400", "5,50,500");
    }

    @Test
    void testFastForwardStopsWhenLockLeaseExpires() throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        InterceptingFileIO io = new InterceptingFileIO();
        HeldFlagLockFactory lockFactory = new HeldFlagLockFactory();
        FileStoreTable table = createTableWithLock(io, lockFactory);
        commitRow(table, 1);
        table.createTag("tag1", 1);
        table.createBranch("b1", "tag1");
        FileStoreTable branch = table.switchToBranch("b1");
        commitRow(branch, 2);
        commitRow(branch, 3);

        // The lease expires once the first of the branch's snapshots reaches main.
        io.watch(table.snapshotManager().snapshotDirectory(), "snapshot-");
        io.afterWatchedWrite = () -> lockFactory.expired.set(true);
        assertThatThrownBy(() -> table.fastForward("b1")).hasStackTraceContaining("Lease expired");

        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        assertThat(reloaded.snapshotManager().snapshotExists(3)).isFalse();
    }

    @Test
    void testFastForwardRechecksLeaseAfterReadingSource() throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        InterceptingFileIO io = new InterceptingFileIO();
        HeldFlagLockFactory lockFactory = new HeldFlagLockFactory();
        FileStoreTable table = createTableWithLock(io, lockFactory);
        commitRow(table, 1);
        table.createTag("tag1", 1);
        table.createBranch("b1", "tag1");
        FileStoreTable branch = table.switchToBranch("b1");
        commitRow(branch, 2);

        // The lease expires while the branch's snapshot 2 is read, before main gets a copy.
        io.onOpen(branch.snapshotManager().snapshotPath(2), () -> lockFactory.expired.set(true));
        assertThatThrownBy(() -> table.fastForward("b1")).hasStackTraceContaining("Lease expired");

        assertThat(io.exists(table.snapshotManager().snapshotPath(2))).isFalse();
    }

    @Test
    void testReplaceStopsDeletingWhenLeaseExpires() throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        InterceptingFileIO io = new InterceptingFileIO();
        HeldFlagLockFactory lockFactory = new HeldFlagLockFactory();
        FileStoreTable table = createTableWithLock(io, lockFactory);
        commitRow(table, 1);
        table.createTag("tag1", 1);
        table.createBranch("b1", "tag1");
        commitRow(table.switchToBranch("b1"), 2);
        commitRow(table, 2);
        commitRow(table, 3);

        // Replacing the diverged main deletes its snapshots 1 to 3; the lease expires after the
        // first delete.
        Path snapshotDirectory = table.snapshotManager().snapshotDirectory();
        io.watch(snapshotDirectory, "snapshot-");
        io.afterWatchedDelete = () -> lockFactory.expired.set(true);
        assertThatThrownBy(() -> table.fastForward("b1")).hasStackTraceContaining("Lease expired");

        assertThat(io.changes).containsExactly("delete snapshot-1");
    }

    @Test
    void testReplaceDeletesTagsBeforeSnapshots() throws Exception {
        schemaManager.commitChanges(
                SchemaChange.setOption(BUCKET.key(), "1"),
                SchemaChange.setOption(CoreOptions.WRITE_ONLY.key(), "true"));
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = FileStoreTableFactory.create(io, tablePath);
        for (int i = 1; i <= 9; i++) {
            commitRow(table, i);
        }
        table.createTag("tag9", 9);
        table.createBranch("b1", "tag9");
        commitRow(table.switchToBranch("b1"), 10);
        // Main's own snapshots 10 and 11, with a tag on 11.
        commitRow(table, 20);
        commitRow(table, 21);
        table.createTag("mtag", 11);

        // Replacing main deletes snapshot-10 and snapshot-11 before snapshot-9 in name order, and
        // fails on snapshot-9. Main is then a prefix of the branch, so the retry only appends.
        io.failDeleteOf(table.snapshotManager().snapshotPath(9));
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Failed to delete");
        assertThat(table.snapshotManager().snapshotExists(9)).isTrue();
        assertThat(table.snapshotManager().snapshotExists(10)).isFalse();

        table.fastForward("b1");
        assertThat(table.tagManager().tagExists("mtag")).isFalse();
        assertThat(table.tagManager().tagExists("tag9")).isTrue();
    }

    @Test
    void testReplaceFailsWhenSnapshotCannotBeDeleted() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        for (int i = 5; i <= 7; i++) {
            commitRow(table, i);
        }
        SnapshotManager main = table.snapshotManager();
        long mainLatest = main.latestSnapshotId();
        assertThat(mainLatest).isGreaterThan(4L);

        // Main's own latest snapshot is above all of the branch's. If it stayed, main would go back
        // to it once commits fill the gap below it.
        io.failDeleteOf(main.snapshotPath(mainLatest));
        assertThatThrownBy(() -> table.fastForward("b1"))
                .hasStackTraceContaining("Failed to delete");

        table.fastForward("b1");
        FileStoreTable reloaded = FileStoreTableFactory.create(io, tablePath);
        assertThat(reloaded.snapshotManager().latestSnapshotId()).isEqualTo(4L);
        assertThat(readRows(reloaded))
                .containsExactlyInAnyOrder("1,10,100", "2,20,200", "3,30,300", "4,40,400");
    }

    @Test
    void testFastForwardReplacesDivergentSnapshotOfSameLength() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        SnapshotManager main = table.snapshotManager();
        SnapshotManager branch = table.switchToBranch("b1").snapshotManager();
        // What an older version's replace leaves when it failed to delete main's own snapshot 3,
        // copied the branch's snapshot 4 and then stopped before snapshot 3.
        commitRow(table, 5);
        io.copyFile(branch.snapshotPath(4), main.snapshotPath(4), false);
        main.commitLatestHint(4);
        assertThat(io.getFileSize(main.snapshotPath(3)))
                .isEqualTo(io.getFileSize(branch.snapshotPath(3)));

        table.fastForward("b1");

        assertThat(readBytes(io, main.snapshotPath(3)))
                .isEqualTo(readBytes(io, branch.snapshotPath(3)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFastForwardRunsUnderCommitLock(boolean mainDiverged) throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        InterceptingFileIO io = new InterceptingFileIO();
        HeldFlagLockFactory lockFactory = new HeldFlagLockFactory();
        FileStoreTable table = createTableWithLock(io, lockFactory);
        Path snapshotDirectory = table.snapshotManager().snapshotDirectory();
        io.watch(snapshotDirectory, "snapshot-");
        io.lockHeld = lockFactory::isHeld;

        // A commit publishes its main snapshot under the lock, which shows the recording works.
        commitRow(table, 1);
        assertThat(io.lockHeldOnChanges).isNotEmpty().containsOnly(true);
        Identifier mainCommitLock = lockFactory.lockedIdentifiers.get(0);

        table.createTag("tag1", 1);
        table.createBranch("b1", "tag1");
        commitRow(table.switchToBranch("b1"), 2);
        if (mainDiverged) {
            commitRow(table, 3);
        }
        io.watch(snapshotDirectory, "snapshot-");
        lockFactory.lockedIdentifiers.clear();

        table.fastForward("b1");
        assertThat(io.lockHeldOnChanges).isNotEmpty().containsOnly(true);
        // Replacing a diverged main deletes its snapshots, which must also happen under the lock.
        assertThat(io.changes.stream().anyMatch(change -> change.startsWith("delete")))
                .isEqualTo(mainDiverged);
        assertThat(lockFactory.lockedIdentifiers).containsOnly(mainCommitLock);
    }

    @Test
    void testFastForwardCopiesSchemasInIdOrder() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        // Schemas 2 to 10, which a name-ordered listing returns as 10, 2, 3, ...
        for (int i = 2; i <= 10; i++) {
            table.switchToBranch("b1")
                    .schemaManager()
                    .commitChanges(SchemaChange.updateComment("comment " + i));
        }
        commitRow(table.switchToBranch("b1"), 5);

        io.watch(table.schemaManager().schemaDirectory(), "schema-");
        table.fastForward("b1");

        // In id order, a failure part way leaves main with no missing schema id below its latest.
        List<String> expected = new ArrayList<>();
        for (int i = 2; i <= 10; i++) {
            expected.add("write schema-" + i);
        }
        assertThat(io.changes).containsExactlyElementsOf(expected);
    }

    @Test
    void testFastForwardKeepsMainTagsUnchanged() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        table.switchToBranch("b1").createTag("btag", 3);

        io.watch(table.tagManager().tagDirectory(), "tag-");
        table.fastForward("b1");

        // tag2, which main and the branch share, is not rewritten.
        assertThat(io.changes).containsExactly("write tag-btag");
    }

    @Test
    void testFastForwardAppendsAfterMainExpiredSharedSnapshots() throws Exception {
        InterceptingFileIO io = new InterceptingFileIO();
        FileStoreTable table = createTableWithBranchAhead(io);
        table.fastForward("b1");
        expireAllButLatest(table);
        assertThat(table.snapshotManager().earliestSnapshotId()).isEqualTo(4L);
        commitRow(table.switchToBranch("b1"), 5);
        assertThat(table.switchToBranch("b1").snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(Snapshot.CommitKind.COMPACT);

        // Main no longer has the branch's snapshots 2 and 3, but it still ends at the branch's
        // snapshot 4, so the next publication of the branch only appends. The fifth sorted run
        // also triggers a compaction on the branch, so it has snapshots 5 and 6.
        io.watch(table.snapshotManager().snapshotDirectory(), "snapshot-");
        table.fastForward("b1");
        assertThat(io.changes).containsExactly("write snapshot-5", "write snapshot-6");
    }

    /** Main has snapshots 1 and 2; branch b1 starts from snapshot 2 and adds snapshots 3 and 4. */
    private FileStoreTable createTableWithBranchAhead(FileIO io) throws Exception {
        schemaManager.commitChanges(SchemaChange.setOption(BUCKET.key(), "1"));
        FileStoreTable table = FileStoreTableFactory.create(io, tablePath);
        commitRow(table, 1);
        commitRow(table, 2);
        table.createTag("tag2", 2);
        table.createBranch("b1", "tag2");
        FileStoreTable branch = table.switchToBranch("b1");
        commitRow(branch, 3);
        commitRow(branch, 4);
        return table;
    }

    private FileStoreTable createTableWithLock(FileIO io, CatalogLockFactory lockFactory) {
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
        return FileStoreTableFactory.create(io, tablePath, schemaManager.latest().get(), env);
    }

    private static void expireAllButLatest(FileStoreTable table) {
        table.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                                .build())
                .expire();
    }

    private static void commitRow(FileStoreTable table, int id) throws Exception {
        commit(table, GenericRow.of(id, id * 10, id * 100));
    }

    /** Commit a row to a table that has one more INT column than the initial schema. */
    private static void commitWideRow(FileStoreTable table, int id) throws Exception {
        commit(table, GenericRow.of(id, id * 10, id * 100, id));
    }

    private static void commit(FileStoreTable table, GenericRow row) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(row);
            commit.commit(write.prepareCommit());
        }
    }

    private static List<String> readRows(FileStoreTable table) throws Exception {
        return getResult(
                table.newRead(),
                table.newReadBuilder().newScan().plan().splits(),
                row -> row.getInt(0) + "," + row.getInt(1) + "," + row.getInt(2));
    }

    /**
     * A {@link LocalFileIO} that lists directories in name order, as HDFS and object stores do. It
     * can fail one write of a prefixed file into a directory, and records each change to the
     * prefixed files of a watched directory together with whether the table lock was held. Files
     * are written by copying, by renaming a temporary file, or directly; all three count as writes.
     */
    private static class InterceptingFileIO extends LocalFileIO {

        @Nullable private volatile Path failingDirectory;
        private volatile String failingPrefix = "";
        private final AtomicInteger writesBeforeFailure = new AtomicInteger();
        @Nullable private volatile Path watchedDirectory;
        private volatile String watchedPrefix = "";
        private volatile BooleanSupplier lockHeld = () -> false;
        private volatile Runnable afterWatchedWrite = () -> {};
        private volatile Runnable afterWatchedDelete = () -> {};
        @Nullable private volatile Path openTrigger;
        private volatile Runnable onOpenAction = () -> {};
        @Nullable private volatile Path undeletable;
        private final List<String> changes = new CopyOnWriteArrayList<>();
        private final List<Boolean> lockHeldOnChanges = new CopyOnWriteArrayList<>();

        /** Run the action once, when the file is next opened for reading. */
        void onOpen(Path path, Runnable action) {
            onOpenAction = action;
            openTrigger = path;
        }

        /** Make the next delete of the file report failure and leave it in place. */
        void failDeleteOf(Path path) {
            undeletable = path;
        }

        void failSnapshotWrite(Path directory, int successfulWritesFirst) {
            failWrite(directory, "snapshot-", successfulWritesFirst);
        }

        /**
         * Let the given number of writes of prefixed files into the directory succeed, then fail.
         */
        void failWrite(Path directory, String prefix, int successfulWritesFirst) {
            writesBeforeFailure.set(successfulWritesFirst);
            failingPrefix = prefix;
            failingDirectory = directory;
        }

        /** Record changes to the prefixed files of the directory from now on. */
        void watch(Path directory, String prefix) {
            changes.clear();
            lockHeldOnChanges.clear();
            watchedPrefix = prefix;
            watchedDirectory = directory;
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            Arrays.sort(statuses, Comparator.comparing((FileStatus s) -> s.getPath().getName()));
            return statuses;
        }

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            beforeWrite(path);
            return super.newOutputStream(path, overwrite);
        }

        @Override
        public void copyFile(Path sourcePath, Path targetPath, boolean overwrite)
                throws IOException {
            beforeWrite(targetPath);
            super.copyFile(sourcePath, targetPath, overwrite);
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            beforeWrite(dst);
            boolean renamed = super.rename(src, dst);
            if (renamed && isIn(dst, watchedDirectory, watchedPrefix)) {
                afterWatchedWrite.run();
            }
            return renamed;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (sameFile(path, openTrigger)) {
                openTrigger = null;
                onOpenAction.run();
            }
            return super.newInputStream(path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            if (sameFile(path, undeletable)) {
                undeletable = null;
                return false;
            }
            boolean watched = isIn(path, watchedDirectory, watchedPrefix);
            if (watched) {
                record("delete", path);
            }
            boolean deleted = super.delete(path, recursive);
            if (watched) {
                afterWatchedDelete.run();
            }
            return deleted;
        }

        private static boolean sameFile(Path path, @Nullable Path other) {
            return other != null
                    && new File(path.toUri().getPath()).equals(new File(other.toUri().getPath()));
        }

        private void beforeWrite(Path path) throws IOException {
            if (isIn(path, failingDirectory, failingPrefix)
                    && writesBeforeFailure.getAndDecrement() <= 0) {
                failingDirectory = null;
                throw new IOException("Injected write failure");
            }
            if (isIn(path, watchedDirectory, watchedPrefix)) {
                record("write", path);
            }
        }

        private void record(String change, Path path) {
            changes.add(change + " " + path.getName());
            lockHeldOnChanges.add(lockHeld.getAsBoolean());
        }

        private static boolean isIn(Path path, @Nullable Path directory, String prefix) {
            return directory != null
                    && path.getName().startsWith(prefix)
                    && new File(path.toUri().getPath())
                            .getParentFile()
                            .equals(new File(directory.toUri().getPath()));
        }
    }

    /**
     * Hands out locks that record which identifiers were locked and whether one of them is held.
     * Once {@code expired} is set, a held lock fails its validity check like an expired lease.
     */
    private static class HeldFlagLockFactory implements CatalogLockFactory {

        private final AtomicBoolean held = new AtomicBoolean();
        private final AtomicBoolean expired = new AtomicBoolean();
        private final List<Identifier> lockedIdentifiers = new CopyOnWriteArrayList<>();

        @Override
        public String identifier() {
            return "held-flag";
        }

        @Override
        public Lock createLock(
                CatalogLockContext context,
                Identifier identifier,
                @Nullable String tableUuid,
                @Nullable String commitUser,
                Options tableOptions) {
            lockedIdentifiers.add(identifier);
            return new Lock() {
                @Override
                public AutoCloseable acquire() {
                    held.set(true);
                    return () -> held.set(false);
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
        }

        boolean isHeld() {
            return held.get();
        }
    }
}
