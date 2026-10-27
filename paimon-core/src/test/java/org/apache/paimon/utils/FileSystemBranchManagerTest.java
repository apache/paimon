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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
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

import java.nio.file.Files;
import java.util.List;
import java.util.Set;
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
}
