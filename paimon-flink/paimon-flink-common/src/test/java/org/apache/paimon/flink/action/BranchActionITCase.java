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

package org.apache.paimon.flink.action;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for branch management actions. */
class BranchActionITCase extends ActionITCaseBase {

    @Test
    void testCreateAndDeleteBranch() throws Exception {

        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        TagManager tagManager = new TagManager(table.fileIO(), table.location());
        callProcedure(
                String.format(
                        "CALL sys.create_tag('%s.%s', 'tag2', 2, '5 d')", database, tableName));
        assertThat(tagManager.tagExists("tag2")).isTrue();

        BranchManager branchManager = table.branchManager();
        callProcedure(
                String.format(
                        "CALL sys.create_branch('%s.%s', 'branch_name', 'tag2')",
                        database, tableName));
        assertThat(branchManager.branchExists("branch_name")).isTrue();

        callProcedure(
                String.format(
                        "CALL sys.delete_branch('%s.%s', 'branch_name')", database, tableName));
        assertThat(branchManager.branchExists("branch_name")).isFalse();
    }

    @Test
    void testCreateAndDeleteBranchWithSnapshotId() throws Exception {

        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        BranchManager branchManager = table.branchManager();

        callProcedure(
                String.format(
                        "CALL sys.create_branch('%s.%s', 'branch_name_with_snapshotId', 2)",
                        database, tableName));
        assertThat(branchManager.branchExists("branch_name_with_snapshotId")).isTrue();

        callProcedure(
                String.format(
                        "CALL sys.delete_branch('%s.%s', 'branch_name_with_snapshotId')",
                        database, tableName));
        assertThat(branchManager.branchExists("branch_name_with_snapshotId")).isFalse();
    }

    @Test
    void testCreateAndDeleteEmptyBranch() throws Exception {

        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        BranchManager branchManager = table.branchManager();
        callProcedure(
                String.format(
                        "CALL sys.create_branch('%s.%s', 'empty_branch_name')",
                        database, tableName));
        assertThat(branchManager.branchExists("empty_branch_name")).isTrue();

        callProcedure(
                String.format(
                        "CALL sys.delete_branch('%s.%s', 'empty_branch_name')",
                        database, tableName));
        assertThat(branchManager.branchExists("empty_branch_name")).isFalse();
    }

    @Test
    void testReplaceBranch() throws Exception {
        init(warehouse);
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        // Create tag2
        TagManager tagManager = new TagManager(table.fileIO(), table.location());
        callProcedure(
                String.format("CALL sys.create_tag('%s.%s', 'tag2', 2)", database, tableName));
        assertThat(tagManager.tagExists("tag2")).isTrue();

        // Create replace_branch_name branch
        BranchManager branchManager = table.branchManager();
        callProcedure(
                String.format(
                        "CALL sys.create_branch('%s.%s', 'replace_branch_name', 'tag2')",
                        database, tableName));
        assertThat(branchManager.branchExists("replace_branch_name")).isTrue();

        // Replace branch
        callProcedure(
                String.format(
                        "CALL sys.replace_branch('%s.%s', 'replace_branch_name')",
                        database, tableName));

        // Check snapshot
        SnapshotManager snapshotManager = table.snapshotManager();
        assertThat(snapshotManager.snapshotExists(3)).isFalse();

        // Renew write
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // Add data, forward to replace branch
        for (long i = 4; i < 14; i++) {
            writeData(rowData(i, BinaryString.fromString(String.format("new.data_%s", i))));
        }

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        List<String> result =
                getResult(
                        readBuilder.newRead(),
                        plan == null ? Collections.emptyList() : plan.splits(),
                        rowType);
        List<String> sortedActual = new ArrayList<>(result);
        List<String> expected =
                Arrays.asList(
                        "+I[1, Hi]",
                        "+I[2, Hello]",
                        "+I[4, new.data_4]",
                        "+I[5, new.data_5]",
                        "+I[6, new.data_6]",
                        "+I[7, new.data_7]",
                        "+I[8, new.data_8]",
                        "+I[9, new.data_9]",
                        "+I[10, new.data_10]",
                        "+I[11, new.data_11]",
                        "+I[12, new.data_12]",
                        "+I[13, new.data_13]");
        Assert.assertEquals(expected, sortedActual);

        callProcedure(
                String.format("CALL sys.create_tag('%s.%s', 'tag3', 3)", database, tableName));
        assertThat(tagManager.tagExists("tag3")).isTrue();
    }

    @Test
    void testReplaceMainBranchAndCleanMainBranch() throws Exception {

        init(warehouse);
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"k", "v"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        // Create tag2
        TagManager tagManager = new TagManager(table.fileIO(), table.location());
        callProcedure(
                String.format("CALL sys.create_tag('%s.%s', 'tag2', 2)", database, tableName));
        assertThat(tagManager.tagExists("tag2")).isTrue();

        BranchManager branchManager = table.branchManager();
        callProcedure(
                String.format(
                        "CALL sys.create_branch('%s.%s', 'branch_name', 'tag2')",
                        database, tableName));
        assertThat(branchManager.branchExists("branch_name")).isTrue();

        callProcedure(
                String.format(
                        "CALL sys.set_main_branch('%s.%s', 'branch_name')", database, tableName));
        assertThat(branchManager.mainBranch()).isEqualTo("branch_name");

        callProcedure(String.format("CALL sys.clean_main_branch('%s.%s')", database, tableName));
        assertThat(branchManager.mainBranch()).isEqualTo("main");
    }
}
