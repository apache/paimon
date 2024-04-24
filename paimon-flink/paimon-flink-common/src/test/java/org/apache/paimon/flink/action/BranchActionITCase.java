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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.Test;

import java.util.Collections;

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
        branchManager.branches();

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
}
