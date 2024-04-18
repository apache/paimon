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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CloneAction}. */
public class CloneActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private final String targetTableName = "copy_table";

    @Test
    @Timeout(60_000)
    public void testCloneLatestSnapshot() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // do clone
        runCloneLatestSnapshotAction();

        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);
        List<DataSplit> splits2 = targetTable.newSnapshotReader().read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        TableScan sourceTableScan = table.newReadBuilder().newScan();
        TableScan targetTableScan = targetTable.newReadBuilder().newScan();

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]",
                        "+I[2, 100, 15, 20221208]",
                        "+I[2, 100, 15, 20221209]",
                        "+I[2, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, sourceTableScan, scanResult, 60_000);
        validateResult(targetTable, ROW_TYPE, targetTableScan, scanResult, 60_000);
    }

    @Test
    @Timeout(60_000)
    public void testCloneSpecificSnapshot() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // do clone
        runCloneSpecificSnapshotAction();

        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 1, Snapshot.CommitKind.APPEND);

        SnapshotReader snapshotReader1 = table.newSnapshotReader().withSnapshot(1);
        List<DataSplit> splits1 = snapshotReader1.read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);
        SnapshotReader snapshotReader2 = targetTable.newSnapshotReader().withSnapshot(1);
        List<DataSplit> splits2 = snapshotReader2.read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, snapshotReader1.read(), scanResult, 60_000);
        validateResult(targetTable, ROW_TYPE, snapshotReader2.read(), scanResult, 60_000);
    }

    @Test
    @Timeout(60_000)
    public void testCloneFromTimestampSnapshot() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // do clone
        runCloneFromTimestampAction();

        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        SnapshotReader snapshotReader1 = table.newSnapshotReader().withSnapshot(2);
        List<DataSplit> splits1 = snapshotReader1.read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);
        SnapshotReader snapshotReader2 = targetTable.newSnapshotReader().withSnapshot(2);
        List<DataSplit> splits2 = snapshotReader2.read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]",
                        "+I[2, 100, 15, 20221208]",
                        "+I[2, 100, 15, 20221209]",
                        "+I[2, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, snapshotReader1.read(), scanResult, 60_000);
        validateResult(targetTable, ROW_TYPE, snapshotReader2.read(), scanResult, 60_000);
    }

    @Test
    @Timeout(60_000)
    public void testCloneTag() throws Exception {
        String tagName = "test_clone_tag";
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        table.createTag(tagName, 1);

        // do clone
        runCloneTagAction();

        FileStoreTable targetTable = getFileStoreTable(targetTableName);

        SnapshotReader snapshotReader1 = table.newSnapshotReader().withSnapshot(1);
        List<DataSplit> splits1 = snapshotReader1.read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);

        Snapshot snapshot = targetTable.tagManager().taggedSnapshot(tagName);
        SnapshotReader snapshotReader2 = targetTable.newSnapshotReader().withSnapshot(snapshot);
        List<DataSplit> splits2 = snapshotReader2.read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, snapshotReader1.read(), scanResult, 60_000);
        validateResult(targetTable, ROW_TYPE, snapshotReader2.read(), scanResult, 60_000);
    }

    @Test
    @Timeout(60_000)
    public void testCloneTable() throws Exception {
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        // do clone
        runCloneTableAction();

        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        // validate snapshot-1
        SnapshotReader snapshotReader1 = table.newSnapshotReader().withSnapshot(1);
        List<DataSplit> splits11 = snapshotReader1.read().dataSplits();
        assertThat(splits11.size()).isEqualTo(3);
        SnapshotReader snapshotReader2 = targetTable.newSnapshotReader().withSnapshot(1);
        List<DataSplit> splits12 = snapshotReader2.read().dataSplits();
        assertThat(splits12.size()).isEqualTo(3);

        List<String> scanResult1 =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, snapshotReader1.read(), scanResult1, 60_000);
        validateResult(targetTable, ROW_TYPE, snapshotReader2.read(), scanResult1, 60_000);

        // validate snapshot-2
        snapshotReader1 = table.newSnapshotReader().withSnapshot(2);
        List<DataSplit> splits21 = snapshotReader1.read().dataSplits();
        assertThat(splits21.size()).isEqualTo(3);
        snapshotReader2 = targetTable.newSnapshotReader().withSnapshot(2);
        List<DataSplit> splits22 = snapshotReader2.read().dataSplits();
        assertThat(splits22.size()).isEqualTo(3);

        List<String> scanResult2 =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]",
                        "+I[2, 100, 15, 20221208]",
                        "+I[2, 100, 15, 20221209]",
                        "+I[2, 100, 16, 20221208]");
        validateResult(table, ROW_TYPE, snapshotReader1.read(), scanResult2, 60_000);
        validateResult(targetTable, ROW_TYPE, snapshotReader2.read(), scanResult2, 60_000);
    }

    private FileStoreTable prepareTable(
            List<String> partitionKeys, List<String> primaryKeys, Map<String, String> tableOptions)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(ROW_TYPE, partitionKeys, primaryKeys, tableOptions);
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();
        return table;
    }

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }

    private void runCloneLatestSnapshotAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneAction action =
                createAction(
                        CloneAction.class,
                        "clone",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        database,
                        "--target_table",
                        targetTableName,
                        "--clone_type",
                        "LatestSnapshot");
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }

    private void runCloneSpecificSnapshotAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneAction action =
                createAction(
                        CloneAction.class,
                        "clone",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        database,
                        "--target_table",
                        targetTableName,
                        "--clone_type",
                        "SpecificSnapshot",
                        "--snapshot_id",
                        "1");
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }

    private void runCloneFromTimestampAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneAction action =
                createAction(
                        CloneAction.class,
                        "clone",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        database,
                        "--target_table",
                        targetTableName,
                        "--clone_type",
                        "FromTimestamp",
                        "--timestamp",
                        "9999999999999");
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }

    private void runCloneTagAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneAction action =
                createAction(
                        CloneAction.class,
                        "clone",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        database,
                        "--target_table",
                        targetTableName,
                        "--clone_type",
                        "Tag",
                        "--tag_name",
                        "test_clone_tag");
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }

    private void runCloneTableAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneAction action =
                createAction(
                        CloneAction.class,
                        "clone",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        database,
                        "--target_table",
                        targetTableName,
                        "--clone_type",
                        "Table");
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }
}
