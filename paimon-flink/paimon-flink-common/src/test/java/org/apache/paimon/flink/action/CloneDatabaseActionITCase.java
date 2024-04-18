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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CloneDatabaseAction}. */
public class CloneDatabaseActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private static final String sourceTableName1 = "sourceTableName1";
    private static final String sourceTableName2 = "sourceTableName2";
    private static final String targetDatabase = "targetDatabase";

    private StreamTableWrite write1;
    private StreamTableCommit commit1;
    private StreamTableWrite write2;
    private StreamTableCommit commit2;

    @AfterEach
    public void after() throws Exception {
        if (write1 != null) {
            write1.close();
            write1 = null;
        }
        if (commit1 != null) {
            commit1.close();
            commit1 = null;
        }
        if (write2 != null) {
            write2.close();
            write2 = null;
        }
        if (commit2 != null) {
            commit2.close();
            commit2 = null;
        }
        super.after();
    }

    @Test
    @Timeout(60_000)
    public void testCloneDatabase() throws Exception {
        List<FileStoreTable> sourceTables =
                prepareTables(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
        FileStoreTable sourceTable1 = sourceTables.get(0);
        FileStoreTable sourceTable2 = sourceTables.get(0);

        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));

        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));

        checkLatestSnapshot(sourceTable1, 2, Snapshot.CommitKind.APPEND);
        checkLatestSnapshot(sourceTable2, 2, Snapshot.CommitKind.APPEND);

        // do clone
        runCloneDatabaseAction();

        FileStoreTable targetTable1 = getFileStoreTable(targetDatabase, sourceTableName1);
        FileStoreTable targetTable2 = getFileStoreTable(targetDatabase, sourceTableName2);
        checkLatestSnapshot(targetTable1, 2, Snapshot.CommitKind.APPEND);
        checkLatestSnapshot(targetTable2, 2, Snapshot.CommitKind.APPEND);

        List<DataSplit> sourceSplits1 = sourceTable1.newSnapshotReader().read().dataSplits();
        assertThat(sourceSplits1.size()).isEqualTo(3);
        List<DataSplit> sourceSplits2 = sourceTable1.newSnapshotReader().read().dataSplits();
        assertThat(sourceSplits2.size()).isEqualTo(3);
        List<DataSplit> targetSplits1 = sourceTable1.newSnapshotReader().read().dataSplits();
        assertThat(targetSplits1.size()).isEqualTo(3);
        List<DataSplit> targetSplits2 = sourceTable1.newSnapshotReader().read().dataSplits();
        assertThat(targetSplits2.size()).isEqualTo(3);

        TableScan sourceTableScan1 = sourceTable1.newReadBuilder().newScan();
        TableScan sourceTableScan2 = sourceTable2.newReadBuilder().newScan();
        TableScan targetTableScan1 = targetTable1.newReadBuilder().newScan();
        TableScan targetTableScan2 = targetTable2.newReadBuilder().newScan();

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221208]",
                        "+I[1, 100, 15, 20221209]",
                        "+I[1, 100, 16, 20221208]",
                        "+I[2, 100, 15, 20221208]",
                        "+I[2, 100, 15, 20221209]",
                        "+I[2, 100, 16, 20221208]");

        validateResult(sourceTable1, ROW_TYPE, sourceTableScan1, scanResult, 60_000);
        validateResult(sourceTable2, ROW_TYPE, sourceTableScan2, scanResult, 60_000);
        validateResult(targetTable1, ROW_TYPE, targetTableScan1, scanResult, 60_000);
        validateResult(targetTable2, ROW_TYPE, targetTableScan2, scanResult, 60_000);
    }

    private List<FileStoreTable> prepareTables(
            List<String> partitionKeys, List<String> primaryKeys, Map<String, String> tableOptions)
            throws Exception {
        List<FileStoreTable> tables = new ArrayList<>(2);

        FileStoreTable sourceTable1 =
                createFileStoreTable(
                        sourceTableName1, ROW_TYPE, partitionKeys, primaryKeys, tableOptions);
        StreamWriteBuilder streamWriteBuilder1 =
                sourceTable1.newStreamWriteBuilder().withCommitUser(commitUser);
        write1 = streamWriteBuilder1.newWrite();
        commit1 = streamWriteBuilder1.newCommit();
        tables.add(sourceTable1);

        FileStoreTable sourceTable2 =
                createFileStoreTable(
                        sourceTableName2, ROW_TYPE, partitionKeys, primaryKeys, tableOptions);
        StreamWriteBuilder streamWriteBuilder2 =
                sourceTable2.newStreamWriteBuilder().withCommitUser(commitUser);
        write2 = streamWriteBuilder2.newWrite();
        commit2 = streamWriteBuilder2.newCommit();
        tables.add(sourceTable2);

        return tables;
    }

    protected void writeData(GenericRow... data) throws Exception {
        for (GenericRow d : data) {
            write1.write(d);
            write2.write(d);
        }
        commit1.commit(incrementalIdentifier, write1.prepareCommit(true, incrementalIdentifier));
        commit2.commit(incrementalIdentifier, write2.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
    }

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }

    private void runCloneDatabaseAction() throws Exception {
        StreamExecutionEnvironment env = streamExecutionEnvironmentBuilder().batchMode().build();
        CloneDatabaseAction action =
                createAction(
                        CloneDatabaseAction.class,
                        "clone_database",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--target_warehouse",
                        warehouse,
                        "--target_database",
                        targetDatabase);
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }
}
