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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link CloneProcedure}. */
public class CloneProcedureITCase extends CatalogITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    @Test
    public void testCloneLatestSnapshot() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.clone('%s', '%s', 'T', '', '%s', '%s', 'T_copy', '', '', 'LatestSnapshot')",
                path, tEnv.getCurrentDatabase(), path, tEnv.getCurrentDatabase());

        FileStoreTable targetTable = paimonTable("T_copy");
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);
        List<DataSplit> splits2 = targetTable.newSnapshotReader().read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        TableScan sourceTableScan = table.newReadBuilder().newStreamScan();
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
    public void testCloneSpecificSnapshot() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.clone('%s', '%s', 'T', '', '%s', '%s', 'T_copy', '', '', 'SpecificSnapshot', 1)",
                path, tEnv.getCurrentDatabase(), path, tEnv.getCurrentDatabase());

        FileStoreTable targetTable = paimonTable("T_copy");
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
    public void testCloneFromTimestampSnapshot() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.clone('%s', '%s', 'T', '', '%s', '%s', 'T_copy', '', '', 'FromTimestamp', 9999999999999)",
                path, tEnv.getCurrentDatabase(), path, tEnv.getCurrentDatabase());

        FileStoreTable targetTable = paimonTable("T_copy");
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(3);
        List<DataSplit> splits2 = targetTable.newSnapshotReader().read().dataSplits();
        assertThat(splits2.size()).isEqualTo(3);

        TableScan sourceTableScan = table.newReadBuilder().newStreamScan();
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
    public void testCloneTag() throws Exception {
        String tagName = "test_clone_tag";
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        table.createTag(tagName, 1);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.clone('%s', '%s', 'T', '', '%s', '%s', 'T_copy', '', '', 'Tag', '%s')",
                path, tEnv.getCurrentDatabase(), path, tEnv.getCurrentDatabase(), tagName);

        FileStoreTable targetTable = paimonTable("T_copy");

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
    public void testCloneTable() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        sql(
                "INSERT INTO T VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO T VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.clone('%s', '%s', 'T', '', '%s', '%s', 'T_copy', '', '', 'Table')",
                path, tEnv.getCurrentDatabase(), path, tEnv.getCurrentDatabase());

        FileStoreTable targetTable = paimonTable("T_copy");
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

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }
}
