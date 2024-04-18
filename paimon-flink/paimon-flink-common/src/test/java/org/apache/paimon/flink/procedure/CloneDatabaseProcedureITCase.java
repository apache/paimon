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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link CloneDatabaseProcedure}. */
public class CloneDatabaseProcedureITCase extends CatalogITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private static final String sourceTableName1 = "sourceTableName1";
    private static final String sourceTableName2 = "sourceTableName2";

    @Test
    public void testCloneDatabase() throws Exception {
        sql(
                "CREATE TABLE sourceTableName1 ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        sql(
                "CREATE TABLE sourceTableName2 ("
                        + " k INT,"
                        + " v INT,"
                        + " hh INT,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt, hh) NOT ENFORCED"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable sourceTable1 = paimonTable(sourceTableName1);
        FileStoreTable sourceTable2 = paimonTable(sourceTableName2);

        sql(
                "INSERT INTO sourceTableName1 VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO sourceTableName1 VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");
        sql(
                "INSERT INTO sourceTableName2 VALUES (1, 100, 15, '20221208'), (1, 100, 16, '20221208'), (1, 100, 15, '20221209')");
        sql(
                "INSERT INTO sourceTableName2 VALUES (2, 100, 15, '20221208'), (2, 100, 16, '20221208'), (2, 100, 15, '20221209')");

        checkLatestSnapshot(sourceTable1, 2, Snapshot.CommitKind.APPEND);
        checkLatestSnapshot(sourceTable2, 2, Snapshot.CommitKind.APPEND);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        String targetDatabase = "targetDatabase";
        sql(
                "CALL sys.clone_database('%s', '%s', '', '%s', '%s', '', '')",
                path, tEnv.getCurrentDatabase(), path, targetDatabase);

        FileStoreTable targetTable1 = paimonTable(targetDatabase, sourceTableName1);
        FileStoreTable targetTable2 = paimonTable(targetDatabase, sourceTableName2);
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

    private void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }
}
