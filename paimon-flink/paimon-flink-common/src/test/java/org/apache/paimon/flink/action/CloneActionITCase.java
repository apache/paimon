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
import org.apache.paimon.flink.clone.PickFilesUtil;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CloneAction}. */
public class CloneActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    private final String targetTableName = "copy_table";
    private long incrementalIdentifier = 0;

    @Test
    @Timeout(60_000)
    public void testCloneLatestSnapshot() throws Exception {
        /** prepare source table */
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        Collections.emptyMap());

        /** write data to source table */
        writeData(
                rowData(1, 100, 15, BinaryString.fromString("20221208")),
                rowData(1, 100, 16, BinaryString.fromString("20221208")),
                rowData(1, 100, 15, BinaryString.fromString("20221209")));
        writeData(
                rowData(2, 100, 15, BinaryString.fromString("20221208")),
                rowData(2, 100, 16, BinaryString.fromString("20221208")),
                rowData(2, 100, 15, BinaryString.fromString("20221209")));
        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        /** do clone */
        runCloneLatestSnapshotAction();
        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        /** check scan result */
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
    public void testCopyFilesNameAndSize() throws Exception {
        /** prepare source table */
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        new HashMap<String, String>() {
                            {
                                put(
                                        CoreOptions.CHANGELOG_PRODUCER.key(),
                                        CoreOptions.ChangelogProducer.INPUT.toString());
                                put(CoreOptions.BUCKET.key(), "-1");
                            }
                        });

        /** write data to source table */
        writeDataToDynamicBucket(
                Arrays.asList(
                        rowData(1, 100, 15, BinaryString.fromString("20221201")),
                        rowData(2, 40000, 16, BinaryString.fromString("20221208")),
                        rowData(4, 100, 19, BinaryString.fromString("20221209"))),
                1);
        writeDataToDynamicBucket(
                Arrays.asList(
                        rowData(24, 898, 1, BinaryString.fromString("20221209")),
                        rowData(12, 312, 90, BinaryString.fromString("20221208")),
                        rowData(29, 434, 15, BinaryString.fromString("20221209"))),
                2);
        checkLatestSnapshot(table, 2, Snapshot.CommitKind.APPEND);

        /** do clone */
        runCloneLatestSnapshotAction();
        FileStoreTable targetTable = getFileStoreTable(targetTableName);
        checkLatestSnapshot(targetTable, 2, Snapshot.CommitKind.APPEND);

        /** check scan result */
        List<DataSplit> splits1 = table.newSnapshotReader().read().dataSplits();
        assertThat(splits1.size()).isEqualTo(6);
        List<DataSplit> splits2 = targetTable.newSnapshotReader().read().dataSplits();
        assertThat(splits2.size()).isEqualTo(6);

        TableScan sourceTableScan = table.newReadBuilder().newScan();
        TableScan targetTableScan = targetTable.newReadBuilder().newScan();

        List<String> scanResult =
                Arrays.asList(
                        "+I[1, 100, 15, 20221201]",
                        "+I[12, 312, 90, 20221208]",
                        "+I[2, 40000, 16, 20221208]",
                        "+I[24, 898, 1, 20221209]",
                        "+I[29, 434, 15, 20221209]",
                        "+I[4, 100, 19, 20221209]");
        validateResult(table, ROW_TYPE, sourceTableScan, scanResult, 60_000);
        validateResult(targetTable, ROW_TYPE, targetTableScan, scanResult, 60_000);

        /**
         * check file name and file size of manifest && index && data && schema && changelog &&
         * snapshot files
         */
        List<Path> targetTableFiles = PickFilesUtil.getUsedFilesForLatestSnapshot(targetTable);
        List<Pair<Path, Path>> filesPathInfoList =
                targetTableFiles.stream()
                        .map(
                                absolutePath ->
                                        Pair.of(
                                                absolutePath,
                                                getPathExcludeTableRoot(
                                                        absolutePath, targetTable.location())))
                        .collect(Collectors.toList());

        Path tableLocation = table.location();
        for (Pair<Path, Path> filesPathInfo : filesPathInfoList) {
            Path sourceTableFile = new Path(tableLocation.toString() + filesPathInfo.getRight());
            Assertions.assertTrue(table.fileIO().exists(sourceTableFile));
            Assertions.assertEquals(
                    table.fileIO().getFileSize(sourceTableFile),
                    targetTable.fileIO().getFileSize(filesPathInfo.getLeft()));
        }
    }

    @Test
    @Timeout(60_000)
    public void testSnapshotWithFastExpire() throws Exception {
        /** prepare source table */
        FileStoreTable table =
                prepareTable(
                        Arrays.asList("dt", "hh"),
                        Arrays.asList("dt", "hh", "k"),
                        new HashMap<String, String>() {
                            {
                                put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "3");
                                put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "3");
                            }
                        });

        /** write data into source table and commit them */
        Thread writeDataToSourceTablethread =
                new Thread(
                        () -> {
                            try {
                                /** write and commit data to source table. */
                                while (true) {
                                    writeData(
                                            rowData(
                                                    1,
                                                    100,
                                                    15,
                                                    BinaryString.fromString("20221208")),
                                            rowData(
                                                    1,
                                                    100,
                                                    16,
                                                    BinaryString.fromString("20221208")),
                                            rowData(
                                                    1,
                                                    100,
                                                    15,
                                                    BinaryString.fromString("20221209")),
                                            rowData(
                                                    2,
                                                    100,
                                                    15,
                                                    BinaryString.fromString("20221208")),
                                            rowData(
                                                    2,
                                                    100,
                                                    16,
                                                    BinaryString.fromString("20221208")),
                                            rowData(
                                                    2,
                                                    100,
                                                    15,
                                                    BinaryString.fromString("20221209")));
                                }
                            } catch (Exception ignored) {
                            }
                        });
        writeDataToSourceTablethread.start();

        /** do clone with source table fast expire */
        boolean cloneJobHasException;
        do {
            try {
                runCloneLatestSnapshotAction();
                cloneJobHasException = false;
            } catch (Exception e) {
                cloneJobHasException = true;
            }
        } while (cloneJobHasException);

        /** check scan result */
        FileStoreTable targetTable = getFileStoreTable(targetTableName);
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

    private void writeDataToDynamicBucket(List<GenericRow> data, int bucket) throws Exception {
        for (GenericRow d : data) {
            write.write(d, bucket);
        }
        commit.commit(incrementalIdentifier, write.prepareCommit(true, incrementalIdentifier));
        incrementalIdentifier++;
    }

    private Path getPathExcludeTableRoot(Path absolutePath, Path sourceTableRoot) {
        String fileAbsolutePath = absolutePath.toUri().toString();
        String sourceTableRootPath = sourceTableRoot.toString();

        checkState(
                fileAbsolutePath.startsWith(sourceTableRootPath),
                "This is a bug, please report. fileAbsolutePath is : "
                        + fileAbsolutePath
                        + ", sourceTableRootPath is : "
                        + sourceTableRootPath);

        return new Path(fileAbsolutePath.substring(sourceTableRootPath.length()));
    }

    private FileStoreTable prepareTable(
            List<String> partitionKeys, List<String> primaryKeys, Map<String, String> tableOptions)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        partitionKeys,
                        primaryKeys,
                        Collections.emptyList(),
                        tableOptions);
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
                        targetTableName);
        action.withStreamExecutionEnvironment(env).build();
        env.execute();
    }
}
