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

package org.apache.paimon.flink.source.statistics;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Statistics tests for {@link FileStoreTable}. */
public abstract class FileStoreTableStatisticsTestBase {
    protected final ObjectIdentifier identifier = ObjectIdentifier.of("c", "d", "t");

    @TempDir java.nio.file.Path tempDir;

    protected Path tablePath;
    protected String commitUser;

    @BeforeEach
    public void before() {
        tablePath = new Path(tempDir.toString() + "/" + UUID.randomUUID());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testTableScanStatistics() throws Exception {
        FileStoreTable table = writeData();
        Map<String, ColStats<?>> colStatsMap = new HashMap<>();
        colStatsMap.put("pt", ColStats.newColStats(0, 2L, 1, 2, 0L, null, null));
        colStatsMap.put("a", ColStats.newColStats(1, 9L, 10, 90, 0L, null, null));
        colStatsMap.put("b", ColStats.newColStats(2, 7L, 100L, 900L, 2L, null, null));
        colStatsMap.put(
                "c",
                ColStats.newColStats(
                        3,
                        7L,
                        BinaryString.fromString("S1"),
                        BinaryString.fromString("S8"),
                        2L,
                        null,
                        null));

        FileStore<?> fileStore = table.store();
        FileStoreCommit fileStoreCommit = fileStore.newCommit(commitUser);
        Snapshot latestSnapshot = fileStore.snapshotManager().latestSnapshot();
        Statistics colStats =
                new Statistics(
                        latestSnapshot.id(), latestSnapshot.schemaId(), 9L, null, colStatsMap);
        fileStoreCommit.commitStatistics(colStats, Long.MAX_VALUE);
        fileStoreCommit.close();
        DataTableSource scanSource = new DataTableSource(identifier, table, false, null, null);
        Assertions.assertThat(scanSource.reportStatistics().getRowCount()).isEqualTo(9L);
        Map<String, ColumnStats> expectedColStats = new HashMap<>();
        expectedColStats.put(
                "pt",
                ColumnStats.Builder.builder()
                        .setNdv(2L)
                        .setMin(1)
                        .setMax(2)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "a",
                ColumnStats.Builder.builder()
                        .setNdv(9L)
                        .setMin(10)
                        .setMax(90)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "b",
                ColumnStats.Builder.builder()
                        .setNdv(7L)
                        .setMin(100L)
                        .setMax(900L)
                        .setNullCount(2L)
                        .build());
        expectedColStats.put(
                "c",
                ColumnStats.Builder.builder()
                        .setNdv(7L)
                        .setMin(BinaryString.fromString("S1"))
                        .setMax(BinaryString.fromString("S8"))
                        .setNullCount(2L)
                        .build());
        Assertions.assertThat(scanSource.reportStatistics().getColumnStats())
                .isEqualTo(expectedColStats);
    }

    @Test
    public void testTableStreamingStatistics() throws Exception {
        FileStoreTable table = writeData();
        DataTableSource streamSource = new DataTableSource(identifier, table, true, null, null);
        Assertions.assertThat(streamSource.reportStatistics()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testTableFilterPartitionStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource partitionFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.equal(0, 1),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(partitionFilterSource.reportStatistics().getRowCount()).isEqualTo(5L);
        Map<String, ColStats<?>> colStatsMap = new HashMap<>();
        colStatsMap.put("pt", ColStats.newColStats(0, 1L, 1, 2, 0L, null, null));
        colStatsMap.put("a", ColStats.newColStats(1, 5L, 10, 90, 0L, null, null));
        colStatsMap.put("b", ColStats.newColStats(2, 3L, 100L, 900L, 2L, null, null));
        colStatsMap.put(
                "c",
                ColStats.newColStats(
                        3,
                        3L,
                        BinaryString.fromString("S1"),
                        BinaryString.fromString("S7"),
                        2L,
                        null,
                        null));

        FileStore<?> fileStore = table.store();
        FileStoreCommit fileStoreCommit = fileStore.newCommit(commitUser);
        Snapshot latestSnapshot = fileStore.snapshotManager().latestSnapshot();
        Statistics colStats =
                new Statistics(
                        latestSnapshot.id(), latestSnapshot.schemaId(), 9L, null, colStatsMap);
        fileStoreCommit.commitStatistics(colStats, Long.MAX_VALUE);
        fileStoreCommit.close();

        Map<String, ColumnStats> expectedColStats = new HashMap<>();
        expectedColStats.put(
                "pt",
                ColumnStats.Builder.builder()
                        .setNdv(1L)
                        .setMin(1)
                        .setMax(2)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "a",
                ColumnStats.Builder.builder()
                        .setNdv(5L)
                        .setMin(10)
                        .setMax(90)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "b",
                ColumnStats.Builder.builder()
                        .setNdv(3L)
                        .setMin(100L)
                        .setMax(900L)
                        .setNullCount(2L)
                        .build());
        expectedColStats.put(
                "c",
                ColumnStats.Builder.builder()
                        .setNdv(3L)
                        .setMin(BinaryString.fromString("S1"))
                        .setMax(BinaryString.fromString("S7"))
                        .setNullCount(2L)
                        .build());
        Assertions.assertThat(partitionFilterSource.reportStatistics().getColumnStats())
                .isEqualTo(expectedColStats);
    }

    @Test
    public void testTableFilterKeyStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource keyFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.equal(1, 50),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(keyFilterSource.reportStatistics().getRowCount()).isEqualTo(2L);
        Map<String, ColStats<?>> colStatsMap = new HashMap<>();
        colStatsMap.put("pt", ColStats.newColStats(0, 1L, 2, 2, 0L, null, null));
        colStatsMap.put("a", ColStats.newColStats(1, 1L, 50, 50, 0L, null, null));
        colStatsMap.put("b", ColStats.newColStats(2, 1L, null, null, 1L, null, null));
        colStatsMap.put(
                "c",
                ColStats.newColStats(
                        3,
                        1L,
                        BinaryString.fromString("S5"),
                        BinaryString.fromString("S5"),
                        0L,
                        null,
                        null));

        FileStore<?> fileStore = table.store();
        FileStoreCommit fileStoreCommit = fileStore.newCommit(commitUser);
        Snapshot latestSnapshot = fileStore.snapshotManager().latestSnapshot();
        Statistics colStats =
                new Statistics(
                        latestSnapshot.id(), latestSnapshot.schemaId(), 9L, null, colStatsMap);
        fileStoreCommit.commitStatistics(colStats, Long.MAX_VALUE);
        fileStoreCommit.close();

        Map<String, ColumnStats> expectedColStats = new HashMap<>();
        expectedColStats.put(
                "pt",
                ColumnStats.Builder.builder()
                        .setNdv(1L)
                        .setMin(2)
                        .setMax(2)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "a",
                ColumnStats.Builder.builder()
                        .setNdv(1L)
                        .setMin(50)
                        .setMax(50)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "b",
                ColumnStats.Builder.builder()
                        .setNdv(1L)
                        .setMin(null)
                        .setMax(null)
                        .setNullCount(1L)
                        .build());
        expectedColStats.put(
                "c",
                ColumnStats.Builder.builder()
                        .setNdv(1L)
                        .setMin(BinaryString.fromString("S5"))
                        .setMax(BinaryString.fromString("S5"))
                        .setNullCount(0L)
                        .build());
        Assertions.assertThat(keyFilterSource.reportStatistics().getColumnStats())
                .isEqualTo(expectedColStats);
    }

    @Test
    public void testTableFilterValueStatistics() throws Exception {
        FileStoreTable table = writeData();
        PredicateBuilder builder = new PredicateBuilder(table.schema().logicalRowType());
        DataTableSource keyFilterSource =
                new DataTableSource(
                        identifier,
                        table,
                        false,
                        null,
                        null,
                        builder.greaterThan(2, 500L),
                        null,
                        null,
                        null,
                        null,
                        false);
        Assertions.assertThat(keyFilterSource.reportStatistics().getRowCount()).isEqualTo(4L);
        Map<String, ColStats<?>> colStatsMap = new HashMap<>();
        colStatsMap.put("pt", ColStats.newColStats(0, 4L, 2, 2, 0L, null, null));
        colStatsMap.put("a", ColStats.newColStats(1, 4L, 50, 50, 0L, null, null));
        colStatsMap.put("b", ColStats.newColStats(2, 4L, null, null, 1L, null, null));
        colStatsMap.put(
                "c",
                ColStats.newColStats(
                        3,
                        4L,
                        BinaryString.fromString("S5"),
                        BinaryString.fromString("S8"),
                        0L,
                        null,
                        null));

        FileStore<?> fileStore = table.store();
        FileStoreCommit fileStoreCommit = fileStore.newCommit(commitUser);
        Snapshot latestSnapshot = fileStore.snapshotManager().latestSnapshot();
        Statistics colStats =
                new Statistics(
                        latestSnapshot.id(), latestSnapshot.schemaId(), 9L, null, colStatsMap);
        fileStoreCommit.commitStatistics(colStats, Long.MAX_VALUE);
        fileStoreCommit.close();

        Map<String, ColumnStats> expectedColStats = new HashMap<>();
        expectedColStats.put(
                "pt",
                ColumnStats.Builder.builder()
                        .setNdv(4L)
                        .setMin(2)
                        .setMax(2)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "a",
                ColumnStats.Builder.builder()
                        .setNdv(4L)
                        .setMin(50)
                        .setMax(50)
                        .setNullCount(0L)
                        .build());
        expectedColStats.put(
                "b",
                ColumnStats.Builder.builder()
                        .setNdv(4L)
                        .setMin(null)
                        .setMax(null)
                        .setNullCount(1L)
                        .build());
        expectedColStats.put(
                "c",
                ColumnStats.Builder.builder()
                        .setNdv(4L)
                        .setMin(BinaryString.fromString("S5"))
                        .setMax(BinaryString.fromString("S8"))
                        .setNullCount(0L)
                        .build());
        Assertions.assertThat(keyFilterSource.reportStatistics().getColumnStats())
                .isEqualTo(expectedColStats);
    }

    protected FileStoreTable writeData() throws Exception {
        FileStoreTable table = createStoreTable();

        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L, "S1"));
        write.write(rowData(1, 20, 200L, null));
        write.write(rowData(2, 30, 300L, "S3"));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(1, 40, 400L, "S4"));
        write.write(rowData(2, 50, null, "S5"));
        write.write(rowData(2, 60, 600L, "S6"));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(1, 70, 700L, "S7"));
        write.write(rowData(2, 80, null, "S8"));
        write.write(rowData(1, 90, 900L, null));
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        return table;
    }

    abstract FileStoreTable createStoreTable() throws Exception;

    protected InternalRow rowData(Object... values) {
        return GenericRow.of(
                Arrays.stream(values)
                        .map(
                                v -> {
                                    if (v instanceof String) {
                                        return BinaryString.fromString((String) v);
                                    } else {
                                        return v;
                                    }
                                })
                        .toArray());
    }

    protected Schema.Builder schemaBuilder() {
        return Schema.newBuilder()
                .column("pt", new IntType())
                .column("a", new IntType())
                .column("b", new BigIntType(true))
                .column("c", new VarCharType(100));
    }
}
