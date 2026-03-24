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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SchemaEvolutionTableTestBase.rowData;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FallbackReadFileStoreTable}. */
public class FallbackReadFileStoreTableTest {
    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(), DataTypes.INT(),
                    },
                    new String[] {"pt", "a"});

    @TempDir java.nio.file.Path tempDir;

    private Path tablePath;

    private String commitUser;

    private FileIO fileIO;

    @BeforeEach
    public void before() {
        tablePath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());
        commitUser = UUID.randomUUID().toString();
        fileIO = FileIOFinder.find(tablePath);
    }

    @Test
    public void testListPartitions() throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(3, 60));

        FallbackReadFileStoreTable fallbackTable =
                new FallbackReadFileStoreTable(mainTable, branchTable);

        List<Integer> partitionsFromMainTable =
                mainTable.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        assertThat(partitionsFromMainTable).containsExactlyInAnyOrder(1, 2);

        List<Integer> partitionsFromFallbackTable =
                fallbackTable.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        // this should contain all partitions
        assertThat(partitionsFromFallbackTable).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    public void testListPartitionEntries() throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(1, 30), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(1, 50), rowData(3, 60), rowData(4, 70));

        FallbackReadFileStoreTable fallbackTable =
                new FallbackReadFileStoreTable(mainTable, branchTable);

        List<PartitionEntry> partitionEntries = mainTable.newScan().listPartitionEntries();
        assertThat(partitionEntries)
                .map(e -> e.partition().getInt(0))
                .containsExactlyInAnyOrder(1, 2);

        List<PartitionEntry> fallbackPartitionEntries =
                fallbackTable.newScan().listPartitionEntries();
        assertThat(fallbackPartitionEntries)
                .map(e -> Pair.of(e.partition().getInt(0), e.recordCount()))
                .containsExactlyInAnyOrder(
                        Pair.of(1, 2L), Pair.of(2, 1L), Pair.of(3, 1L), Pair.of(4, 1L));
    }

    /**
     * Test that FallbackReadScan.plan() determines partition ownership based on partition
     * predicates only, not mixed with data filters. If a partition exists in the main branch, it
     * should never be read from fallback, regardless of the data filter.
     *
     * <p>Without the fix, the old code built completePartitions from mainScan.plan() results which
     * already had data filters applied. When the data filter excluded all files of a main partition
     * via filterByStats, that partition was incorrectly treated as "not in main" and read from
     * fallback.
     */
    @Test
    public void testPlanWithDataFilter() throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // Main branch: partition 1 (a=10), partition 2 (a=20)
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // Fallback branch: partition 1 already has a=10 (inherited), add a=100.
        // Also add partition 3 (a=30) which is fallback-only.
        writeDataIntoTable(branchTable, 1, rowData(1, 100), rowData(3, 30));

        FallbackReadFileStoreTable fallbackTable =
                new FallbackReadFileStoreTable(mainTable, branchTable);
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);

        // Case 1: WHERE pt = 1 AND a = 100
        // Partition 1 exists in main branch. Even though main has no a=100 data,
        // we should never fall back for it. The result should contain no fallback splits.
        DataTableScan scan1 = fallbackTable.newScan();
        scan1.withFilter(PredicateBuilder.and(builder.equal(0, 1), builder.equal(1, 100)));
        List<Split> splits1 = scan1.plan().splits();

        for (Split split : splits1) {
            FallbackReadFileStoreTable.FallbackSplit fs =
                    (FallbackReadFileStoreTable.FallbackSplit) split;
            assertThat(fs.isFallback())
                    .as("Partition that exists in main branch should never be read from fallback")
                    .isFalse();
        }

        // Case 2: WHERE pt = 3 AND a = 30
        // Partition 3 only exists in fallback branch, so it should be read from fallback.
        DataTableScan scan2 = fallbackTable.newScan();
        scan2.withFilter(PredicateBuilder.and(builder.equal(0, 3), builder.equal(1, 30)));
        List<Split> splits2 = scan2.plan().splits();

        assertThat(splits2).hasSize(1);
        FallbackReadFileStoreTable.FallbackSplit fs2 =
                (FallbackReadFileStoreTable.FallbackSplit) splits2.get(0);
        assertThat(fs2.isFallback())
                .as("Partition that only exists in fallback branch should be read from fallback")
                .isTrue();
    }

    /**
     * Reproduces issue #7503: when main branch has more partition keys (e.g. dt, t1) than the
     * fallback branch (e.g. dt only), a query predicate containing extra keys (t1) must not be
     * pushed to the fallback scan, and the partition-ownership comparison must use the fallback key
     * layout so that fallback-only partitions are correctly identified and read.
     *
     * <p>Setup:
     *
     * <ul>
     *   <li>Main table: PARTITIONED BY (dt, a) — simulates t1 with multi-key partition
     *   <li>Fallback table: PARTITIONED BY (dt) — simulates branch_snapshot with fewer keys
     *   <li>Main (delta): dt=20250811, a=aaa → row (20250811, aaa, x_new)
     *   <li>Fallback (snapshot): dt=20250810 → rows (20250810, aaa, x), (20250810, bbb, y)
     * </ul>
     *
     * <p>Query: WHERE dt=20250811. Expected: fallback dt=20250810 data is NOT returned (not in
     * predicate), delta dt=20250811 data IS returned. The extra key "a" in the predicate must be
     * stripped before pushing to fallback to avoid incorrect filtering.
     *
     * <p>Query: WHERE dt=20250810. Expected: no delta data, fallback dt=20250810 data IS returned.
     * Previously the bug caused fallback to never be read (because BinaryRow {dt,a} never equals
     * BinaryRow {dt}, so all fallback partitions appeared "remaining" but the predicate stripped
     * them; or they were incorrectly filtered by the extra key).
     */
    @Test
    public void testFallbackWithSubsetPartitionKeys() throws Exception {
        // Row type shared by both tables: columns dt, a, val
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"dt", "a", "val"});

        // Main table: partitioned by (dt, a)
        Path mainPath = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString() + "/main");
        FileIO mainFileIO = FileIOFinder.find(mainPath);
        TableSchema mainSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), mainPath),
                        new Schema(
                                rowType.getFields(),
                                Arrays.asList("dt", "a"),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                ""));
        AppendOnlyFileStoreTable mainTable =
                new AppendOnlyFileStoreTable(mainFileIO, mainPath, mainSchema);

        // Fallback table: partitioned by (dt) only
        Path fallbackPath =
                new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString() + "/fallback");
        FileIO fallbackFileIO = FileIOFinder.find(fallbackPath);
        TableSchema fallbackSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), fallbackPath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("dt"),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                ""));
        AppendOnlyFileStoreTable fallbackTable =
                new AppendOnlyFileStoreTable(fallbackFileIO, fallbackPath, fallbackSchema);

        // Delta (main): dt=20250811, a=aaa
        writeDataIntoTable(
                mainTable,
                0,
                GenericRow.of(
                        org.apache.paimon.data.BinaryString.fromString("20250811"),
                        org.apache.paimon.data.BinaryString.fromString("aaa"),
                        org.apache.paimon.data.BinaryString.fromString("x_new")));

        // Snapshot (fallback): dt=20250810, a=aaa and a=bbb
        writeDataIntoTable(
                fallbackTable,
                0,
                GenericRow.of(
                        org.apache.paimon.data.BinaryString.fromString("20250810"),
                        org.apache.paimon.data.BinaryString.fromString("aaa"),
                        org.apache.paimon.data.BinaryString.fromString("x")),
                GenericRow.of(
                        org.apache.paimon.data.BinaryString.fromString("20250810"),
                        org.apache.paimon.data.BinaryString.fromString("bbb"),
                        org.apache.paimon.data.BinaryString.fromString("y")));

        FallbackReadFileStoreTable combined =
                new FallbackReadFileStoreTable(mainTable, fallbackTable);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        // Case 1: WHERE dt=20250811 — main owns dt=20250811; must have at least one non-fallback
        // split.
        DataTableScan scan1 = combined.newScan();
        scan1.withFilter(
                builder.equal(0, org.apache.paimon.data.BinaryString.fromString("20250811")));
        List<Split> splits1 = scan1.plan().splits();
        assertThat(splits1).isNotEmpty();
        boolean hasMainSplit1 = false;
        for (Split s : splits1) {
            if (!((FallbackReadFileStoreTable.FallbackSplit) s).isFallback()) {
                hasMainSplit1 = true;
            }
        }
        assertThat(hasMainSplit1)
                .as("dt=20250811 owned by main; must have at least one non-fallback split")
                .isTrue();

        // Case 2: WHERE dt=20250810 — only fallback data; main has no dt=20250810
        DataTableScan scan2 = combined.newScan();
        scan2.withFilter(
                builder.equal(0, org.apache.paimon.data.BinaryString.fromString("20250810")));
        List<Split> splits2 = scan2.plan().splits();
        // dt=20250810 not in main; must be read from fallback
        assertThat(splits2).isNotEmpty();
        boolean hasFallback = false;
        for (Split s : splits2) {
            if (((FallbackReadFileStoreTable.FallbackSplit) s).isFallback()) {
                hasFallback = true;
            }
        }
        assertThat(hasFallback)
                .as(
                        "dt=20250810 exists only in fallback; extra key 'a' must not prevent "
                                + "fallback from being found (Bug #7503)")
                .isTrue();
    }

    private void writeDataIntoTable(
            FileStoreTable table, long commitIdentifier, InternalRow... allData) throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        for (InternalRow data : allData) {
            write.write(data);
        }

        commit.commit(commitIdentifier, write.prepareCommit(false, commitIdentifier));
        write.close();
        commit.close();
    }

    private AppendOnlyFileStoreTable createTable() throws Exception {
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                ""));
        return new AppendOnlyFileStoreTable(fileIO, tablePath, tableSchema);
    }

    private FileStoreTable createTableFromBranch(FileStoreTable baseTable, String branchName) {
        Options options = new Options(baseTable.options());
        options.set(CoreOptions.BRANCH, branchName);
        return new AppendOnlyFileStoreTable(
                        fileIO,
                        tablePath,
                        new SchemaManager(fileIO, tablePath, branchName).latest().get())
                .copy(options.toMap());
    }
}
