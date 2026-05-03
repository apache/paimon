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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.table.SchemaEvolutionTableTestBase.rowData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testListPartitions(boolean wrappedFirst) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(3, 60));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst);

        List<Integer> partitions =
                table.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        // this should contain all partitions
        assertThat(partitions).containsExactlyInAnyOrder(1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testListPartitionEntries(boolean wrappedFirst) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(1, 30), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(1, 50), rowData(3, 60), rowData(4, 70));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst);

        List<PartitionEntry> entries = table.newScan().listPartitionEntries();
        // partition 1 exists in both: record count depends on which table has priority
        // wrappedFirst=true → mainTable has priority (2 records), false → branchTable (1 record)
        long expectedPt1Count = wrappedFirst ? 2L : 1L;
        assertThat(entries)
                .map(e -> Pair.of(e.partition().getInt(0), e.recordCount()))
                .containsExactlyInAnyOrder(
                        Pair.of(1, expectedPt1Count),
                        Pair.of(2, 1L),
                        Pair.of(3, 1L),
                        Pair.of(4, 1L));
    }

    /**
     * Test that FallbackReadScan.plan() determines partition ownership based on partition
     * predicates only, not mixed with data filters. If a partition exists in the priority table, it
     * should never be read from the supplemental table, regardless of the data filter.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPlanWithDataFilter(boolean wrappedFirst) throws Exception {
        String branchName = "bc";
        InternalRow[] firstValues = new InternalRow[] {rowData(1, 10), rowData(2, 20)};
        InternalRow[] secondValues = new InternalRow[] {rowData(1, 100), rowData(3, 30)};

        FileStoreTable mainTable = createTable();

        // Main branch: partition 1 (a=10), partition 2 (a=20)
        writeDataIntoTable(mainTable, 0, wrappedFirst ? firstValues : secondValues);

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // Fallback branch: partition 1 already has a=10 (inherited), add a=100.
        // Also add partition 3 (a=30) which is fallback-only.
        writeDataIntoTable(branchTable, 1, wrappedFirst ? secondValues : firstValues);

        FallbackReadFileStoreTable fallbackTable =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst);
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testWriteGoesToWrapped(boolean wrappedFirst) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 for main
        writeDataIntoTable(mainTable, 0, rowData(1, 10));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition 2 for branch
        writeDataIntoTable(branchTable, 0, rowData(2, 20));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst);

        // write through the merged table — should go to mainTable (wrapped)
        writeDataIntoTable(table, 1, rowData(3, 30));

        // verify: main branch should now have partition 1 and 3
        List<Integer> mainPartitions =
                mainTable.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        assertThat(mainPartitions).containsExactlyInAnyOrder(1, 3);

        // verify: branch should still only have partition 2
        List<Integer> branchPartitions =
                branchTable.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        assertThat(branchPartitions).containsExactlyInAnyOrder(2);

        // verify: merged read should see all three partitions
        List<Integer> mergedPartitions =
                table.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        assertThat(mergedPartitions).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    public void testFallbackReadFailFastDefaultSwallowsException() throws Exception {
        FallbackReadFileStoreTable table = setUpTableWithThrowingFallback(false);
        Split split = onlyFallbackSplit(table);

        // Default behavior: the failing fallback read is swallowed and the reader
        // falls through to the main branch, which has no data for partition 3 and
        // either returns an empty reader or throws something other than the
        // injected fallback exception.
        try {
            table.newRead().createReader(split);
        } catch (Exception e) {
            assertThat(e.getMessage())
                    .as("fallback exception must not propagate when fail-fast is disabled")
                    .doesNotContain("injected fallback failure");
        }
    }

    @Test
    public void testFallbackReadFailFastPropagatesException() throws Exception {
        FallbackReadFileStoreTable table = setUpTableWithThrowingFallback(true);
        Split split = onlyFallbackSplit(table);

        assertThatThrownBy(() -> table.newRead().createReader(split))
                .hasMessageContaining("injected fallback failure");
    }

    private FallbackReadFileStoreTable setUpTableWithThrowingFallback(boolean failFast)
            throws Exception {
        String branchName = "bc";
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch(branchName);
        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);
        writeDataIntoTable(branchTable, 0, rowData(3, 60));

        Options overrides = new Options();
        overrides.set(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST, failFast);
        FileStoreTable mainWithOption = mainTable.copy(overrides.toMap());

        FileStoreTable spyBranch = Mockito.spy(branchTable);
        InnerTableRead throwing = throwingInnerTableRead();
        Mockito.doReturn(throwing).when(spyBranch).newRead();

        return new FallbackReadFileStoreTable(mainWithOption, spyBranch, true);
    }

    private static Split onlyFallbackSplit(FallbackReadFileStoreTable table) {
        DataTableScan scan = table.newScan();
        scan.withFilter(new PredicateBuilder(ROW_TYPE).equal(0, 3));
        List<Split> splits = scan.plan().splits();
        assertThat(splits).hasSize(1);
        FallbackReadFileStoreTable.FallbackSplit fs =
                (FallbackReadFileStoreTable.FallbackSplit) splits.get(0);
        assertThat(fs.isFallback()).isTrue();
        return splits.get(0);
    }

    private static InnerTableRead throwingInnerTableRead() {
        return new InnerTableRead() {
            @Override
            public InnerTableRead withFilter(Predicate predicate) {
                return this;
            }

            @Override
            public InnerTableRead withReadType(RowType readType) {
                return this;
            }

            @Override
            public TableRead withIOManager(org.apache.paimon.disk.IOManager ioManager) {
                return this;
            }

            @Override
            public org.apache.paimon.reader.RecordReader<InternalRow> createReader(Split split)
                    throws IOException {
                throw new IOException("injected fallback failure");
            }
        };
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
