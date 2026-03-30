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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testListPartitions(boolean primaryMode) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(3, 60));

        FileStoreTable wrapped = primaryMode ? branchTable : mainTable;
        FileStoreTable fallback = primaryMode ? mainTable : branchTable;
        FallbackReadFileStoreTable table = new FallbackReadFileStoreTable(wrapped, fallback);

        List<Integer> partitions =
                table.newScan().listPartitions().stream()
                        .map(row -> row.getInt(0))
                        .collect(Collectors.toList());
        // this should contain all partitions
        assertThat(partitions).containsExactlyInAnyOrder(1, 2, 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testListPartitionEntries(boolean primaryMode) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // write data into partition 1 and 2.
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(1, 30), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // write data into partition for branch only
        writeDataIntoTable(branchTable, 0, rowData(1, 50), rowData(3, 60), rowData(4, 70));

        FileStoreTable wrapped = primaryMode ? branchTable : mainTable;
        FileStoreTable fallback = primaryMode ? mainTable : branchTable;
        FallbackReadFileStoreTable table = new FallbackReadFileStoreTable(wrapped, fallback);

        List<PartitionEntry> entries = table.newScan().listPartitionEntries();
        // partition 1 exists in both: record count depends on which table has priority
        long expectedPt1Count = primaryMode ? 1L : 2L;
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
     * predicates only, not mixed with data filters. If a partition exists in the wrapped (priority)
     * table, it should never be read from fallback, regardless of the data filter.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testPlanWithDataFilter(boolean primaryMode) throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();

        // Main branch: partition 1 (a=10), partition 2 (a=20)
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);

        // Branch: partition 1 (a=100), partition 3 (a=30)
        writeDataIntoTable(branchTable, 0, rowData(1, 100), rowData(3, 30));

        FileStoreTable wrapped = primaryMode ? branchTable : mainTable;
        FileStoreTable fallback = primaryMode ? mainTable : branchTable;
        FallbackReadFileStoreTable table = new FallbackReadFileStoreTable(wrapped, fallback);
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);

        // partition 1 exists in both tables, wrapped has priority
        int partOnlyInFallback = primaryMode ? 2 : 3;

        // Case 1: WHERE pt = 1
        // Partition 1 exists in wrapped. Should never be read from fallback.
        DataTableScan scan1 = table.newScan();
        scan1.withFilter(builder.equal(0, 1));
        List<Split> splits1 = scan1.plan().splits();

        assertThat(splits1).isNotEmpty();
        for (Split split : splits1) {
            FallbackReadFileStoreTable.FallbackSplit fs =
                    (FallbackReadFileStoreTable.FallbackSplit) split;
            assertThat(fs.isFallback())
                    .as("Partition in wrapped table should not be read from fallback")
                    .isFalse();
        }

        // Case 2: partition only in fallback → should have isFallback=true
        DataTableScan scan2 = table.newScan();
        scan2.withFilter(builder.equal(0, partOnlyInFallback));
        List<Split> splits2 = scan2.plan().splits();

        assertThat(splits2).hasSize(1);
        FallbackReadFileStoreTable.FallbackSplit fs2 =
                (FallbackReadFileStoreTable.FallbackSplit) splits2.get(0);
        assertThat(fs2.isFallback())
                .as("Partition only in fallback table should be read from fallback")
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
