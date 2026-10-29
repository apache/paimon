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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.QueryAuthSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
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

    @Test
    public void testScanForwardsReadType() {
        FileStoreTable mainTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable fallbackTable = Mockito.mock(FileStoreTable.class);
        DataTableScan mainScan = Mockito.mock(DataTableScan.class);
        DataTableScan fallbackScan = Mockito.mock(DataTableScan.class);
        RowType readType = ROW_TYPE.project("a");

        FallbackReadFileStoreTable.FallbackReadScan scan =
                new FallbackReadFileStoreTable.FallbackReadScan(
                        mainTable,
                        fallbackTable,
                        Mockito.mock(TableSchema.class),
                        table -> table == mainTable ? mainScan : fallbackScan);

        assertThat(scan.withReadType(readType)).isSameAs(scan);
        Mockito.verify(mainScan).withReadType(readType);
        Mockito.verify(fallbackScan).withReadType(readType);
    }

    @ParameterizedTest
    @CsvSource({
        "scan.fallback-branch,pt,false",
        "scan.primary-branch,pt,false",
        "scan.fallback-branch,a,false",
        "scan.primary-branch,a,false",
        "scan.fallback-branch,pt,true",
        "scan.primary-branch,pt,true",
        "scan.fallback-branch,a,true",
        "scan.primary-branch,a,true"
    })
    void testQueryAuthWithProjectedColumns(
            String branchOption, String projectedColumn, boolean dataEvolution) throws Exception {
        FileStoreTable localTable = createTable(dataEvolution);
        localTable.createBranch("backup");
        writeDataIntoTable(localTable, 0, rowData(1, 10), rowData(3, 50));
        writeDataIntoTable(
                createTableFromBranch(localTable, "backup"),
                0,
                rowData(1, 20),
                rowData(1, 30),
                rowData(2, 40));

        List<String> allowedColumns = new ArrayList<>(Collections.singletonList(projectedColumn));
        Catalog catalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.authTableQuery(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation -> {
                            List<String> select = invocation.getArgument(1);
                            if (select == null || !allowedColumns.containsAll(select)) {
                                throw new SecurityException(
                                        "No permission to read unselected columns");
                            }
                            return new TableQueryAuthResult(null, null);
                        });
        Mockito.when(catalog.loadSnapshot(Mockito.any(Identifier.class)))
                .thenThrow(new UnsupportedOperationException());
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("mydb", "mytable"),
                        null,
                        () -> catalog,
                        null,
                        null,
                        null,
                        false,
                        false);
        Options options = new Options();
        options.set(CoreOptions.QUERY_AUTH_ENABLED, true);
        options.set(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST, true);
        options.set(branchOption, "backup");
        FileStoreTable table =
                FileStoreTableFactory.create(
                        fileIO, tablePath, localTable.schema(), options, environment);
        ReadBuilder readBuilder =
                table.newReadBuilder().withReadType(table.rowType().project(projectedColumn));
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan().splits())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0)));
        }
        List<Integer> expected;
        if ("pt".equals(projectedColumn)) {
            expected =
                    "scan.primary-branch".equals(branchOption)
                            ? Arrays.asList(1, 1, 2, 3)
                            : Arrays.asList(1, 2, 3);
        } else {
            expected =
                    "scan.primary-branch".equals(branchOption)
                            ? Arrays.asList(20, 30, 40, 50)
                            : Arrays.asList(10, 40, 50);
        }
        assertThat(rows).containsExactlyInAnyOrderElementsOf(expected);
        assertThat(readBuilder.newScan().listPartitionEntries()).hasSize(3);

        String deniedColumn = "pt".equals(projectedColumn) ? "a" : "pt";
        assertThatThrownBy(
                        () ->
                                table.newReadBuilder()
                                        .withReadType(table.rowType().project(deniedColumn))
                                        .newScan()
                                        .plan())
                .hasMessageContaining("No permission");
        readBuilder.withFilter(
                new PredicateBuilder(table.rowType())
                        .equal(table.rowType().getFieldIndex(deniedColumn), 1));
        assertThatThrownBy(() -> readBuilder.newScan().plan())
                .hasMessageContaining("No permission");
        assertThatThrownBy(() -> readBuilder.newScan().listPartitionEntries())
                .hasMessageContaining("No permission");
        if ("a".equals(projectedColumn)) {
            DataTableScan internalScan = table.newScan();
            internalScan.withReadType(table.rowType().project("a"));
            internalScan.withInternalPartitionFilter(
                    PartitionPredicate.fromPredicate(
                            table.schema().logicalPartitionType(),
                            new PredicateBuilder(table.schema().logicalPartitionType())
                                    .equal(0, 2)));
            assertThat(internalScan.listPartitions())
                    .extracting(partition -> partition.getInt(0))
                    .containsExactly(2);
            List<Integer> internalRows = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    table.newReadBuilder()
                            .withReadType(table.rowType().project("a"))
                            .newRead()
                            .createReader(internalScan.plan().splits())) {
                reader.forEachRemaining(row -> internalRows.add(row.getInt(0)));
            }
            assertThat(internalRows).containsExactly(40);

            ReadBuilder partitionFiltered =
                    table.newReadBuilder()
                            .withReadType(table.rowType().project("a"))
                            .withPartitionFilter(Collections.singletonMap("pt", "1"));
            assertThatThrownBy(() -> partitionFiltered.newScan().plan())
                    .hasMessageContaining("No permission");
            assertThatThrownBy(() -> partitionFiltered.newScan().listPartitionEntries())
                    .hasMessageContaining("No permission");

            // Preserve user operands in both the underlying scan and newly created wrapper scans.
            FileStoreTable underlyingTable =
                    FileStoreTableFactory.createWithoutFallbackBranch(
                            fileIO, tablePath, localTable.schema(), options, environment);
            for (DataTableScan scan : Arrays.asList(underlyingTable.newScan(), table.newScan())) {
                scan.withReadType(table.rowType().project("a"));
                scan.withPartitionFilter(Collections.singletonMap("pt", "1"));
                scan.withInternalPartitionFilter(PartitionPredicate.ALWAYS_TRUE);
                assertThatThrownBy(scan::plan).hasMessageContaining("No permission");
                assertThatThrownBy(scan::listPartitionEntries)
                        .hasMessageContaining("No permission");

                // Unlike internal planning, a new user filter replaces the old user operands.
                scan.withPartitionFilter(PartitionPredicate.ALWAYS_TRUE);
                assertThat(scan.plan().splits()).isNotEmpty();
                assertThat(scan.listPartitionEntries()).isNotEmpty();
                scan.withPartitionFilter(
                        PartitionPredicate.fromPredicate(
                                table.schema().logicalPartitionType(),
                                new PredicateBuilder(table.schema().logicalPartitionType())
                                        .equal(0, 1)));
                assertThatThrownBy(scan::plan).hasMessageContaining("No permission");
                assertThatThrownBy(scan::listPartitionEntries)
                        .hasMessageContaining("No permission");
            }
        }

        if ("pt".equals(projectedColumn)) {
            allowedColumns.add("a");
            // A value filter must be authorized without affecting which branch owns a partition.
            // In fallback mode, a = 20 in backup must not replace a = 10 in main's pt = 1.
            ReadBuilder valueFiltered =
                    table.newReadBuilder()
                            .withReadType(table.rowType().project("pt"))
                            .withFilter(new PredicateBuilder(table.rowType()).equal(1, 20));
            assertThat(valueFiltered.newScan().listPartitionEntries()).hasSize(3);
            List<Integer> filteredRows = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    valueFiltered
                            .newRead()
                            .executeFilter()
                            .createReader(valueFiltered.newScan().plan().splits())) {
                reader.forEachRemaining(row -> filteredRows.add(row.getInt(0)));
            }
            assertThat(filteredRows)
                    .containsExactlyElementsOf(
                            "scan.primary-branch".equals(branchOption)
                                    ? Collections.singletonList(1)
                                    : Collections.emptyList());
        }
    }

    @Test
    public void testPlanAndReadWithQueryAuthSplit() throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));

        mainTable.createBranch("bc");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "bc");
        writeDataIntoTable(branchTable, 0, rowData(2, 20));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, true);
        TableQueryAuthResult authResult =
                new TableQueryAuthResult(
                        null,
                        Collections.singletonMap(
                                "a",
                                JsonSerdeUtil.toFlatJson(
                                        new FieldTransform(
                                                new FieldRef(0, "pt", DataTypes.INT())))));
        DataTableScan scan =
                table.newFallbackScan(
                        fileStoreTable -> queryAuthScan(fileStoreTable.newScan(), authResult));

        List<Split> splits = scan.plan().splits();
        assertThat(splits).hasSize(2);
        assertThat(splits)
                .allSatisfy(
                        split -> {
                            assertThat(split)
                                    .isInstanceOf(FallbackReadFileStoreTable.FallbackSplit.class);
                            Split wrapped =
                                    ((FallbackReadFileStoreTable.FallbackSplit) split).wrapped();
                            assertThat(wrapped).isInstanceOf(QueryAuthSplit.class);
                        });

        List<Pair<Integer, Integer>> result = new ArrayList<>();
        for (Split split : splits) {
            Split deserialized =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(split), getClass().getClassLoader());
            RecordReader<InternalRow> reader = table.newRead().createReader(deserialized);
            reader.forEachRemaining(r -> result.add(Pair.of(r.getInt(0), r.getInt(1))));
            reader.close();
        }

        assertThat(result).containsExactlyInAnyOrder(Pair.of(1, 1), Pair.of(2, 2));
    }

    @Test
    public void testScanVersionForFallbackBranch() throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createTag("base");

        writeDataIntoTable(mainTable, 1, rowData(2, 20));
        long mainSnapshotTime = mainTable.snapshotManager().snapshot(2).timeMillis();
        mainTable.createBranch("bc", "base");
        while (System.currentTimeMillis() <= mainSnapshotTime) {
            Thread.yield();
        }
        FileStoreTable branchTable = mainTable.switchToBranch("bc");
        writeDataIntoTable(branchTable, 2, rowData(3, 30));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, true);
        FileStoreTable versioned =
                table.copy(Collections.singletonMap(CoreOptions.SCAN_VERSION.key(), "2"));

        assertThat(readAndCollect((FallbackReadFileStoreTable) versioned, scan -> {}))
                .containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(2, 20));
    }

    @ParameterizedTest
    @CsvSource({
        "false,false,false", "false,false,true", "false,true,false", "false,true,true",
        "true,false,false", "true,false,true", "true,true,false", "true,true,true"
    })
    public void testTimeTravelWithEmptyOtherBranch(
            boolean wrappedFirst, boolean scanVersion, boolean allowEarliest) throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch("empty");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "empty");
        assertThat(branchTable.snapshotManager().latestSnapshotId()).isNull();
        Map<String, String> options = new HashMap<>();
        options.put(
                scanVersion ? CoreOptions.SCAN_VERSION.key() : CoreOptions.SCAN_SNAPSHOT_ID.key(),
                "1");
        options.put(
                CoreOptions.SCAN_FALLBACK_BRANCH_ALLOW_EARLIEST_SNAPSHOT.key(),
                String.valueOf(allowEarliest));

        FallbackReadFileStoreTable table =
                (FallbackReadFileStoreTable)
                        new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst)
                                .copy(options);

        // Copying and serialization must retain the empty time-travel result.
        for (FileStoreTable copy :
                Arrays.asList(
                        table,
                        table.copy(Collections.emptyMap()),
                        table.copyWithoutTimeTravel(Collections.emptyMap()),
                        table.copy(table.schema()),
                        table.copyWithLatestSchema(),
                        InstantiationUtil.deserializeObject(
                                InstantiationUtil.serializeObject(table),
                                getClass().getClassLoader()))) {
            assertThat(readAndCollect((FallbackReadFileStoreTable) copy, scan -> {}))
                    .containsExactly(Pair.of(1, 10));
            assertThat(copy.newScan().listPartitionEntries())
                    .map(entry -> entry.partition().getInt(0))
                    .containsExactly(1);
        }
    }

    @Test
    public void testTimeTravelWithEmptyTable() throws Exception {
        FileStoreTable table =
                createTable()
                        .copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));

        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("There is currently no snapshot.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLatestWithEmptyOtherBranch(boolean wrappedFirst) throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch("empty");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "empty");
        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst);

        assertThat(readAndCollect(table, scan -> {})).containsExactly(Pair.of(1, 10));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testTimeTravelBeforeOtherBranchFirstSnapshot(
            boolean wrappedFirst, boolean allowEarliest) throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        long snapshotTime = mainTable.snapshotManager().snapshot(1).timeMillis();
        mainTable.createBranch("later");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "later");
        while (System.currentTimeMillis() <= snapshotTime) {
            Thread.yield();
        }
        writeDataIntoTable(branchTable, 0, rowData(1, 100), rowData(2, 20));
        assertThat(branchTable.snapshotManager().snapshot(1).timeMillis())
                .isGreaterThan(snapshotTime);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        if (allowEarliest) {
            options.put(CoreOptions.SCAN_FALLBACK_BRANCH_ALLOW_EARLIEST_SNAPSHOT.key(), "true");
        }
        FallbackReadFileStoreTable table =
                (FallbackReadFileStoreTable)
                        new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst)
                                .copy(options);

        if (allowEarliest) {
            assertThat(readAndCollect(table, scan -> {}))
                    .containsExactlyInAnyOrder(Pair.of(1, wrappedFirst ? 10 : 100), Pair.of(2, 20));
            assertThat(table.newScan().listPartitionEntries())
                    .map(entry -> entry.partition().getInt(0))
                    .containsExactlyInAnyOrder(1, 2);
        } else {
            assertThatThrownBy(() -> table.newScan().plan())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("No snapshot at or before")
                    .hasMessageContaining(
                            CoreOptions.SCAN_FALLBACK_BRANCH_ALLOW_EARLIEST_SNAPSHOT.key());
        }

        writeDataIntoTable(mainTable, 1, rowData(3, 30));
        FallbackReadFileStoreTable later =
                (FallbackReadFileStoreTable)
                        table.copy(
                                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "2"));
        assertThat(readAndCollect(later, scan -> {}))
                .containsExactlyInAnyOrder(
                        Pair.of(1, wrappedFirst ? 10 : 100), Pair.of(2, 20), Pair.of(3, 30));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testTimeTravelWithEmptyBranchOption(boolean wrappedFirst) throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch("empty");
        Options options = new Options();
        options.set(
                wrappedFirst ? CoreOptions.SCAN_FALLBACK_BRANCH : CoreOptions.SCAN_PRIMARY_BRANCH,
                "empty");
        FileStoreTable table =
                FileStoreTableFactory.create(
                                fileIO,
                                tablePath,
                                mainTable.schema(),
                                options,
                                CatalogEnvironment.empty())
                        .copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));

        assertThat(readAndCollect((FallbackReadFileStoreTable) table, scan -> {}))
                .containsExactly(Pair.of(1, 10));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testTimeTravelWithExpiredOtherSnapshot(boolean wrappedFirst, boolean allowEarliest)
            throws Exception {
        FileStoreTable mainTable = createTable();
        mainTable.createBranch("bc");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "bc");
        writeDataIntoTable(branchTable, 0, rowData(2, 20));
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        long snapshotTime = mainTable.snapshotManager().snapshot(1).timeMillis();
        while (System.currentTimeMillis() <= snapshotTime) {
            Thread.yield();
        }
        writeDataIntoTable(branchTable, 1, rowData(3, 30));
        branchTable.snapshotManager().deleteSnapshot(1);
        branchTable.snapshotManager().commitEarliestHint(2);
        // Earliest and latest must differ, so this verifies that the option selects earliest.
        writeDataIntoTable(branchTable, 2, rowData(4, 40));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        if (allowEarliest) {
            options.put(CoreOptions.SCAN_FALLBACK_BRANCH_ALLOW_EARLIEST_SNAPSHOT.key(), "true");
        }
        FileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, wrappedFirst).copy(options);

        if (allowEarliest) {
            assertThat(readAndCollect((FallbackReadFileStoreTable) table, scan -> {}))
                    .containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(2, 20), Pair.of(3, 30));
            assertThat(table.newScan().listPartitionEntries())
                    .map(entry -> entry.partition().getInt(0))
                    .containsExactlyInAnyOrder(1, 2, 3);
            DataTableScan pinned = table.newScan();
            branchTable.snapshotManager().deleteSnapshot(2);
            branchTable.snapshotManager().commitEarliestHint(3);
            assertThatThrownBy(pinned::plan)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("out of available snapshotId range");
        } else {
            assertThatThrownBy(() -> table.newScan().plan())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("No snapshot at or before");
        }
    }

    @Test
    public void testClearTimeTravelWithEmptyOtherBranch() throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch("bc");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "bc");
        FileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, true)
                        .copy(Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1"));
        long snapshotTime = mainTable.snapshotManager().snapshot(1).timeMillis();
        while (System.currentTimeMillis() <= snapshotTime) {
            Thread.yield();
        }
        writeDataIntoTable(branchTable, 0, rowData(2, 20));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
        options.put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST_FULL.toString());
        Map<String, String> schemaOptions = new HashMap<>(table.options());
        schemaOptions.put(CoreOptions.BRANCH.key(), mainTable.coreOptions().branch());
        schemaOptions.remove(CoreOptions.SCAN_SNAPSHOT_ID.key());
        schemaOptions.put(
                CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST_FULL.toString());
        for (FileStoreTable latest :
                Arrays.asList(
                        table.copy(options),
                        table.copy(table.schema().copy(schemaOptions)),
                        table.switchToBranch("main"))) {
            assertThat(readAndCollect((FallbackReadFileStoreTable) latest, scan -> {}))
                    .as(
                            "Latest options: %s, other options: %s",
                            latest.options(),
                            ((FallbackReadFileStoreTable) latest).other().options())
                    .containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(2, 20));
        }
        assertThatThrownBy(() -> table.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No snapshot at or before");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testExplicitExpiredSnapshotIsNotReplaced(boolean allowEarliest) throws Exception {
        FileStoreTable table = createTable();
        writeDataIntoTable(table, 0, rowData(1, 10));
        writeDataIntoTable(table, 1, rowData(2, 20));
        table.snapshotManager().deleteSnapshot(1);
        table.snapshotManager().commitEarliestHint(2);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), "1");
        options.put(
                CoreOptions.SCAN_FALLBACK_BRANCH_ALLOW_EARLIEST_SNAPSHOT.key(),
                String.valueOf(allowEarliest));
        FileStoreTable expired = table.copy(options);

        assertThatThrownBy(() -> expired.newScan().plan())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of available snapshotId range");
    }

    private DataTableScan queryAuthScan(DataTableScan delegate, TableQueryAuthResult authResult) {
        DataTableScan scan =
                Mockito.mock(DataTableScan.class, AdditionalAnswers.delegatesTo(delegate));
        Mockito.doAnswer(ignored -> authResult.convertPlan(delegate.plan())).when(scan).plan();
        return scan;
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
    public void testReadPlainSplitWithPrimaryBranch() throws Exception {
        String branchName = "bc";

        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10));

        mainTable.createBranch(branchName);
        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);
        writeDataIntoTable(branchTable, 0, rowData(2, 20));

        // evolve only the main branch, so its new files use a schema id the branch lacks
        TableSchema newSchema =
                new FileSystemSchemaManager(fileIO, tablePath)
                        .commitChanges(SchemaChange.setOption("my-custom-key", "my-value"));
        mainTable = mainTable.copy(newSchema);
        writeDataIntoTable(mainTable, 1, rowData(3, 30));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, false);

        // splits planned by newSnapshotReader() belong to the wrapped table
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        for (Split split : table.newSnapshotReader().read().splits()) {
            assertThat(split).isNotInstanceOf(FallbackReadFileStoreTable.FallbackSplit.class);
            RecordReader<InternalRow> reader = table.newRead().createReader(split);
            reader.forEachRemaining(r -> result.add(Pair.of(r.getInt(0), r.getInt(1))));
            reader.close();
        }
        assertThat(result).containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(3, 30));
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

    /**
     * Test that FallbackReadScan uses separate partition predicates for main and fallback scans.
     * When withPartitionFilter(mainPredicate, fallbackPredicate) is called, plan() should only list
     * partitions matching the corresponding predicate from each branch.
     */
    @Test
    public void testMainAndFallbackPartitionPredicates() throws Exception {
        FileStoreTable mainTable = createTable();
        writeDataIntoTable(mainTable, 0, rowData(1, 10), rowData(2, 20));

        mainTable.createBranch("bc");
        FileStoreTable branchTable = createTableFromBranch(mainTable, "bc");
        writeDataIntoTable(
                branchTable, 0, rowData(1, 100), rowData(2, 200), rowData(3, 300), rowData(4, 400));

        FallbackReadFileStoreTable table =
                new FallbackReadFileStoreTable(mainTable, branchTable, true);

        RowType partitionType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"pt"});
        PartitionPredicate mainPredicate =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(BinaryRow.singleColumn(1)));
        PartitionPredicate fallbackPredicate =
                PartitionPredicate.fromMultiple(
                        partitionType, Collections.singletonList(BinaryRow.singleColumn(3)));

        // Case 1: both predicates set, pt=1 from main, pt=3 from fallback
        assertThat(
                        readAndCollect(
                                table,
                                scan -> scan.withPartitionFilter(mainPredicate, fallbackPredicate)))
                .containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(3, 300));

        // Case 2: main predicate is null, fallback predicate set
        assertThat(readAndCollect(table, scan -> scan.withPartitionFilter(null, fallbackPredicate)))
                .containsExactlyInAnyOrder(Pair.of(1, 10), Pair.of(2, 20), Pair.of(3, 300));

        // Case 3: main predicate set, fallback predicate is null
        assertThat(readAndCollect(table, scan -> scan.withPartitionFilter(mainPredicate, null)))
                .containsExactlyInAnyOrder(
                        Pair.of(1, 10), Pair.of(2, 200), Pair.of(3, 300), Pair.of(4, 400));

        // Case 4: both null
        assertThat(readAndCollect(table, scan -> scan.withPartitionFilter(null, null)))
                .containsExactlyInAnyOrder(
                        Pair.of(1, 10), Pair.of(2, 20), Pair.of(3, 300), Pair.of(4, 400));
    }

    private List<Pair<Integer, Integer>> readAndCollect(
            FallbackReadFileStoreTable table,
            Consumer<FallbackReadFileStoreTable.FallbackReadScan> consumer)
            throws Exception {
        FallbackReadFileStoreTable.FallbackReadScan scan =
                (FallbackReadFileStoreTable.FallbackReadScan) table.newScan();
        consumer.accept(scan);
        List<Pair<Integer, Integer>> result = new ArrayList<>();
        for (Split split : scan.plan().splits()) {
            RecordReader<InternalRow> reader = table.newRead().createReader(split);
            reader.forEachRemaining(r -> result.add(Pair.of(r.getInt(0), r.getInt(1))));
            reader.close();
        }
        return result;
    }

    @Test
    void testSwitchToBranch() throws Exception {
        String branchName = "bc";

        Identifier mainId = Identifier.create("mydb", "mytable");
        CatalogEnvironment env =
                new CatalogEnvironment(mainId, "uuid-1", null, null, null, null, false, false);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                ""));
        AppendOnlyFileStoreTable mainTable =
                new AppendOnlyFileStoreTable(fileIO, tablePath, tableSchema, env);

        writeDataIntoTable(mainTable, 0, rowData(1, 10));
        mainTable.createBranch(branchName);

        FileStoreTable branchTable = createTableFromBranch(mainTable, branchName);
        writeDataIntoTable(branchTable, 0, rowData(2, 20));

        FallbackReadFileStoreTable fallbackTable =
                new FallbackReadFileStoreTable(mainTable, branchTable, true);

        FileStoreTable switched = fallbackTable.switchToBranch(branchName);
        Identifier switchedId = switched.catalogEnvironment().identifier();

        assertThat(switchedId).isNotNull();
        assertThat(switchedId.getDatabaseName()).isEqualTo("mydb");
        assertThat(switchedId.getBranchName()).isEqualTo(branchName);
        assertThat(switchedId.getObjectName()).isEqualTo("mytable$branch_bc");
        assertThat(switched.catalogEnvironment().uuid()).isNull();
        assertThat(mainTable.catalogEnvironment().uuid()).isEqualTo("uuid-1");
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan.fallback-branch", "scan.primary-branch"})
    void testBranchViewsRollbackWithoutSourceUuid(String branchOption) throws Exception {
        FileStoreTable localTable = createTable();
        localTable.createBranch("bc");
        Catalog catalog = Mockito.mock(Catalog.class);
        Identifier mainId = Identifier.create("mydb", "mytable");
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        mainId, "main-uuid", () -> catalog, null, null, null, true, false);
        Options options = new Options();
        options.set(branchOption, "bc");
        FallbackReadFileStoreTable table =
                (FallbackReadFileStoreTable)
                        FileStoreTableFactory.create(
                                fileIO, tablePath, localTable.schema(), options, environment);
        Identifier branchId = new Identifier("mydb", "mytable", "bc");

        table.other().rollbackTo(1L);
        Mockito.verify(catalog)
                .rollbackTo(
                        Mockito.eq(branchId),
                        Mockito.isNull(),
                        Mockito.argThat(
                                instant ->
                                        instant instanceof Instant.SnapshotInstant
                                                && ((Instant.SnapshotInstant) instant)
                                                                .getSnapshotId()
                                                        == 1L),
                        Mockito.isNull());

        table.switchToBranch("bc").rollbackTo("checkpoint");
        Mockito.verify(catalog)
                .rollbackTo(
                        Mockito.eq(branchId),
                        Mockito.isNull(),
                        Mockito.argThat(
                                instant ->
                                        instant instanceof Instant.TagInstant
                                                && "checkpoint"
                                                        .equals(
                                                                ((Instant.TagInstant) instant)
                                                                        .getTagName())),
                        Mockito.isNull());
        assertThat(table.catalogEnvironment().uuid()).isEqualTo("main-uuid");
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
        return createTable(false);
    }

    private AppendOnlyFileStoreTable createTable(boolean dataEvolution) throws Exception {
        Options options = new Options();
        options.set(CoreOptions.DATA_EVOLUTION_ENABLED, dataEvolution);
        options.set(CoreOptions.ROW_TRACKING_ENABLED, dataEvolution);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new FileSystemSchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Collections.emptyList(),
                                options.toMap(),
                                ""));
        return new AppendOnlyFileStoreTable(fileIO, tablePath, tableSchema);
    }

    private FileStoreTable createTableFromBranch(FileStoreTable baseTable, String branchName) {
        Options options = new Options(baseTable.options());
        options.set(CoreOptions.BRANCH, branchName);
        return new AppendOnlyFileStoreTable(
                        fileIO,
                        tablePath,
                        new FileSystemSchemaManager(fileIO, tablePath, branchName).latest().get())
                .copy(options.toMap());
    }
}
