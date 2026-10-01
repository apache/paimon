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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexCoverage;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.ScanResult;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexTestUtils;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Composite lookups must avoid single-column postings and retain uncovered data. */
class CompositeBTreeTableTest extends DataEvolutionTestBase {

    @Test
    void testCoexistsAndAvoidsSingleColumnPostings() throws Exception {
        createTableDefault();
        FileStoreTable table =
                table().copy(Collections.singletonMap("sorted-index.records-per-file", "13"));
        append(table, 0, 100);
        build(table, "f1");
        build(table, "f0");
        build(table, "f1", "f0");
        deleteSingleColumnFiles(table);
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate query = query(predicates, "category-a", 7);
        for (boolean inReader : Arrays.asList(false, true)) {
            FileStoreTable configured = configured(table, "full", inReader);
            assertThat(read(configured, query))
                    .containsExactlyInAnyOrder("p7", "p27", "p47", "p67", "p87");
            // Both OR branches are composite queries; predicate order differs from index order.
            Predicate union = PredicateBuilder.or(query, query(predicates, "category-b", 8));
            assertThat(read(configured, union)).hasSize(10).contains("p7", "p18", "p98");
            assertThat(read(configured, query(predicates, "missing", 7))).isEmpty();
            assertThat(read(configured, PredicateBuilder.and(query, predicates.equal(0, 8))))
                    .isEmpty();
            assertThat(
                            read(
                                    configured,
                                    PredicateBuilder.and(
                                            query,
                                            predicates.isNotNull(1),
                                            predicates.startsWith(
                                                    1, BinaryString.fromString("category-")),
                                            predicates.greaterThan(0, 5))))
                    .containsExactlyInAnyOrder("p7", "p27", "p47", "p67", "p87");
            assertThat(
                            read(
                                    configured,
                                    PredicateBuilder.and(
                                            query,
                                            predicates.startsWith(
                                                    1, BinaryString.fromString("missing")))))
                    .isEmpty();
        }
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(
                                table,
                                table.store().newIndexFileHandler().scanEntries().stream()
                                        .map(IndexManifestEntry::indexFile)
                                        .collect(Collectors.toList()))
                        .get()) {
            assertThat(scanner.scan(query).get().results().toRangeList())
                    .containsExactly(
                            new Range(7, 7),
                            new Range(27, 27),
                            new Range(47, 47),
                            new Range(67, 67),
                            new Range(87, 87));
        }
    }

    @Test
    void testUnsupportedScalarPredicatesRetainCompositeFallback() throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 20);
        build(table, "f1", "f0");
        append(table, 20, 40);
        build(table, "f2");
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate joint = query(predicates, "category-a", 7);
        Predicate unsupported = predicates.notLike(2, BinaryString.fromString("x%"));
        Predicate conjunction = PredicateBuilder.and(joint, unsupported);
        Predicate union =
                PredicateBuilder.or(joint, predicates.notLike(2, BinaryString.fromString("p1%")));
        FileStoreTable withoutIndex =
                table.copy(
                        Collections.singletonMap(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "false"));
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "fast", inReader), conjunction))
                    .containsExactly("p7");
            for (String mode : Arrays.asList("full", "detail")) {
                FileStoreTable configured = configured(table, mode, inReader);
                assertThat(read(configured, conjunction)).containsExactlyInAnyOrder("p7", "p27");
                assertThat(read(configured, union))
                        .containsExactlyInAnyOrderElementsOf(read(withoutIndex, union));
                FileStoreTable budgeted =
                        configured.copy(
                                Collections.singletonMap(
                                        "btree-index.fallback-scan-max-size", "0 b"));
                assertThat(
                                read(
                                        budgeted,
                                        PredicateBuilder.and(
                                                joint,
                                                predicates.contains(
                                                        2, BinaryString.fromString("7")))))
                        .containsExactlyInAnyOrder("p7", "p27");
            }
        }
    }

    @Test
    void testPartialResidualIndexCoverageAcrossSplits() throws Exception {
        createTableDefault();
        FileStoreTable table =
                table().copy(Collections.singletonMap("source.split.target-size", "1 b"));
        append(table, 0, 20);
        build(table, "f2");
        append(table, 20, 40);
        build(table, "f1", "f0");
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate joint = query(predicates, "category-a", 7);
        Predicate unsupported =
                PredicateBuilder.and(joint, predicates.notLike(2, BinaryString.fromString("x%")));
        Predicate supported =
                PredicateBuilder.and(joint, predicates.startsWith(2, BinaryString.fromString("p")));
        for (boolean inReader : Arrays.asList(false, true)) {
            FileStoreTable configured = configured(table, "fast", inReader);
            assertThat(read(configured, unsupported)).containsExactlyInAnyOrder("p7", "p27");
            assertThat(read(configured, supported)).containsExactly("p7");
            FileStoreTable budgeted =
                    configured.copy(
                            Collections.singletonMap("btree-index.fallback-scan-max-size", "0 b"));
            assertThat(
                            read(
                                    budgeted,
                                    PredicateBuilder.and(
                                            joint,
                                            predicates.contains(2, BinaryString.fromString("p")))))
                    .containsExactlyInAnyOrder("p7", "p27");
            for (String mode : Arrays.asList("full", "detail")) {
                assertThat(read(configured(table, mode, inReader), supported))
                        .containsExactlyInAnyOrder("p7", "p27");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"full", "detail"})
    void testPublicScannerFallbackUsesActualSupportedCoverage(String mode) throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 20);
        build(table, "f2");
        append(table, 20, 40);
        build(table, "f2");
        build(table, "f1", "f0");
        List<IndexFileMeta> files = new ArrayList<>();
        for (IndexManifestEntry entry : table.store().newIndexFileHandler().scanEntries()) {
            IndexFileMeta file = entry.indexFile();
            if (file.globalIndexMeta().indexFieldId() == 2
                    && file.globalIndexMeta().rowRangeStart() == 20) {
                // Model a residual shard over the scan budget without creating a large fixture.
                file =
                        new IndexFileMeta(
                                file.indexType(),
                                file.fileName(),
                                1_000_000_000L,
                                file.rowCount(),
                                file.globalIndexMeta(),
                                null);
            }
            files.add(file);
        }
        FileStoreTable configured =
                configured(table, mode, false)
                        .copy(
                                Collections.singletonMap(
                                        "btree-index.fallback-scan-max-size", "16 kb"));
        PredicateBuilder predicates = new PredicateBuilder(configured.rowType());
        Predicate query =
                PredicateBuilder.and(
                        query(predicates, "category-a", 7),
                        predicates.contains(2, BinaryString.fromString("p")));
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(configured, files).get()) {
            assertThat(scanner.scan(query).get().results().toRangeList())
                    .containsExactly(new Range(7, 7));
            assertThat(scanner.unindexedRows(query).results().toRangeList())
                    .containsExactly(new Range(20, 39));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"full", "detail"})
    void testDroppedDefinitionDoesNotCoverSurvivingExtraColumn(String mode) throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 40);
        IndexFileMeta stale =
                new IndexFileMeta(
                        "test-scalar",
                        "stale",
                        1,
                        40,
                        new GlobalIndexMeta(0, 39, 0, new int[] {1}, null),
                        null);
        IndexFileMeta valid =
                new IndexFileMeta(
                        "test-scalar",
                        "valid",
                        1,
                        20,
                        new GlobalIndexMeta(0, 19, 1, null, null),
                        null);
        catalog.alterTable(identifier(), SchemaChange.dropColumn("f0"), false);
        FileStoreTable evolved = configured(table(), mode, false);
        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(
                        evolved,
                        evolved.snapshotManager().latestSnapshot(),
                        null,
                        Arrays.asList(stale, valid),
                        evolved.coreOptions().scalarIndexSearchMode());
        assertThat(coverage.unindexedRanges(1)).containsExactly(new Range(20, 39));
    }

    @ParameterizedTest
    @CsvSource({"f0,false", "f0,true", "f1,false", "f1,true"})
    void testDroppedCompositeComponentFallsBack(String dropped, boolean scalar) throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 40);
        build(table, "f1", "f0");
        String surviving = dropped.equals("f0") ? "f1" : "f0";
        if (scalar) {
            build(table, surviving);
        }
        catalog.alterTable(identifier(), SchemaChange.dropColumn(dropped), false);
        FileStoreTable evolved = table();
        PredicateBuilder predicates = new PredicateBuilder(evolved.rowType());
        Predicate query =
                predicates.equal(
                        evolved.rowType().getFieldIndex(surviving),
                        surviving.equals("f0") ? 7 : BinaryString.fromString("category-a"));
        FileStoreTable withoutIndex =
                evolved.copy(
                        Collections.singletonMap(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "false"));
        List<String> expected = read(withoutIndex, query);
        assertThat(expected).hasSize(surviving.equals("f0") ? 4 : 20);
        for (boolean inReader : Arrays.asList(false, true)) {
            for (String mode : Arrays.asList("fast", "full", "detail")) {
                assertThat(read(configured(evolved, mode, inReader), query))
                        .containsExactlyInAnyOrderElementsOf(expected);
            }
        }
        Optional<DataEvolutionGlobalIndexScanner> scanner =
                DataEvolutionGlobalIndexScanner.create(
                        evolved,
                        evolved.store().newIndexFileHandler().scanEntries().stream()
                                .map(IndexManifestEntry::indexFile)
                                .collect(Collectors.toList()));
        assertThat(scanner.isPresent()).isEqualTo(scalar);
        if (scanner.isPresent()) {
            scanner.get().close();
        }
    }

    @Test
    void testDroppedUnrelatedScalarDoesNotBreakCompositeLookup() throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 40);
        build(table, "f0");
        build(table, "f1", "f2");
        catalog.alterTable(identifier(), SchemaChange.dropColumn("f0"), false);
        FileStoreTable evolved = table();
        PredicateBuilder predicates = new PredicateBuilder(evolved.rowType());
        Predicate query =
                PredicateBuilder.and(
                        predicates.equal(0, BinaryString.fromString("category-a")),
                        predicates.equal(1, BinaryString.fromString("p7")));
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(
                                evolved,
                                evolved.store().newIndexFileHandler().scanEntries().stream()
                                        .map(IndexManifestEntry::indexFile)
                                        .collect(Collectors.toList()))
                        .get()) {
            assertThat(scanner.scan(query).get().results().toRangeList())
                    .containsExactly(new Range(7, 7));
        }
        for (boolean inReader : Arrays.asList(false, true)) {
            for (String mode : Arrays.asList("fast", "full", "detail")) {
                assertThat(read(configured(evolved, mode, inReader), query)).containsExactly("p7");
            }
        }
    }

    @Test
    void testPartialCompositeCoverageDoesNotBorrowSingleColumnCoverage() throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 20);
        build(table, "f1", "f0");
        append(table, 20, 40);
        build(table, "f1");
        build(table, "f0");
        deleteSingleColumnFiles(table);
        Predicate query = query(new PredicateBuilder(table.rowType()), "category-a", 7);
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "fast", inReader), query)).containsExactly("p7");
            for (String mode : Arrays.asList("full", "detail")) {
                assertThat(read(configured(table, mode, inReader), query))
                        .containsExactlyInAnyOrder("p7", "p27");
            }
        }
    }

    @Test
    void testPartialSingleCoverageDoesNotBorrowCompositeCoverage() throws Exception {
        createTableDefault();
        FileStoreTable table = table();
        append(table, 0, 20);
        build(table, "f1");
        append(table, 20, 40);
        build(table, "f1", "f0");
        Predicate scalar =
                new PredicateBuilder(table.rowType())
                        .equal(1, BinaryString.fromString("category-a"));
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "fast", inReader), scalar)).hasSize(10);
            for (String mode : Arrays.asList("full", "detail")) {
                assertThat(read(configured(table, mode, inReader), scalar))
                        .hasSize(20)
                        .contains("p27");
            }
        }
    }

    @Test
    void testIncrementalBuildAndRefreshOfEitherComponent() throws Exception {
        createTableDefault();
        FileStoreTable table =
                table().copy(
                                Collections.singletonMap(
                                        CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(),
                                        "IGNORE"));
        append(table, 0, 20);
        build(table, "f1", "f0");
        append(table, 20, 40);
        build(table, "f1", "f0");
        assertThat(
                        new SortedGlobalIndexScanner(table, "btree")
                                .withIndexFields(Arrays.asList("f1", "f0"))
                                .incrementalScan())
                .isEmpty();
        update(table, "f0", 8);
        build(table, "f1", "f0");
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        assertThat(read(configured(table, "fast", false), query(predicates, "category-a", 7)))
                .containsExactly("p27");
        assertThat(read(configured(table, "fast", true), query(predicates, "category-a", 8)))
                .containsExactlyInAnyOrder("p7", "p8", "p28");
        update(table, "f1", BinaryString.fromString("changed"));
        build(table, "f1", "f0");
        assertThat(read(configured(table, "fast", true), query(predicates, "changed", 8)))
                .containsExactly("p7");
        assertThat(read(configured(table, "fast", false), query(predicates, "category-a", 8)))
                .containsExactlyInAnyOrder("p8", "p28");
    }

    @Test
    void testThreeFieldsNullComponentsAndUnsupportedPartialPredicate() throws Exception {
        createTableDefault();
        FileStoreTable table =
                table().copy(Collections.singletonMap("btree-index.bloom-filter.enabled", "true"));
        BatchWriteBuilder writes = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writes.newWrite();
                BatchTableCommit commit = writes.newCommit()) {
            write.write(
                    GenericRow.of(
                            -1,
                            BinaryString.fromString("category-a"),
                            BinaryString.fromString("a\u0000b")));
            write.write(
                    GenericRow.of(
                            -1,
                            BinaryString.fromString("category-a"),
                            BinaryString.fromString("")));
            write.write(GenericRow.of(-1, null, BinaryString.fromString("null-category")));
            write.write(
                    GenericRow.of(
                            null,
                            BinaryString.fromString("category-a"),
                            BinaryString.fromString("null-number")));
            commit.commit(write.prepareCommit());
        }
        build(table, "f1", "f0", "f2");
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate full =
                PredicateBuilder.and(
                        query(predicates, "category-a", -1),
                        predicates.equal(2, BinaryString.fromString("a\u0000b")));
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "full", inReader), full)).containsExactly("a\u0000b");
            // A partial tuple must use the ordinary data scan, never serialize a scalar as a tuple.
            assertThat(read(configured(table, "full", inReader), predicates.equal(0, -1)))
                    .hasSize(3);
        }
    }

    private FileStoreTable table() throws Exception {
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private Predicate query(PredicateBuilder builder, String category, int itemNumber) {
        return PredicateBuilder.and(
                builder.equal(0, itemNumber), builder.equal(1, BinaryString.fromString(category)));
    }

    private FileStoreTable configured(FileStoreTable table, String mode, boolean inReader) {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.SCALAR_INDEX_SEARCH_MODE.key(), mode);
        options.put(
                CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(), String.valueOf(inReader));
        return table.copy(options);
    }

    private List<String> read(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder builder = table.newReadBuilder().withFilter(predicate);
        TableScan.Plan plan = builder.newScan().plan();
        // Exercise split checkpoint serialization as well as direct execution.
        List<Split> splits = new ArrayList<>();
        for (Split split : plan.splits()) {
            splits.add(
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(split), getClass().getClassLoader()));
        }
        List<String> values = new ArrayList<>();
        builder.newRead()
                .executeFilter()
                .createReader(splits)
                .forEachRemaining(
                        row ->
                                values.add(
                                        row.getString(table.rowType().getFieldIndex("f2"))
                                                .toString()));
        return values;
    }

    private void append(FileStoreTable table, int from, int to) throws Exception {
        BatchWriteBuilder writes = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writes.newWrite();
                BatchTableCommit commit = writes.newCommit()) {
            for (int i = from; i < to; i++) {
                write.write(
                        GenericRow.of(
                                i % 10,
                                BinaryString.fromString(
                                        (i / 10) % 2 == 0 ? "category-a" : "category-b"),
                                BinaryString.fromString("p" + i)));
            }
            commit.commit(write.prepareCommit());
        }
    }

    private void update(FileStoreTable table, String field, Object value) throws Exception {
        BatchWriteBuilder writes = table.newBatchWriteBuilder();
        try (BatchTableWrite write =
                        writes.newWrite()
                                .withWriteType(
                                        table.rowType().project(Collections.singletonList(field)));
                BatchTableCommit commit = writes.newCommit()) {
            for (int i = 0; i < 20; i++) {
                Object original =
                        field.equals("f0")
                                ? i % 10
                                : BinaryString.fromString(
                                        (i / 10) % 2 == 0 ? "category-a" : "category-b");
                write.write(GenericRow.of(i == 7 ? value : original));
            }
            List<CommitMessage> messages = write.prepareCommit();
            setFirstRowId(messages, 0L);
            commit.commit(messages);
        }
    }

    private void build(FileStoreTable table, String... fields) throws Exception {
        List<String> names = Arrays.asList(fields);
        Optional<ScanResult<DataSplit>> scan =
                new SortedGlobalIndexScanner(table, "btree")
                        .withIndexFields(names)
                        .incrementalScan();
        if (!scan.isPresent()) {
            return;
        }
        ScanResult<DataSplit> result = scan.get();
        List<CommitMessage> messages = new ArrayList<>();
        for (DataSplit split : result.entries()) {
            messages.addAll(
                    SortedGlobalIndexTestUtils.buildIndex(
                            table, "btree", names, split, result.scanSnapshotId()));
        }
        for (IndexManifestEntry entry : result.deletedIndexEntries()) {
            messages.add(
                    new CommitMessageImpl(
                            entry.partition(),
                            entry.bucket(),
                            null,
                            DataIncrement.deleteIndexIncrement(
                                    Collections.singletonList(entry.indexFile())),
                            CompactIncrement.emptyIncrement()));
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(messages);
        }
    }

    private void deleteSingleColumnFiles(FileStoreTable table) throws Exception {
        int deleted = 0;
        for (IndexManifestEntry entry : table.store().newIndexFileHandler().scanEntries()) {
            IndexFileMeta file = entry.indexFile();
            if (file.globalIndexMeta().extraFieldIds() == null
                    || file.globalIndexMeta().extraFieldIds().length == 0) {
                assertThat(
                                table.fileIO()
                                        .delete(
                                                table.store()
                                                        .pathFactory()
                                                        .globalIndexFileFactory()
                                                        .toPath(file),
                                                false))
                        .isTrue();
                deleted++;
            }
        }
        assertThat(deleted).isPositive();
    }
}
