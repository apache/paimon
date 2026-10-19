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

package org.apache.paimon.eslib.index;

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexCoverage;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Real ES archives must accelerate scalar predicates without dropping uncovered table rows. */
class ESIndexScalarTableTest {

    @TempDir java.nio.file.Path temporary;

    private final AuditedFileIO fileIO = new AuditedFileIO();
    private final List<IndexFileMeta> indexFiles = new ArrayList<>();

    @ParameterizedTest
    @CsvSource({
        "fast,false",
        "fast,true",
        "full,false",
        "full,true",
        "detail,false",
        "detail,true"
    })
    void testScalarPredicatesUseGroupedIndex(String mode, boolean inReader) throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 12, "embedding", "category", "price");
        FileStoreTable indexed = configured(table, mode, inReader);
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        List<Predicate> predicates =
                Arrays.asList(
                        b.equal(2, BinaryString.fromString("c1")),
                        b.greaterOrEqual(3, 3.0),
                        PredicateBuilder.and(b.greaterOrEqual(3, 3.0), b.lessThan(3, 9.0)),
                        b.in(
                                2,
                                Arrays.asList(
                                        BinaryString.fromString("c0"),
                                        BinaryString.fromString("c2"))),
                        b.isNull(2),
                        b.isNotNull(3),
                        b.notEqual(2, BinaryString.fromString("c1")),
                        b.notIn(
                                2,
                                Arrays.asList(
                                        BinaryString.fromString("c0"),
                                        BinaryString.fromString("c2"))),
                        b.startsWith(2, BinaryString.fromString("c")),
                        PredicateBuilder.and(
                                b.equal(2, BinaryString.fromString("c1")), b.greaterThan(3, 2.0)),
                        PredicateBuilder.or(
                                b.equal(2, BinaryString.fromString("c1")), b.lessThan(3, 3.0)));
        for (Predicate predicate : predicates) {
            List<Integer> expected = read(withoutIndex(table), predicate);
            fileIO.indexOpens.set(0);
            assertThat(read(indexed, predicate))
                    .as("%s", predicate)
                    .containsExactlyElementsOf(expected);
            assertThat(fileIO.indexOpens.get())
                    .as("ES index must serve %s", predicate)
                    .isPositive();
        }
    }

    @ParameterizedTest
    @CsvSource({"full,false", "full,true", "detail,false", "detail,true"})
    void testPartialAndDifferentDefinitionsRetainUncoveredRows(String mode, boolean inReader)
            throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 4, "embedding", "category", "price");
        build(table, 6, 8, "embedding", "price");
        PredicateBuilder b = new PredicateBuilder(table.rowType());
        Predicate category = b.equal(2, BinaryString.fromString("c1"));
        Predicate price = b.greaterOrEqual(3, 2.0);
        Predicate residual = b.equal(4, BinaryString.fromString("keep"));
        for (Predicate predicate :
                Arrays.asList(
                        category,
                        price,
                        PredicateBuilder.and(category, price),
                        PredicateBuilder.or(category, price),
                        PredicateBuilder.and(price, residual),
                        PredicateBuilder.or(category, residual),
                        PredicateBuilder.or(category, b.isNaN(3)))) {
            List<Integer> expected = read(withoutIndex(table), predicate);
            assertThat(read(configured(table, mode, inReader), predicate))
                    .as("%s", predicate)
                    .containsExactlyElementsOf(expected);
        }
    }

    @Test
    void testCoverageIncludesOnlyFieldsOfEachEligibleDefinition() throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 4, "embedding", "category", "price");
        build(table, 6, 8, "embedding", "price");
        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(
                        table,
                        table.snapshotManager().latestSnapshot(),
                        null,
                        indexFiles,
                        GlobalIndexSearchMode.FULL);
        assertThat(coverage.unindexedRanges(2)).containsExactly(new Range(4, 11));
        assertThat(coverage.unindexedRanges(3)).containsExactly(new Range(4, 5), new Range(8, 11));
        assertThat(coverage.unindexedRanges(4)).containsExactly(new Range(0, 11));
        assertThat(coverage.unindexedRanges(Arrays.asList(2, 3), null))
                .containsExactly(new Range(4, 11));
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.createForScalarFilters(
                                table, table.snapshotManager().latestSnapshot(), null, indexFiles)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Grouped ES must be eligible for scalar filters"))) {
            assertThat(scanner.scan(new PredicateBuilder(table.rowType()).greaterThan(3, 1.0)))
                    .isPresent();
        }
    }

    @Test
    void testFastModeDoesNotIncludeUnindexedTail() throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 8, "embedding", "category", "price");
        Predicate predicate = new PredicateBuilder(table.rowType()).greaterOrEqual(3, 2.0);
        List<Integer> expected =
                read(withoutIndex(table), predicate).stream()
                        .filter(id -> id < 8)
                        .collect(Collectors.toList());
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "fast", inReader), predicate))
                    .containsExactlyElementsOf(expected);
        }
    }

    @Test
    void testVectorPrimaryDesignIsUnchanged() throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 12, "embedding", "category", "price");
        assertThat(
                        table.newVectorSearchBuilder()
                                .withVectorColumn("embedding")
                                .withVector(vector(5))
                                .withLimit(1)
                                .executeLocal()
                                .results()
                                .toRangeList())
                .containsExactly(new Range(5, 5));
        assertThat(
                        table.newVectorSearchBuilder()
                                .withVectorColumn("embedding")
                                .withVector(vector(5))
                                .withLimit(1)
                                .withFilter(
                                        new PredicateBuilder(table.rowType())
                                                .equal(2, BinaryString.fromString("c1")))
                                .executeLocal()
                                .results()
                                .toRangeList())
                .containsExactly(new Range(4, 4));
    }

    @Test
    void testCompanionVectorIsNotSelectedForAnn() throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 12, "category", "embedding", "price");
        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(2, BinaryString.fromString("c1"));
        for (boolean inReader : Arrays.asList(false, true)) {
            fileIO.indexOpens.set(0);
            assertThat(read(configured(table, "full", inReader), predicate))
                    .containsExactly(1, 4, 7);
            assertThat(fileIO.indexOpens.get()).isPositive();
        }
        FileStoreTable fast =
                table.copy(Collections.singletonMap("vector-index.search-mode", "fast"));
        fileIO.indexOpens.set(0);
        assertThat(
                        fast.newVectorSearchBuilder()
                                .withVectorColumn("embedding")
                                .withVector(vector(5))
                                .withLimit(1)
                                .executeLocal()
                                .results()
                                .isEmpty())
                .isTrue();
        assertThat(fileIO.indexOpens.get()).isZero();
    }

    @Test
    void testOnlyEsOptsIntoGroupedIndexScalarLookups() {
        assertThat(GlobalIndexerFactoryUtils.supportsGroupedIndexScalarQueries("es-index"))
                .isTrue();
        assertThat(GlobalIndexerFactoryUtils.supportsGroupedIndexScalarQueries("btree")).isFalse();
        assertThat(GlobalIndexerFactoryUtils.supportsGroupedIndexScalarQueries("bitmap")).isFalse();
        assertThat(GlobalIndexerFactoryUtils.supportsGroupedIndexScalarQueries("unknown"))
                .isFalse();
    }

    @Test
    void testScalarPrimaryAndFullTextPreFilter() throws Exception {
        FileStoreTable table = createTable();
        build(table, 0, 12, "note", "category", "price");
        Predicate predicate =
                new PredicateBuilder(table.rowType()).equal(2, BinaryString.fromString("c1"));
        for (boolean inReader : Arrays.asList(false, true)) {
            assertThat(read(configured(table, "full", inReader), predicate))
                    .containsExactlyElementsOf(read(withoutIndex(table), predicate));
        }
        assertThat(
                        table.newFullTextSearchBuilder()
                                .withQuery("note", "{\"match\":{\"query\":\"keep\"}}")
                                .withFilter(predicate)
                                .withLimit(10)
                                .executeLocal()
                                .results()
                                .toRangeList())
                .containsExactly(new Range(4, 4));
    }

    private FileStoreTable createTable() throws Exception {
        Path path = new Path(temporary.toUri());
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .column("category", DataTypes.STRING())
                        .column("price", DataTypes.DOUBLE())
                        .column("note", DataTypes.STRING())
                        .option("bucket", "-1")
                        .option("write-only", "true")
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", "true")
                        .option("global-index.enabled", "true")
                        .option("global-index.es-index.fields.embedding.dimension", "3")
                        .option("global-index.es-index.fields.embedding.algorithm", "hnsw")
                        .option("global-index.es-index.fields.embedding.metric", "l2")
                        .option("global-index.es-index.fields.category.type", "keyword")
                        .option("global-index.es-index.fields.note.type", "fulltext")
                        .build();
        new FileSystemSchemaManager(fileIO, path).createTable(schema);
        FileStoreTable table = FileStoreTableFactory.create(fileIO, path);
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (int id = 0; id < 12; id++) {
                writer.write(
                        GenericRow.of(
                                id,
                                new GenericArray(vector(id)),
                                category(id),
                                price(id),
                                BinaryString.fromString(id % 2 == 0 ? "keep" : "skip")));
            }
            commit.commit(writer.prepareCommit());
        }
        return table;
    }

    private void build(FileStoreTable table, int from, int to, String... names) throws Exception {
        List<DataField> fields =
                Arrays.stream(names).map(table.rowType()::getField).collect(Collectors.toList());
        GlobalIndexFileReadWrite io =
                new GlobalIndexFileReadWrite(
                        fileIO, table.store().pathFactory().globalIndexFileFactory());
        GlobalIndexMultiColumnWriter writer =
                (GlobalIndexMultiColumnWriter)
                        GlobalIndexer.create("es-index", fields, Options.fromMap(table.options()))
                                .createWriter(io);
        for (int id = from; id < to; id++) {
            Map<String, Object> values = new HashMap<>();
            values.put("embedding", new GenericArray(vector(id)));
            values.put("category", category(id));
            values.put("price", price(id));
            values.put("note", BinaryString.fromString(id % 2 == 0 ? "keep" : "skip"));
            writer.write(id - from, GenericRow.of(Arrays.stream(names).map(values::get).toArray()));
        }
        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        List<IndexFileMeta> added = new ArrayList<>();
        for (ResultEntry result : writer.finish()) {
            added.add(
                    new IndexFileMeta(
                            "es-index",
                            result.fileName(),
                            io.fileSize(result.fileName()),
                            result.rowCount(),
                            new GlobalIndexMeta(
                                    from,
                                    to - 1,
                                    fields.get(0).id(),
                                    fields.stream().skip(1).mapToInt(DataField::id).toArray(),
                                    result.meta(),
                                    new DataEvolutionIndexSourceMeta(snapshot.id()).serialize()),
                            null));
            fileIO.indexFiles.add(result.fileName());
        }
        try (TableCommitImpl commit = table.newCommit("es-scalar-test")) {
            commit.commit(
                    snapshot.id(),
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    0,
                                    null,
                                    DataIncrement.indexIncrement(added),
                                    CompactIncrement.emptyIncrement())));
        }
        indexFiles.addAll(added);
    }

    private List<Integer> read(FileStoreTable table, Predicate predicate) throws Exception {
        ReadBuilder builder = table.newReadBuilder().withFilter(predicate);
        List<Split> splits = new ArrayList<>();
        for (Split split : builder.newScan().plan().splits()) {
            splits.add(
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(split), getClass().getClassLoader()));
        }
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().executeFilter().createReader(splits)) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        Collections.sort(result);
        return result;
    }

    private FileStoreTable configured(FileStoreTable table, String mode, boolean inReader) {
        Map<String, String> options = new HashMap<>();
        options.put("scalar-index.search-mode", mode);
        options.put("global-index.query-in-reader.enabled", Boolean.toString(inReader));
        return table.copy(options);
    }

    private FileStoreTable withoutIndex(FileStoreTable table) {
        return table.copy(Collections.singletonMap("global-index.enabled", "false"));
    }

    private static float[] vector(int id) {
        return new float[] {id + 0.1f, id * 0.02f, 1.0f};
    }

    private static BinaryString category(int id) {
        return id % 5 == 0 ? null : BinaryString.fromString("c" + id % 3);
    }

    private static Double price(int id) {
        return id % 4 == 0 ? null : (double) id;
    }

    private static class AuditedFileIO extends LocalFileIO {
        private static final long serialVersionUID = 1L;
        private final Set<String> indexFiles = new HashSet<>();
        private final AtomicInteger indexOpens = new AtomicInteger();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (indexFiles.contains(path.getName())) {
                indexOpens.incrementAndGet();
            }
            return super.newInputStream(path);
        }
    }
}
