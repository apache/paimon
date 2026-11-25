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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.generic.GenericGlobalIndexScanner;
import org.apache.paimon.globalindex.sorted.SortedGlobalIndexWriter;
import org.apache.paimon.index.DataEvolutionIndexSourceMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests of R-tree row IDs, coverage and Data Evolution column reconstruction. */
class RTreeGlobalIndexTableTest extends DataEvolutionTestBase {

    private boolean queryInReader;

    @Override
    protected Schema schemaDefault() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("geom", DataTypes.GEOMETRY("EPSG:3857"))
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .option("scalar-index.search-mode", "full")
                .option("rtree-index.page-size", "2")
                .option("rtree-index.partition-row-count", "2")
                .build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndexedScansAcrossRowRangesAndUnindexedAppends(boolean queryInReader)
            throws Exception {
        this.queryInReader = queryInReader;
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(
                table,
                GenericRow.of(0, point(-10, -10)),
                GenericRow.of(1, point(1, 1)),
                GenericRow.of(2, null),
                GenericRow.of(3, point(20, 20)),
                GenericRow.of(4, point(2, 2)),
                GenericRow.of(5, point(Double.NaN, Double.NaN)));
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate box = predicates.bboxIntersects(1, 0, 0, 2, 2);
        assertThat(readIds(table, box)).containsExactly(1, 4);
        buildIndex(table, new Range(0, 2));
        table = getTableDefault();
        buildIndex(table, new Range(3, 5));
        table = getTableDefault();
        assertThat(table.store().newIndexFileHandler().scan("rtree"))
                .extracting(entry -> entry.indexFile().rowCount())
                .containsExactlyInAnyOrder(2L, 1L, 2L, 1L);
        try (DataEvolutionGlobalIndexScanner scanner =
                DataEvolutionGlobalIndexScanner.create(table, PartitionPredicate.ALWAYS_TRUE, box)
                        .get()) {
            assertThat(scanner.scan(box).get().results().toRangeList())
                    .containsExactly(new Range(1, 1), new Range(4, 4));
        }
        assertThat(readIds(table, box)).containsExactly(1, 4);
        assertThat(readIds(table, predicates.isNull(1))).containsExactly(2);
        assertThat(readIds(table, predicates.isNotNull(1))).containsExactly(0, 1, 3, 4, 5);
        assertThat(readIds(table, PredicateBuilder.and(box, predicates.greaterThan(0, 2))))
                .containsExactly(4);
        assertThat(readIds(table, PredicateBuilder.or(box, predicates.equal(0, 3))))
                .containsExactly(1, 3, 4);
        write(table, GenericRow.of(6, point(1, 1)));
        assertThat(readIds(getTableDefault(), box)).containsExactly(1, 4, 6);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testGeometryEqualityRemainsADataFilter(boolean deferred) throws Exception {
        queryInReader = deferred;
        createTableDefault();
        FileStoreTable table = getTableDefault();
        byte[] value = point(1, 1);
        write(table, GenericRow.of(0, value));
        buildIndex(table, new Range(0, 0));
        write(getTableDefault(), GenericRow.of(1, value));
        for (String mode : new String[] {"fast", "full", "detail"}) {
            table =
                    getTableDefault()
                            .copy(Collections.singletonMap("scalar-index.search-mode", mode));
            PredicateBuilder builder = new PredicateBuilder(table.rowType());
            Predicate equality = builder.equal(1, value);
            Predicate bounds = builder.bboxIntersects(1, 0, 0, 2, 2);
            Integer[] expected = "fast".equals(mode) ? new Integer[] {0} : new Integer[] {0, 1};
            assertThat(readIds(table, equality)).containsExactly(0, 1);
            assertThat(readIds(table, PredicateBuilder.or(bounds, equality))).containsExactly(0, 1);
            assertThat(readIds(table, bounds)).containsExactly(expected);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFastMixedPredicatesWithDisjointIndexCoverage(boolean deferred) throws Exception {
        queryInReader = deferred;
        createTableDefault();
        FileStoreTable table = getTableDefault();
        byte[] geom = point(1, 1);
        write(table, GenericRow.of(0, geom), GenericRow.of(1, geom), GenericRow.of(2, geom));
        table = getTableDefault();
        SortedGlobalIndexWriter writer =
                new SortedGlobalIndexWriter(table, "btree")
                        .withIndexFields(Collections.singletonList("id"));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    writer.buildForSinglePartition(
                            new Range(0, 0),
                            BinaryRow.EMPTY_ROW,
                            Collections.<InternalRow>singletonList(GenericRow.of(0, 0L)).iterator(),
                            table.latestSnapshot().get().id()));
        }
        buildIndex(getTableDefault(), new Range(1, 1));
        table =
                getTableDefault()
                        .copy(Collections.singletonMap("scalar-index.search-mode", "fast"));
        PredicateBuilder builder = new PredicateBuilder(table.rowType());
        Predicate equality = builder.equal(1, geom);
        assertThat(readIds(table, PredicateBuilder.and(equality, builder.greaterOrEqual(0, 0))))
                .containsExactly(0);
        assertThat(readIds(table, PredicateBuilder.and(equality, builder.greaterOrEqual(0, 1))))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSpatialFilterWithUnsupportedBranchOnAnotherColumn(boolean deferred) throws Exception {
        queryInReader = deferred;
        createTableDefault();
        catalog.alterTable(
                identifier(), SchemaChange.addColumn("content", DataTypes.STRING()), false);
        FileStoreTable table = getTableDefault();
        BinaryString text = BinaryString.fromString("same");
        byte[] geom = point(1, 1);
        write(table, GenericRow.of(0, geom, text));
        write(getTableDefault(), GenericRow.of(1, geom, text));
        write(getTableDefault(), GenericRow.of(2, geom, text));
        buildIndex(getTableDefault(), new Range(0, 0));
        buildIndex(
                getTableDefault(),
                new Range(1, 1),
                Collections.emptyList(),
                "test-fulltext",
                "content");
        table =
                getTableDefault()
                        .copy(Collections.singletonMap("scalar-index.search-mode", "fast"));
        table =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(), "1 b"));
        assertThat(table.newReadBuilder().newScan().plan().splits()).hasSize(3);
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate equality = predicates.equal(2, text);
        Predicate bounds = predicates.bboxIntersects(1, 0, 0, 2, 2);
        assertThat(readIds(table, PredicateBuilder.and(bounds, equality))).containsExactly(0);
        // Without a scalar index on content the disjunction needs the ordinary data scan.
        assertThat(readIds(table, PredicateBuilder.or(bounds, equality))).containsExactly(0, 1, 2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testIndexReadsLatestUnrelatedColumnVersion(boolean queryInReader) throws Exception {
        this.queryInReader = queryInReader;
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(
                table,
                GenericRow.of(0, point(0, 0)),
                GenericRow.of(1, point(10, 10)),
                GenericRow.of(2, point(2, 2)));
        buildIndex(table, new Range(0, 2));
        table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer =
                builder.newWrite()
                        .withWriteType(table.rowType().project(Collections.singletonList("id")))) {
            writer.write(GenericRow.of(100));
            writer.write(GenericRow.of(101));
            writer.write(GenericRow.of(102));
            List<CommitMessage> messages = writer.prepareCommit();
            setFirstRowId(messages, 0);
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(messages);
            }
        }
        table = getTableDefault();
        PredicateBuilder predicates = new PredicateBuilder(table.rowType());
        Predicate box = predicates.bboxIntersects(1, 0, 0, 2, 2);
        assertThat(readIds(table, box)).containsExactly(100, 102);
        assertThat(readIds(table, PredicateBuilder.and(box, predicates.greaterThan(0, 100))))
                .containsExactly(102);
    }

    @Test
    void testChangedGeometryRangesCanBeRefreshed() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        write(table, GenericRow.of(0, point(10, 10)), GenericRow.of(1, point(1, 1)));
        buildIndex(table, new Range(0, 1));
        catalog.alterTable(
                identifier(),
                SchemaChange.setOption("global-index.column-update-action", "IGNORE"),
                false);
        table = getTableDefault();
        assertThat(
                        new GenericGlobalIndexScanner(table)
                                .withIndex(
                                        "rtree", Collections.singletonList("geom"), new Options())
                                .incrementalScan())
                .isEmpty();
        updateGeometry(getTableDefault());
        table = getTableDefault();
        List<IndexManifestEntry> retired =
                new GenericGlobalIndexScanner(table)
                        .withIndex("rtree", Collections.singletonList("geom"), new Options())
                        .incrementalScan()
                        .get()
                        .deletedIndexEntries();
        assertThat(retired).hasSize(1);
        buildIndex(table, new Range(0, 1), retired);
        Predicate box = new PredicateBuilder(table.rowType()).bboxIntersects(1, 0, 0, 2, 2);
        assertThat(readIds(getTableDefault(), box)).containsExactly(0);
        assertThat(
                        new GenericGlobalIndexScanner(getTableDefault())
                                .withIndex(
                                        "rtree", Collections.singletonList("geom"), new Options())
                                .incrementalScan())
                .isEmpty();
    }

    private void updateGeometry(FileStoreTable table) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer =
                builder.newWrite()
                        .withWriteType(
                                table.rowType().project(Collections.singletonList("geom")))) {
            writer.write(GenericRow.of((Object) point(0, 0)));
            writer.write(GenericRow.of((Object) point(100, 100)));
            List<CommitMessage> messages = writer.prepareCommit();
            setFirstRowId(messages, 0);
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(messages);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDeletedIndexedRowsStayInvisible(boolean queryInReader) throws Exception {
        this.queryInReader = queryInReader;
        createTableDefault();
        catalog.alterTable(
                identifier(), SchemaChange.setOption("deletion-vectors.enabled", "true"), false);
        FileStoreTable table = getTableDefault();
        write(
                table,
                GenericRow.of(0, point(0, 0)),
                GenericRow.of(1, point(1, 1)),
                GenericRow.of(2, point(2, 2)));
        buildIndex(table, new Range(0, 2));
        table = getTableDefault();
        DataSplit split = (DataSplit) table.newScan().plan().splits().get(0);
        BaseAppendDeleteFileMaintainer maintainer =
                BaseAppendDeleteFileMaintainer.forUnawareAppend(
                        table.store().newIndexFileHandler(),
                        table.latestSnapshot().get(),
                        org.apache.paimon.data.BinaryRow.EMPTY_ROW);
        DeletionVector vector = new BitmapDeletionVector();
        vector.delete(1);
        maintainer.notifyNewDeletionVector(split.dataFiles().get(0).fileName(), vector);
        List<IndexFileMeta> additions = new ArrayList<>();
        for (IndexManifestEntry entry : maintainer.persist()) {
            if (entry.kind() == FileKind.ADD) {
                additions.add(entry.indexFile());
            }
        }
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                                    0,
                                    null,
                                    new DataIncrement(
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            additions,
                                            Collections.emptyList()),
                                    CompactIncrement.emptyIncrement())));
        }
        table = getTableDefault();
        Predicate box = new PredicateBuilder(table.rowType()).bboxIntersects(1, 0, 0, 2, 2);
        assertThat(readIds(table, box)).containsExactly(0, 2);
    }

    private void buildIndex(FileStoreTable table, Range range) throws Exception {
        buildIndex(table, range, Collections.emptyList());
    }

    private void buildIndex(FileStoreTable table, Range range, List<IndexManifestEntry> retired)
            throws Exception {
        buildIndex(table, range, retired, "rtree", "geom");
    }

    private void buildIndex(
            FileStoreTable table,
            Range range,
            List<IndexManifestEntry> retired,
            String type,
            String column)
            throws Exception {
        DataField field = table.rowType().getField(column);
        RowType readType =
                SpecialFields.rowTypeWithRowId(
                        table.rowType().project(Collections.singletonList(column)));
        ReadBuilder read =
                table.newReadBuilder()
                        .withReadType(readType)
                        .withRowRanges(Collections.singletonList(range));
        GlobalIndexWriter indexWriter =
                GlobalIndexBuilderUtils.createIndexWriter(
                        table,
                        type,
                        Collections.singletonList(field),
                        table.coreOptions().toConfiguration());
        List<Split> splits = read.newScan().plan().splits();
        try {
            try (RecordReader<InternalRow> reader = read.newRead().createReader(splits);
                    CloseableIterator<InternalRow> rows = reader.toCloseableIterator()) {
                InternalRow.FieldGetter getter =
                        InternalRow.createFieldGetter(field.type(), readType.getFieldIndex(column));
                int idIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    long id = row.getLong(idIndex);
                    if (id >= range.from && id <= range.to) {
                        ((GlobalIndexSingleColumnWriter) indexWriter)
                                .write(getter.getFieldOrNull(row), id - range.from);
                    }
                }
            }
            List<ResultEntry> results = indexWriter.finish();
            List<IndexFileMeta> metas =
                    GlobalIndexBuilderUtils.toIndexFileMetas(
                            table.fileIO(),
                            table.coreOptions(),
                            range,
                            Collections.singletonList(field),
                            type,
                            results,
                            new DataEvolutionIndexSourceMeta(table.latestSnapshot().get().id())
                                    .serialize());
            CommitMessage message =
                    new CommitMessageImpl(
                            org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                            0,
                            null,
                            DataIncrement.indexIncrement(metas),
                            CompactIncrement.emptyIncrement());
            try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
                List<CommitMessage> messages = new ArrayList<>();
                messages.add(message);
                for (IndexManifestEntry entry : retired) {
                    messages.add(
                            new CommitMessageImpl(
                                    entry.partition(),
                                    entry.bucket(),
                                    null,
                                    DataIncrement.deleteIndexIncrement(
                                            Collections.singletonList(entry.indexFile())),
                                    CompactIncrement.emptyIncrement()));
                }
                commit.commit(messages);
            }
        } finally {
            if (indexWriter instanceof java.io.Closeable) {
                ((java.io.Closeable) indexWriter).close();
            }
        }
    }

    private List<Integer> readIds(FileStoreTable table, Predicate predicate) throws Exception {
        table =
                table.copy(
                        Collections.singletonMap(
                                CoreOptions.GLOBAL_INDEX_QUERY_IN_READER_ENABLED.key(),
                                Boolean.toString(queryInReader)));
        ReadBuilder read = table.newReadBuilder().withFilter(predicate);
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                read.newRead().executeFilter().createReader(read.newScan().plan())) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        Collections.sort(result);
        return result;
    }

    private static byte[] point(double x, double y) {
        return ByteBuffer.allocate(21)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 1)
                .putInt(1)
                .putDouble(x)
                .putDouble(y)
                .array();
    }
}
