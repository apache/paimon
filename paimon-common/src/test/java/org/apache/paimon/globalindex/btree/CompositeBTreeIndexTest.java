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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongArrayList;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for typed composite BTree keys. */
class CompositeBTreeIndexTest {

    @TempDir java.nio.file.Path tempPath;

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testMutableTupleKeysPostingListsAndLocalRanges(int version) throws Exception {
        RowType type =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "category", DataTypes.STRING()),
                                new DataField(20, "item_number", DataTypes.INT()),
                                new DataField(30, "tag", DataTypes.STRING())));
        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_FILE_VERSION, version);
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOOM_FILTER_ENABLED, true);
        options.set(BTreeIndexOptions.BTREE_INDEX_COMPRESSION, "lz4");
        GlobalIndexer indexer =
                GlobalIndexer.create(
                        "btree", type.getFields().get(0), type.getFields().subList(1, 3), options);
        LocalFileIO io = LocalFileIO.create();
        Path directory = new Path(tempPath.toUri());
        GlobalIndexFileWriter files =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name)
                            throws java.io.IOException {
                        return io.newOutputStream(new Path(directory, name), false);
                    }
                };
        GenericRow reused = row("category-a", -1, "");
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) indexer.createWriter(files);
        writer.write(reused, 0);
        reused.setField(1, 7);
        writer.write(reused, 1);
        reused.setField(2, BinaryString.fromString("tag"));
        writer.write(reused, 2);
        writer.write(reused, 3);
        reused.setField(0, BinaryString.fromString("category-b"));
        writer.write(reused, 4);
        ResultEntry result = writer.finish().get(0);
        Path path = new Path(directory, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, io.getFileSize(path), result.rowCount(), result.meta());
        ExecutorService executor = newDirectExecutorService();
        try (GlobalIndexReader reader =
                indexer.createReader(
                        file -> io.newInputStream(file.filePath()),
                        Collections.singletonList(meta),
                        5,
                        null,
                        executor)) {
            assertThat(
                            reader.visitCompositeEqual(
                                            Arrays.asList(
                                                    BinaryString.fromString("category-a"),
                                                    -1,
                                                    BinaryString.fromString("")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(0, 0));
            assertThat(
                            reader.visitCompositeEqual(
                                            Arrays.asList(
                                                    BinaryString.fromString("category-a"),
                                                    7,
                                                    BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(2, 3));
            assertThat(
                            reader.visitCompositeEqual(
                                            Arrays.asList(
                                                    BinaryString.fromString("category-a"),
                                                    8,
                                                    BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .isEmpty())
                    .isTrue();
        }
        try (GlobalIndexReader reader =
                indexer.createReader(
                        file -> io.newInputStream(file.filePath()),
                        Collections.singletonList(meta),
                        5,
                        Collections.singletonList(new Range(3, 3)),
                        executor)) {
            assertThat(
                            reader.visitCompositeEqual(
                                            Arrays.asList(
                                                    BinaryString.fromString("category-a"),
                                                    7,
                                                    BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(3, 3));
        } finally {
            executor.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testPrefixesRangesInNullsAndKeyFilters(int version) throws Exception {
        RowType type =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "category", DataTypes.STRING()),
                                new DataField(20, "item_number", DataTypes.INT()),
                                new DataField(30, "tag", DataTypes.STRING())));
        List<GenericRow> rows =
                Arrays.asList(
                        row("category-a", Integer.MIN_VALUE, "x"),
                        row("category-a", -1, "x"),
                        row("category-a", 0, "y"),
                        row("category-a", 7, "x"),
                        row("category-a", 7, "y"),
                        row("category-a", 8, "x"),
                        row("category-a", Integer.MAX_VALUE, "x"),
                        row("category-b", 7, "x"),
                        row(null, 7, "x"),
                        row("category-a", 9, null),
                        row("category-a", 10, "y"),
                        GenericRow.of(
                                BinaryString.fromString("category-a"),
                                null,
                                BinaryString.fromString("x")),
                        row("category-b", -1, null));
        Options options = new Options();
        options.set(BTreeIndexOptions.BTREE_INDEX_FILE_VERSION, version);
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOOM_FILTER_ENABLED, true);
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE, MemorySize.ofBytes(128));
        GlobalIndexer indexer =
                GlobalIndexer.create(
                        "btree", type.getFields().get(0), type.getFields().subList(1, 3), options);
        LocalFileIO io = LocalFileIO.create();
        Path directory = new Path(tempPath.toUri());
        GlobalIndexFileWriter files =
                new GlobalIndexFileWriter() {
                    @Override
                    public String newFileName(String prefix) {
                        return prefix + UUID.randomUUID();
                    }

                    @Override
                    public PositionOutputStream newOutputStream(String name)
                            throws java.io.IOException {
                        return io.newOutputStream(new Path(directory, name), false);
                    }
                };
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) indexer.createWriter(files);
        List<Integer> sortedRows =
                IntStream.range(0, rows.size()).boxed().collect(Collectors.toList());
        Comparator<Object> comparator = new CompositeKeySerializer(type).createComparator();
        sortedRows.sort((left, right) -> comparator.compare(rows.get(left), rows.get(right)));
        for (int rowId : sortedRows) {
            writer.write(rows.get(rowId), rowId);
        }
        ResultEntry result = writer.finish().get(0);
        Path path = new Path(directory, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, io.getFileSize(path), result.rowCount(), result.meta());
        PredicateBuilder b = new PredicateBuilder(type);
        Predicate a = b.equal(0, BinaryString.fromString("category-a"));
        List<Predicate> queries =
                Arrays.asList(
                        a,
                        PredicateBuilder.and(a, b.greaterThan(1, 7)),
                        PredicateBuilder.and(b.lessThan(1, 0), a),
                        PredicateBuilder.and(a, b.greaterOrEqual(1, 0), b.lessOrEqual(1, 7)),
                        PredicateBuilder.and(a, b.between(1, 7, 8)),
                        PredicateBuilder.and(a, b.greaterThan(1, 7), b.lessOrEqual(1, 7)),
                        PredicateBuilder.and(a, b.greaterOrEqual(1, 7), b.lessOrEqual(1, 7)),
                        PredicateBuilder.and(a, b.greaterThan(1, Integer.MAX_VALUE)),
                        PredicateBuilder.and(a, b.lessThan(1, Integer.MIN_VALUE)),
                        PredicateBuilder.and(a, b.lessOrEqual(1, Integer.MAX_VALUE)),
                        PredicateBuilder.and(a, b.in(1, Arrays.asList(7, 7, null, 8))),
                        PredicateBuilder.and(
                                b.in(
                                        0,
                                        Arrays.asList(
                                                BinaryString.fromString("category-a"),
                                                BinaryString.fromString("category-b"))),
                                b.in(1, Arrays.asList(7, 8))),
                        PredicateBuilder.and(a, b.isNull(1)),
                        PredicateBuilder.and(b.isNull(0), b.equal(1, 7)),
                        PredicateBuilder.and(a, b.isNotNull(1)),
                        PredicateBuilder.and(
                                a, b.greaterThan(1, 7), b.equal(2, BinaryString.fromString("x"))),
                        PredicateBuilder.and(a, b.equal(2, BinaryString.fromString("x"))),
                        PredicateBuilder.and(
                                a, b.equal(1, 7), b.notLike(2, BinaryString.fromString("x%"))),
                        PredicateBuilder.and(a, b.equal(1, 7), b.isNull(2)),
                        PredicateBuilder.and(a, b.equal(1, 7), b.equal(1, 8)),
                        PredicateBuilder.and(a, b.equal(1, null)),
                        PredicateBuilder.and(a, b.greaterThan(1, null)));
        ExecutorService executor = newDirectExecutorService();
        for (List<Range> allowed :
                Arrays.asList(null, Collections.singletonList(new Range(3, 10)))) {
            try (GlobalIndexReader reader =
                    indexer.createReader(
                            file -> io.newInputStream(file.filePath()),
                            Collections.singletonList(meta),
                            rows.size(),
                            allowed,
                            executor)) {
                for (Predicate query : queries) {
                    RoaringNavigableMap64 expected = new RoaringNavigableMap64();
                    for (int i = 0; i < rows.size(); i++) {
                        if (query.test(rows.get(i)) && (allowed == null || (i >= 3 && i <= 10))) {
                            expected.add(i);
                        }
                    }
                    assertThat(reader.visitComposite(query).get().get().results().toRangeList())
                            .as("predicate %s; allowed %s", query, allowed)
                            .containsExactlyElementsOf(expected.toRangeList());
                }
                assertThat(reader.visitComposite(b.equal(1, 7)).get()).isEmpty();
                Predicate largeIn =
                        new LeafPredicate(
                                In.INSTANCE,
                                DataTypes.INT(),
                                1,
                                "item_number",
                                IntStream.range(0, CompositeBTreePredicate.MAX_INTERVALS + 1)
                                        .boxed()
                                        .map(value -> (Object) value)
                                        .collect(Collectors.toList()));
                assertThat(reader.visitComposite(PredicateBuilder.and(a, largeIn)).get()).isEmpty();
            }
        }
        Options noScan = new Options();
        noScan.set(BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE, MemorySize.ofBytes(0));
        GlobalIndexer budgeted =
                GlobalIndexer.create(
                        "btree", type.getFields().get(0), type.getFields().subList(1, 3), noScan);
        try (GlobalIndexReader reader =
                budgeted.createReader(
                        file -> io.newInputStream(file.filePath()),
                        Collections.singletonList(meta),
                        rows.size(),
                        null,
                        executor)) {
            assertThat(reader.visitComposite(PredicateBuilder.and(a, b.greaterThan(1, 7))).get())
                    .isEmpty();
            assertThat(
                            reader.visitComposite(
                                            PredicateBuilder.and(
                                                    b.equal(0, BinaryString.fromString("missing")),
                                                    b.greaterThan(1, 7)))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .isEmpty();
            assertThat(
                            reader.visitComposite(
                                            PredicateBuilder.and(
                                                    a,
                                                    b.equal(1, 7),
                                                    b.equal(2, BinaryString.fromString("x"))))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(3, 3));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testFileOverlapAndInExpansionBoundaries() {
        RowType type =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", DataTypes.INT().notNull()),
                                new DataField(1, "b", DataTypes.INT().notNull())));
        CompositeKeySerializer serializer = new CompositeKeySerializer(type);
        PredicateBuilder b = new PredicateBuilder(type);
        Predicate range =
                PredicateBuilder.and(b.equal(0, 1), b.greaterThan(1, 7), b.lessOrEqual(1, 9));
        GlobalIndexIOMeta below =
                metadata("below", serializer, GenericRow.of(1, 0), GenericRow.of(1, 7));
        GlobalIndexIOMeta inside =
                metadata("inside", serializer, GenericRow.of(1, 8), GenericRow.of(1, 9));
        GlobalIndexIOMeta above =
                metadata("above", serializer, GenericRow.of(1, 10), GenericRow.of(2, 0));
        GlobalIndexIOMeta before =
                metadata(
                        "before",
                        serializer,
                        GenericRow.of(0, 0),
                        GenericRow.of(0, Integer.MAX_VALUE));
        GlobalIndexIOMeta after =
                metadata(
                        "after",
                        serializer,
                        GenericRow.of(2, Integer.MIN_VALUE),
                        GenericRow.of(2, 0));
        GlobalIndexIOMeta noEndpoints = metadata("no-endpoints", serializer, null, null);
        GlobalIndexIOMeta noLast = metadata("no-last", serializer, GenericRow.of(1, 8), null);
        GlobalIndexIOMeta noMetadata = new GlobalIndexIOMeta(new Path("unknown"), 100, 1, null);
        List<GlobalIndexIOMeta> files =
                Arrays.asList(below, inside, above, before, after, noEndpoints, noLast, noMetadata);
        CompositeBTreePredicate.Plan plan =
                CompositeBTreePredicate.plan(type.getFields(), range).get();
        List<GlobalIndexIOMeta> selected = plan.selectFiles(files);
        assertThat(selected).containsExactly(inside, noEndpoints, noLast, noMetadata);
        assertThat(plan.canScan(selected, 400)).isTrue();
        assertThat(plan.canScan(selected, 399)).isFalse();
        assertThat(plan.canScan(selected, 0)).isFalse();
        assertThat(plan.canScan(Collections.emptyList(), 0)).isTrue();
        assertThat(
                        CompositeBTreePredicate.plan(
                                        type.getFields(),
                                        PredicateBuilder.and(range, b.lessOrEqual(1, 7)))
                                .get()
                                .selectFiles(files))
                .isEmpty();
        assertThat(
                        CompositeBTreePredicate.plan(type.getFields(), b.isNull(0))
                                .get()
                                .selectFiles(files))
                .isEmpty();
        List<Object> sixteen =
                IntStream.range(0, 16).boxed().map(i -> (Object) i).collect(Collectors.toList());
        List<Object> seventeen =
                IntStream.range(0, 17).boxed().map(i -> (Object) i).collect(Collectors.toList());
        CompositeBTreePredicate.Plan points =
                CompositeBTreePredicate.plan(
                                type.getFields(),
                                PredicateBuilder.and(b.in(0, sixteen), b.in(1, sixteen)))
                        .get();
        assertThat(points.intervals()).hasSize(256);
        assertThat(points.isPointLookup()).isTrue();
        assertThat(points.canScan(files, 0)).isTrue();
        assertThat(
                        CompositeBTreePredicate.plan(
                                type.getFields(),
                                PredicateBuilder.and(b.in(0, sixteen), b.in(1, seventeen))))
                .isEmpty();
    }

    @Test
    void testKeyFiltersSkipRejectedPostingDecodeAndHonorExactBudget() throws Exception {
        RowType type =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "category", DataTypes.STRING().notNull()),
                                new DataField(1, "item_number", DataTypes.INT().notNull()),
                                new DataField(2, "tag", DataTypes.STRING().notNull())));
        CompositeKeySerializer serializer = new CompositeKeySerializer(type);
        GenericRow accepted = row("category-a", 8, "x");
        GenericRow rejected = row("category-a", 8, "y");
        LocalFileIO io = LocalFileIO.create();
        Path path = new Path(tempPath.resolve("filtered-posting").toUri());
        try (PositionOutputStream out = io.newOutputStream(path, false)) {
            SstFileWriter writer = new SstFileWriter(out, 128, null, null);
            LongArrayList posting = new LongArrayList(1);
            posting.add(0);
            writer.put(serializer.serialize(accepted), BTreePostingList.serialize(posting));
            // Invalid payload: scanning this rejected key must never decode its posting.
            writer.put(serializer.serialize(rejected), new byte[] {99});
            writer.flush();
            writer.writeSlice(
                    BTreeFileFooter.writeFooter(
                            new BTreeFileFooter(
                                    BTreeFileFooter.VERSION_2,
                                    null,
                                    writer.writeIndexBlock(),
                                    null)));
        }
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(
                        path,
                        io.getFileSize(path),
                        2,
                        new SortedIndexFileMeta(
                                        serializer.serialize(accepted),
                                        serializer.serialize(rejected),
                                        false)
                                .serialize());
        PredicateBuilder b = new PredicateBuilder(type);
        Predicate range =
                PredicateBuilder.and(
                        b.equal(0, BinaryString.fromString("category-a")), b.greaterThan(1, 7));
        Predicate filtered = PredicateBuilder.and(range, b.equal(2, BinaryString.fromString("x")));
        ExecutorService executor = newDirectExecutorService();
        try {
            for (long budget : Arrays.asList(meta.fileSize(), meta.fileSize() - 1)) {
                Options options = new Options();
                options.set(
                        BTreeIndexOptions.BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE,
                        MemorySize.ofBytes(budget));
                GlobalIndexer indexer =
                        GlobalIndexer.create(
                                "btree",
                                type.getFields().get(0),
                                type.getFields().subList(1, 3),
                                options);
                try (GlobalIndexReader reader =
                        indexer.createReader(
                                file -> io.newInputStream(file.filePath()),
                                Collections.singletonList(meta),
                                2,
                                null,
                                executor)) {
                    if (budget == meta.fileSize()) {
                        assertThat(
                                        reader.visitComposite(filtered)
                                                .get()
                                                .get()
                                                .results()
                                                .toRangeList())
                                .containsExactly(new Range(0, 0));
                        assertThatThrownBy(() -> reader.visitComposite(range).get())
                                .hasStackTraceContaining("Unknown BTree posting list type: 99");
                    } else {
                        assertThat(reader.visitComposite(filtered).get()).isEmpty();
                    }
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private GlobalIndexIOMeta metadata(
            String name, CompositeKeySerializer serializer, GenericRow first, GenericRow last) {
        return new GlobalIndexIOMeta(
                new Path(name),
                100,
                1,
                new SortedIndexFileMeta(
                                first == null ? null : serializer.serialize(first),
                                last == null ? null : serializer.serialize(last),
                                false)
                        .serialize());
    }

    @Test
    void testCompositeKeysPreserveTypesBoundariesAndNulls() {
        RowType type =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "category", DataTypes.STRING()),
                                new DataField(20, "item_number", DataTypes.INT()),
                                new DataField(30, "tag", DataTypes.STRING())));
        SortedGlobalIndexer indexer =
                (SortedGlobalIndexer)
                        GlobalIndexer.create(
                                "btree",
                                type.getFields().get(0),
                                type.getFields().subList(1, 3),
                                new Options());
        KeySerializer serializer =
                new CompositeKeySerializer((RowType) indexer.keyExtractor().keyType());
        Comparator<Object> comparator = serializer.createComparator();
        GenericRow first = row("category-a", -1, "a\u0000b");
        GenericRow second = row("category-a", 107, "");
        GenericRow nullable = row(null, 107, null);
        for (GenericRow key : Arrays.asList(first, second, nullable)) {
            Object restored = serializer.deserialize(MemorySlice.wrap(serializer.serialize(key)));
            assertThat(restored).isEqualTo(key);
            assertThat(comparator.compare(restored, key)).isZero();
        }
        assertThat(comparator.compare(first, second)).isNegative();
        assertThat(comparator.compare(nullable, first)).isNegative();
        assertThat(serializer.serialize(row("a", 1, "bc")))
                .isNotEqualTo(serializer.serialize(row("ab", 1, "c")));
        assertThat(
                        GlobalIndexer.create(
                                "btree",
                                type.getFields().get(0),
                                Collections.emptyList(),
                                new Options()))
                .isInstanceOf(BTreeGlobalIndexer.class);
    }

    @Test
    void testCompositeSupportDoesNotEnablePhysicalRowIndexes() {
        DataField field = new DataField(40, "nested", RowType.of(DataTypes.INT()));
        for (String indexType : Arrays.asList("btree", "bitmap")) {
            assertThatThrownBy(() -> GlobalIndexer.create(indexType, field, new Options()))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private GenericRow row(String category, int itemNumber, String tag) {
        return GenericRow.of(
                category == null ? null : BinaryString.fromString(category),
                itemNumber,
                tag == null ? null : BinaryString.fromString(tag));
    }
}
