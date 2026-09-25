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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexScanner.IndexMetaFileGroup;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests predicate type preservation and unsupported predicates in index query plans. */
class GlobalIndexQueryPlanTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"false,false", "true,true"})
    void testBoundedRangeUsesSingleScan(boolean lowerInclusive, boolean upperInclusive)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate lower = lowerInclusive ? builder.greaterOrEqual(0, 5) : builder.greaterThan(0, 5);
        Predicate upper = upperInclusive ? builder.lessOrEqual(0, 9) : builder.lessThan(0, 9);
        Predicate range = new CompoundPredicate(And.INSTANCE, Arrays.asList(upper, lower));
        List<IndexFileMeta> files = new ArrayList<>();
        for (int firstKey = 0; firstKey < 15; firstKey += 5) {
            files.add(indexFile("btree", "index-" + firstKey, 0, 99, 0, null));
        }
        IndexPathFactory paths = mock(IndexPathFactory.class);
        when(paths.toPath(any(IndexFileMeta.class)))
                .thenAnswer(
                        invocation ->
                                new Path(invocation.<IndexFileMeta>getArgument(0).fileName()));
        Options options = new Options();
        GlobalIndexQueryPlan plan = GlobalIndexQueryPlan.create(rowType, range, files, paths);
        assertThat(plan).isNotNull();
        IndexQuerySplit split =
                new IndexQuerySplit(dataSplit(), plan, options.toMap(), Collections.emptyList());
        assertThat(SplitSerializer.deserialize(SplitSerializer.serialize(split))).isEqualTo(split);

        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitRange(any(), eq(5), eq(9), eq(lowerInclusive), eq(upperInclusive)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(
                                        GlobalIndexResult.fromRange(
                                                new Range(
                                                        lowerInclusive ? 5 : 6,
                                                        upperInclusive ? 9 : 8)))));
        GlobalIndexer indexer = mock(GlobalIndexer.class);
        when(indexer.createReader(any(), anyList(), eq(100L), anyList(), any())).thenReturn(reader);
        GlobalIndexerFactory factory = mock(GlobalIndexerFactory.class);
        when(factory.create(any(DataField.class), anyList(), any(Options.class)))
                .thenReturn(indexer);
        try (MockedStatic<GlobalIndexerFactoryUtils> factories =
                mockStatic(GlobalIndexerFactoryUtils.class)) {
            factories.when(() -> GlobalIndexerFactoryUtils.load("btree")).thenReturn(factory);
            List<Range> ranges = Collections.singletonList(new Range(0, 99));
            assertThat(plan.evaluate(mock(FileIO.class), options, ranges).results().toRangeList())
                    .containsExactly(new Range(lowerInclusive ? 5 : 6, upperInclusive ? 9 : 8));
            verify(indexer)
                    .createReader(
                            any(),
                            argThat(selected -> selected.size() == 3),
                            eq(100L),
                            anyList(),
                            any());
            verify(reader).visitRange(any(), eq(5), eq(9), eq(lowerInclusive), eq(upperInclusive));
        }

        assertThat(
                        GlobalIndexQueryPlan.create(
                                rowType, PredicateBuilder.or(lower, upper), files, paths))
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    void testNegativePredicatesRetainNullFilesAndOriginalRowCount(String indexType)
            throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory paths =
                new IndexPathFactory() {
                    @Override
                    public Path toPath(String name) {
                        return new Path(tempDir.resolve(name).toString());
                    }

                    @Override
                    public Path newPath() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isExternalPath() {
                        return false;
                    }
                };
        GlobalIndexFileReadWrite io = new GlobalIndexFileReadWrite(fileIO, paths);
        List<IndexFileMeta> files = new ArrayList<>();
        for (boolean nulls : new boolean[] {true, false}) {
            GlobalIndexSingleColumnWriter writer =
                    (GlobalIndexSingleColumnWriter)
                            GlobalIndexer.create(
                                            indexType, rowType.getFields().get(0), new Options())
                                    .createWriter(io);
            if (nulls) {
                writer.write(null, 0);
            } else {
                writer.write(1, 1);
                writer.write(2, 2);
            }
            for (ResultEntry entry : writer.finish()) {
                files.add(
                        new IndexFileMeta(
                                indexType,
                                entry.fileName(),
                                io.fileSize(entry.fileName()),
                                entry.rowCount(),
                                new GlobalIndexMeta(100, 102, 0, null, entry.meta()),
                                null));
            }
        }
        assertThat(files).hasSize(2);
        PredicateBuilder b = new PredicateBuilder(rowType);
        List<Predicate> predicates =
                Arrays.asList(b.isNotNull(0), b.notEqual(0, 1), b.notIn(0, Arrays.asList(1, 3)));
        for (int i = 0; i < predicates.size(); i++) {
            GlobalIndexQueryPlan plan =
                    GlobalIndexQueryPlan.create(rowType, predicates.get(i), files, paths);
            List<Range> ranges = Collections.singletonList(new Range(100, 102));
            assertThat(plan).isNotNull();
            assertThat(plan.evaluate(fileIO, new Options(), ranges).results().toRangeList())
                    .containsExactly(new Range(i == 0 ? 101 : 102, 102));
            List<Range> tail = Collections.singletonList(new Range(102, 102));
            assertThat(
                            plan.forRanges(tail)
                                    .evaluate(fileIO, new Options(), tail)
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(102, 102));
        }
    }

    static Stream<Arguments> literals() {
        return Stream.of("btree", "bitmap")
                .flatMap(
                        indexType ->
                                Stream.of(
                                        Arguments.of(indexType, DataTypes.DATE(), 20000),
                                        Arguments.of(indexType, DataTypes.TIME(), 12345),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.TIMESTAMP(6),
                                                Timestamp.fromEpochMillis(-12345678, 123000)),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6),
                                                Timestamp.fromEpochMillis(-12345678, 123000)),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.DECIMAL(10, 2),
                                                Decimal.fromBigDecimal(
                                                        new BigDecimal("123.45"), 10, 2)),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.DECIMAL(38, 18),
                                                Decimal.fromBigDecimal(
                                                        new BigDecimal(
                                                                "12345678901234567890.123456789012345678"),
                                                        38,
                                                        18)),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.DOUBLE(),
                                                Double.POSITIVE_INFINITY),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.DOUBLE(),
                                                Double.NEGATIVE_INFINITY),
                                        Arguments.of(indexType, DataTypes.DOUBLE(), Double.NaN),
                                        Arguments.of(indexType, DataTypes.DOUBLE(), 123.45),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.FLOAT(),
                                                Float.POSITIVE_INFINITY),
                                        Arguments.of(indexType, DataTypes.FLOAT(), Float.NaN),
                                        Arguments.of(
                                                indexType,
                                                DataTypes.STRING(),
                                                BinaryString.fromString("value"))));
    }

    @ParameterizedTest
    @MethodSource("literals")
    void testSplitPreservesPredicateLiterals(String indexType, DataType type, Object literal)
            throws Exception {
        RowType rowType = RowType.of(type);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate equal = builder.equal(0, literal);
        for (Predicate predicate :
                Arrays.asList(
                        equal,
                        PredicateBuilder.and(equal, builder.isNotNull(0)),
                        PredicateBuilder.or(equal, builder.isNull(0)))) {
            GlobalIndexQueryPlan plan = create(indexType, rowType, predicate, literal);
            assertThat(plan).isNotNull();
            IndexQuerySplit split =
                    new IndexQuerySplit(
                            dataSplit(), plan, Collections.emptyMap(), Collections.emptyList());
            assertThat(SplitSerializer.deserialize(SplitSerializer.serialize(split)))
                    .isEqualTo(split);
            assertThat(
                            (Object)
                                    InstantiationUtil.deserializeObject(
                                            InstantiationUtil.serializeObject(split),
                                            getClass().getClassLoader()))
                    .isEqualTo(split);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    void testUnsupportedPredicateFailsAtRuntime(String indexType) {
        RowType rowType = RowType.of(DataTypes.DOUBLE());
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate isNaN = builder.isNaN(0);
        Predicate equal = builder.equal(0, 1.0);
        GlobalIndexQueryPlan unsupported = create(indexType, rowType, isNaN, 1.0);
        assertThat(unsupported).isNotNull();
        assertThat(create(indexType, rowType, PredicateBuilder.or(isNaN, equal), 1.0)).isNotNull();
        GlobalIndexQueryPlan and =
                create(indexType, rowType, PredicateBuilder.and(isNaN, equal), 1.0);
        assertThat(and).isNotNull();

        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitIsNaN(any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        GlobalIndexer indexer = mock(GlobalIndexer.class);
        when(indexer.createReader(any(), anyList(), anyLong(), anyList(), any()))
                .thenReturn(reader);
        GlobalIndexerFactory factory = mock(GlobalIndexerFactory.class);
        when(factory.create(any(DataField.class), anyList(), any(Options.class)))
                .thenReturn(indexer);
        try (MockedStatic<GlobalIndexerFactoryUtils> factories =
                mockStatic(GlobalIndexerFactoryUtils.class)) {
            factories.when(() -> GlobalIndexerFactoryUtils.load(indexType)).thenReturn(factory);
            assertThatThrownBy(
                            () ->
                                    unsupported.evaluate(
                                            mock(FileIO.class),
                                            new Options(),
                                            Collections.singletonList(new Range(0, 0))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("does not support predicate");
        }
    }

    private DataSplit dataSplit() {
        DataFileMeta file =
                DataFileMeta.forAppend(
                        "data",
                        1,
                        1,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        0L,
                        null);
        return DataSplit.builder()
                .withSnapshot(1)
                .withPartition(BinaryRow.EMPTY_ROW)
                .withBucket(0)
                .withBucketPath("bucket-0")
                .withDataFiles(Collections.singletonList(file))
                .build();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 100, 4_294_967_296L})
    void testClipsBillionsOfMatchesBeforeMaterializingRanges(long offset) throws Exception {
        long lastRow = 3_000_000_100L;
        RoaringNavigableMap64 rows =
                new RoaringNavigableMap64() {
                    @Override
                    public Iterator<Long> iterator() {
                        throw new AssertionError(
                                "Must clip the index result before offsetting rows");
                    }

                    @Override
                    public List<Range> toRangeList() {
                        throw new AssertionError(
                                "Must clip the index result before expanding ranges");
                    }
                };
        rows.addRange(new Range(0, lastRow));
        rows.andNot(GlobalIndexResult.fromRange(new Range(2, 2)).results());
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), eq(1)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(GlobalIndexResult.create(rows))));
        GlobalIndexer indexer = mock(GlobalIndexer.class);
        when(indexer.createReader(any(), anyList(), anyLong(), anyList(), any()))
                .thenReturn(reader);
        GlobalIndexerFactory factory = mock(GlobalIndexerFactory.class);
        when(factory.create(any(DataField.class), anyList(), any(Options.class)))
                .thenReturn(indexer);

        RowType rowType = RowType.of(DataTypes.INT());
        GlobalIndexQueryPlan plan =
                create(
                        "bitmap",
                        rowType,
                        new PredicateBuilder(rowType).equal(0, 1),
                        1,
                        offset,
                        offset + lastRow);
        try (MockedStatic<GlobalIndexerFactoryUtils> factories =
                mockStatic(GlobalIndexerFactoryUtils.class)) {
            factories.when(() -> GlobalIndexerFactoryUtils.load("bitmap")).thenReturn(factory);
            List<Range> first = Collections.singletonList(new Range(offset + 1, offset + 3));
            List<Range> second =
                    Collections.singletonList(
                            new Range(offset + 3_000_000_000L, offset + 3_000_000_004L));
            GlobalIndexResult firstResult =
                    plan.forRanges(first).evaluate(mock(FileIO.class), new Options(), first);
            GlobalIndexResult secondResult =
                    plan.forRanges(second).evaluate(mock(FileIO.class), new Options(), second);
            assertThat(firstResult.results().toRangeList())
                    .containsExactly(
                            new Range(offset + 1, offset + 1), new Range(offset + 3, offset + 3));
            assertThat(secondResult.results().toRangeList()).containsExactlyElementsOf(second);
            assertThat(firstResult.and(secondResult).results().isEmpty()).isTrue();
            List<Range> boundaries =
                    Arrays.asList(
                            new Range(Math.max(0, offset - 1), offset + 1),
                            new Range(offset + lastRow - 1, offset + lastRow + 1));
            assertThat(
                            plan.evaluate(mock(FileIO.class), new Options(), boundaries)
                                    .results()
                                    .toRangeList())
                    .containsExactly(
                            new Range(offset, offset + 1),
                            new Range(offset + lastRow - 1, offset + lastRow));
            List<Range> disjoint =
                    Collections.singletonList(
                            new Range(offset + lastRow + 1, offset + lastRow + 2));
            assertThat(
                            plan.evaluate(mock(FileIO.class), new Options(), disjoint)
                                    .results()
                                    .isEmpty())
                    .isTrue();
            assertThat(
                            plan.evaluate(
                                            mock(FileIO.class),
                                            new Options(),
                                            Collections.emptyList())
                                    .results()
                                    .isEmpty())
                    .isTrue();
            verify(indexer, times(3))
                    .createReader(any(), anyList(), eq(lastRow + 1), anyList(), any());
            verify(reader, times(3)).close();
            assertThat(rows.getLongCardinality()).isEqualTo(lastRow);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"btree", "bitmap"})
    void testPushesLocalRangesToIndexer(String indexType) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        GlobalIndexQueryPlan plan =
                create(indexType, rowType, new PredicateBuilder(rowType).equal(0, 1), 1, 100, 199);
        GlobalIndexReader reader = mock(GlobalIndexReader.class);
        when(reader.visitEqual(any(), eq(1)))
                .thenReturn(
                        CompletableFuture.completedFuture(
                                Optional.of(GlobalIndexResult.fromRange(new Range(0, 99)))));
        GlobalIndexer indexer = mock(GlobalIndexer.class);
        when(indexer.createReader(any(), anyList(), eq(100L), anyList(), any())).thenReturn(reader);
        GlobalIndexerFactory factory = mock(GlobalIndexerFactory.class);
        when(factory.create(any(DataField.class), anyList(), any(Options.class)))
                .thenReturn(indexer);
        FileIO fileIO = mock(FileIO.class);
        List<Range> first = Collections.singletonList(new Range(110, 119));
        List<Range> second = Collections.singletonList(new Range(130, 139));

        try (MockedStatic<GlobalIndexerFactoryUtils> factories =
                mockStatic(GlobalIndexerFactoryUtils.class)) {
            factories.when(() -> GlobalIndexerFactoryUtils.load(indexType)).thenReturn(factory);
            assertThat(
                            plan.forRanges(first)
                                    .evaluate(fileIO, new Options(), first)
                                    .results()
                                    .toRangeList())
                    .containsExactlyElementsOf(first);
            assertThat(
                            plan.forRanges(second)
                                    .evaluate(fileIO, new Options(), second)
                                    .results()
                                    .toRangeList())
                    .containsExactlyElementsOf(second);
            verify(indexer)
                    .createReader(
                            any(),
                            anyList(),
                            eq(100L),
                            eq(Collections.singletonList(new Range(10, 19))),
                            any());
            verify(indexer)
                    .createReader(
                            any(),
                            anyList(),
                            eq(100L),
                            eq(Collections.singletonList(new Range(30, 39))),
                            any());
            verify(indexer, times(2)).createReader(any(), anyList(), eq(100L), anyList(), any());
        }
    }

    @Test
    void testGroupsPreserveIndexTypesRangesAndExtraFields() {
        IndexFileMeta first = indexFile("es-index", "first", 0, 99, 10, new int[] {20});
        IndexFileMeta sameRange = indexFile("es-index", "same-range", 0, 99, 10, new int[] {20});
        IndexFileMeta tail = indexFile("es-index", "tail", 100, 199, 10, new int[] {20});
        IndexFileMeta primary = indexFile("btree", "primary", 0, 99, 20, null);
        IndexFileMeta bitmap = indexFile("bitmap", "bitmap", 0, 99, 20, null);
        Map<Integer, List<IndexMetaFileGroup>> groups =
                DataEvolutionGlobalIndexScanner.groupIndexFiles(
                        Arrays.asList(first, sameRange, tail, primary, bitmap));
        assertThat(groups).containsOnlyKeys(10, 20);
        assertThat(groups.get(10)).hasSize(1);
        assertThat(groups.get(20)).hasSize(2);
        IndexMetaFileGroup extra = groups.get(10).get(0);
        assertThat(groups.get(20).get(1)).isSameAs(extra);
        assertThat(extra.metas().get("es-index"))
                .containsOnlyKeys(new Range(0, 99), new Range(100, 199));
        assertThat(extra.metas().get("es-index").get(new Range(0, 99)))
                .containsExactly(first, sameRange);
        assertThat(extra.metas().get("es-index").get(new Range(100, 199))).containsExactly(tail);
        assertThat(groups.get(20).get(0).metas()).containsOnlyKeys("btree", "bitmap");
    }

    @Test
    void testGroupsRejectInconsistentIndexedFields() {
        assertThatThrownBy(
                        () ->
                                DataEvolutionGlobalIndexScanner.groupIndexFiles(
                                        Arrays.asList(
                                                indexFile(
                                                        "es-index",
                                                        "first",
                                                        0,
                                                        99,
                                                        10,
                                                        new int[] {20}),
                                                indexFile("es-index", "tail", 100, 199, 10, null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different columns");
    }

    @Test
    void testPlansAvailableIndexesWithoutTypeChecks() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.INT());
        PredicateBuilder builder = new PredicateBuilder(rowType);
        byte[] key = KeySerializer.create(DataTypes.INT()).serialize(1);
        IndexFileMeta supported =
                new IndexFileMeta(
                        "btree",
                        "supported",
                        1,
                        100,
                        new GlobalIndexMeta(
                                0,
                                99,
                                0,
                                null,
                                new SortedIndexFileMeta(key, key, false).serialize()),
                        null);
        IndexFileMeta multiColumn = indexFile("custom-index", "multi", 0, 99, 1, new int[] {2});
        IndexPathFactory paths = mock(IndexPathFactory.class);
        when(paths.toPath(any(IndexFileMeta.class))).thenReturn(new Path("index"));
        List<IndexFileMeta> files = Arrays.asList(supported, multiColumn);

        Predicate supportedLeaf = builder.equal(0, 1);
        Predicate unsupportedLeaf = builder.equal(1, 2);
        GlobalIndexQueryPlan andPlan =
                GlobalIndexQueryPlan.create(
                        rowType,
                        PredicateBuilder.and(supportedLeaf, unsupportedLeaf),
                        files,
                        paths);
        assertThat(andPlan).isNotNull();
        assertThat(andPlan.contributingFieldIds(rowType)).containsExactlyInAnyOrder(0, 1);
        IndexQuerySplit split =
                new IndexQuerySplit(
                        dataSplit(), andPlan, Collections.emptyMap(), Collections.emptyList());
        assertThat(SplitSerializer.deserialize(SplitSerializer.serialize(split))).isEqualTo(split);
        assertThat(
                        GlobalIndexQueryPlan.create(
                                rowType,
                                PredicateBuilder.or(supportedLeaf, unsupportedLeaf),
                                files,
                                paths))
                .isNotNull();
        assertThat(
                        GlobalIndexQueryPlan.create(
                                rowType,
                                supportedLeaf,
                                Arrays.asList(supported, indexFile("fm", "fm", 0, 99, 0, null)),
                                paths))
                .isNotNull();
    }

    private IndexFileMeta indexFile(
            String type, String name, long start, long end, int fieldId, int[] extraFields) {
        return new IndexFileMeta(
                type,
                name,
                1,
                end - start + 1,
                new GlobalIndexMeta(start, end, fieldId, extraFields, null),
                null);
    }

    private GlobalIndexQueryPlan create(
            String indexType, RowType rowType, Predicate predicate, Object literal) {
        return create(indexType, rowType, predicate, literal, 0, 0);
    }

    private GlobalIndexQueryPlan create(
            String indexType,
            RowType rowType,
            Predicate predicate,
            Object literal,
            long firstRow,
            long lastRow) {
        byte[] key = KeySerializer.create(rowType.getFields().get(0).type()).serialize(literal);
        IndexFileMeta file =
                new IndexFileMeta(
                        indexType,
                        "index",
                        1,
                        lastRow - firstRow + 1,
                        new GlobalIndexMeta(
                                firstRow,
                                lastRow,
                                0,
                                null,
                                new SortedIndexFileMeta(key, key, false).serialize()),
                        null);
        IndexPathFactory paths =
                new IndexPathFactory() {
                    @Override
                    public Path toPath(String name) {
                        return new Path("index/" + name);
                    }

                    @Override
                    public Path newPath() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean isExternalPath() {
                        return false;
                    }
                };
        return GlobalIndexQueryPlan.create(
                rowType, predicate, Collections.singletonList(file), paths);
    }
}
