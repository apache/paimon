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
import org.apache.paimon.data.serializer.RowCompactedSerializer;
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
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for typed composite BTree keys. */
class CompositeBTreeIndexTest {

    @TempDir java.nio.file.Path tempPath;

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testDeserializedTupleKeysPostingListsAndLocalRanges(int version) throws Exception {
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
        options.set(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE, MemorySize.ofBytes(64));
        GlobalIndexer indexer = GlobalIndexer.create("btree", type.getFields(), options);
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
        reused.setRowKind(RowKind.UPDATE_AFTER);
        KeySerializer serializer = new CompositeKeySerializer(type);
        List<byte[]> keys = new ArrayList<>();
        keys.add(serializer.serialize(reused));
        reused.setField(1, 7);
        keys.add(serializer.serialize(reused));
        reused.setField(2, BinaryString.fromString("tag"));
        keys.add(serializer.serialize(reused));
        keys.add(serializer.serialize(reused));
        reused.setField(0, BinaryString.fromString("category-b"));
        keys.add(serializer.serialize(reused));
        GlobalIndexSingleColumnWriter writer =
                (GlobalIndexSingleColumnWriter) indexer.createWriter(files);
        for (int i = 0; i < keys.size(); i++) {
            writer.write(serializer.deserialize(MemorySlice.wrap(keys.get(i))), i);
        }
        ResultEntry result = writer.finish().get(0);
        SortedIndexFileMeta indexMeta = SortedIndexFileMeta.deserialize(result.meta());
        assertThat(serializer.deserialize(MemorySlice.wrap(indexMeta.getFirstKey())))
                .isEqualTo(row("category-a", -1, ""));
        assertThat(serializer.deserialize(MemorySlice.wrap(indexMeta.getLastKey())))
                .isEqualTo(row("category-b", 7, "tag"));
        Path path = new Path(directory, result.fileName());
        GlobalIndexIOMeta meta =
                new GlobalIndexIOMeta(path, io.getFileSize(path), result.rowCount(), result.meta());
        AtomicInteger deserializations = new AtomicInteger();
        KeySerializer countingSerializer =
                new CompositeKeySerializer(type) {
                    @Override
                    public Object deserialize(MemorySlice data) {
                        deserializations.incrementAndGet();
                        return super.deserialize(data);
                    }
                };
        try (CacheManager cache = new CacheManager(MemorySize.ofMebiBytes(1), 0.1);
                BTreeIndexReader reader =
                        new BTreeIndexReader(
                                countingSerializer,
                                file -> io.newInputStream(file.filePath()),
                                meta,
                                cache,
                                null)) {
            int onOpen = deserializations.get();
            assertThat(reader.visitEqual(row("category-a", -1, "")).get().results().toRangeList())
                    .containsExactly(new Range(0, 0));
            assertThat(reader.visitEqual(row("category-a", 7, "tag")).get().results().toRangeList())
                    .containsExactly(new Range(2, 3));
            assertThat(reader.visitEqual(row("category-b", 7, "tag")).get().results().toRangeList())
                    .containsExactly(new Range(4, 4));
            assertThat(reader.visitEqual(row("category-a", 8, "tag")).get().results()).isEmpty();
            assertThat(deserializations.get()).isEqualTo(onOpen);
        }
        assertThat(
                        new BTreeGlobalIndexerFactory()
                                .selectFiles(
                                        type.getFields(),
                                        new PredicateBuilder(type)
                                                .equal(0, BinaryString.fromString("category-a")),
                                        Collections.singletonList(meta)))
                .containsExactly(meta);
        ExecutorService executor = newDirectExecutorService();
        try (GlobalIndexReader reader =
                indexer.createReader(
                        file -> io.newInputStream(file.filePath()),
                        Collections.singletonList(meta),
                        5,
                        null,
                        executor)) {
            assertThat(
                            reader.visitComposite(
                                            equal(type, null, 7, BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .isEmpty();
            assertThat(
                            reader.visitComposite(
                                            equal(
                                                    type,
                                                    BinaryString.fromString("category-a"),
                                                    -1,
                                                    BinaryString.fromString("")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(0, 0));
            assertThat(
                            reader.visitComposite(
                                            equal(
                                                    type,
                                                    BinaryString.fromString("category-a"),
                                                    7,
                                                    BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(2, 3));
            assertThat(
                            reader.visitComposite(
                                            equal(
                                                    type,
                                                    BinaryString.fromString("category-a"),
                                                    8,
                                                    BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .isEmpty())
                    .isTrue();
            PredicateBuilder builder = new PredicateBuilder(type);
            Predicate fullKey =
                    equal(
                            type,
                            BinaryString.fromString("category-a"),
                            7,
                            BinaryString.fromString("tag"));
            for (Predicate unsupported :
                    Arrays.asList(
                            builder.equal(1, 7),
                            builder.notEqual(0, BinaryString.fromString("category-a")),
                            PredicateBuilder.or(fullKey, builder.equal(1, 8)))) {
                assertThat(reader.visitComposite(unsupported).join()).isEmpty();
            }
            assertThat(
                            reader.visitComposite(
                                            PredicateBuilder.and(fullKey, builder.equal(1, 8)))
                                    .join()
                                    .get()
                                    .results()
                                    .isEmpty())
                    .isTrue();
            RowType predicateType =
                    new RowType(
                            Arrays.asList(
                                    type.getFields().get(2),
                                    type.getFields().get(0),
                                    type.getFields().get(1)));
            assertThat(
                            reader.visitComposite(
                                            equal(
                                                    predicateType,
                                                    BinaryString.fromString("tag"),
                                                    BinaryString.fromString("category-a"),
                                                    7))
                                    .join()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .containsExactly(new Range(2, 3));
        }
        try (GlobalIndexReader reader =
                indexer.createReader(
                        file -> io.newInputStream(file.filePath()),
                        Collections.singletonList(meta),
                        5,
                        Collections.singletonList(new Range(3, 3)),
                        executor)) {
            assertThat(
                            reader.visitComposite(
                                            equal(
                                                    type,
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

    @Test
    void testScalarReaderDoesNotSupportCompositePredicates() throws Exception {
        RowType type = RowType.of(DataTypes.INT());
        GlobalIndexer indexer = GlobalIndexer.create("btree", type.getFields(), new Options());
        ExecutorService executor = newDirectExecutorService();
        try (GlobalIndexReader reader =
                indexer.createReader(
                        meta -> LocalFileIO.create().newInputStream(meta.filePath()),
                        Collections.emptyList(),
                        5,
                        null,
                        executor)) {
            assertThat(reader.visitComposite(equal(type, 7)).join()).isEmpty();
        } finally {
            executor.shutdownNow();
        }
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
                        GlobalIndexer.create("btree", type.getFields(), new Options());
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
        SortedGlobalIndexer scalarIndexer =
                (SortedGlobalIndexer)
                        GlobalIndexer.create(
                                "btree",
                                Collections.singletonList(type.getFields().get(0)),
                                new Options());
        assertThat(scalarIndexer.keyExtractor().keyType()).isEqualTo(DataTypes.STRING());
        assertThat(indexer.keyExtractor().keyType()).isEqualTo(type);
    }

    @Test
    void testDeserializedKeysRemainIndependentOfSubsequentReadsAndInputBuffers() {
        KeySerializer serializer =
                new CompositeKeySerializer(
                        RowType.of(DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()));
        GenericRow expectedFirst = row("category-a", 7, "first");
        GenericRow expectedSecond = row("category-b", 8, "second");
        byte[] firstBytes = serializer.serialize(expectedFirst);
        Object first = serializer.deserialize(MemorySlice.wrap(firstBytes));
        Arrays.fill(firstBytes, (byte) 0);
        byte[] secondBytes = serializer.serialize(expectedSecond);
        Object second = serializer.deserialize(MemorySlice.wrap(secondBytes));
        Arrays.fill(secondBytes, (byte) 0);
        for (int i = 0; i < 100; i++) {
            serializer.deserialize(MemorySlice.wrap(serializer.serialize(row("other", i, null))));
        }
        assertThat(first).isEqualTo(expectedFirst);
        assertThat(second).isEqualTo(expectedSecond);
        assertThat(serializer.createComparator().compare(first, second)).isNegative();
    }

    @Test
    void testCompactedEncodingIgnoresRowKind() {
        RowType type = RowType.of(DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING());
        KeySerializer serializer = new CompositeKeySerializer(type);
        GenericRow expected = row("category-a", -1, null);
        byte[] compacted = new RowCompactedSerializer(type).serializeToBytes(expected);
        for (RowKind kind : RowKind.values()) {
            GenericRow key = row("category-a", -1, null);
            key.setRowKind(kind);
            byte[] bytes = serializer.serialize(key);
            assertThat(bytes).isEqualTo(compacted);
            assertThat(key.getRowKind()).isEqualTo(kind);
            assertThat(serializer.deserialize(MemorySlice.wrap(bytes))).isEqualTo(expected);
            assertThat(serializer.createComparator().compare(key, expected)).isZero();
        }
    }

    @Test
    void testEqualNaNKeysHaveIdenticalEncodings() {
        KeySerializer serializer =
                new CompositeKeySerializer(RowType.of(DataTypes.FLOAT(), DataTypes.DOUBLE()));
        GenericRow canonical = GenericRow.of(Float.NaN, Double.NaN);
        GenericRow alternate =
                GenericRow.of(
                        Float.intBitsToFloat(0xffc00001),
                        Double.longBitsToDouble(0xfff8000000000001L));
        assertThat(serializer.createComparator().compare(canonical, alternate)).isZero();
        assertThat(serializer.serialize(alternate)).isEqualTo(serializer.serialize(canonical));
        assertThat(serializer.deserialize(MemorySlice.wrap(serializer.serialize(alternate))))
                .isEqualTo(canonical);
        assertThat(Float.floatToRawIntBits(alternate.getFloat(0))).isEqualTo(0xffc00001);
        assertThat(Double.doubleToRawLongBits(alternate.getDouble(1)))
                .isEqualTo(0xfff8000000000001L);
    }

    @Test
    void testConcurrentRoundTrips() throws Exception {
        RowType type = RowType.of(DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING());
        KeySerializer serializer = new CompositeKeySerializer(type);
        Comparator<MemorySlice> sliceComparator = serializer.createSliceComparator();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int thread = 0; thread < 8; thread++) {
                final int worker = thread;
                tasks.add(
                        executor.submit(
                                () -> {
                                    start.await();
                                    for (int i = 0; i < 500; i++) {
                                        char[] chars = new char[64 + (i % 32) * 128];
                                        Arrays.fill(chars, (char) ('a' + worker));
                                        GenericRow key =
                                                row(
                                                        "category-" + worker,
                                                        i,
                                                        i % 2 == 0 ? new String(chars) : null);
                                        byte[] bytes = serializer.serialize(key);
                                        Object restored =
                                                serializer.deserialize(MemorySlice.wrap(bytes));
                                        assertThat(restored).isEqualTo(key);
                                        byte[] next =
                                                serializer.serialize(
                                                        row("category-" + worker, i + 1, null));
                                        assertThat(
                                                        sliceComparator.compare(
                                                                MemorySlice.wrap(bytes),
                                                                MemorySlice.wrap(next)))
                                                .isNegative();
                                        assertThat(
                                                        serializer
                                                                .createComparator()
                                                                .compare(restored, key))
                                                .isZero();
                                    }
                                    return null;
                                }));
            }
            start.countDown();
            for (Future<?> task : tasks) {
                task.get(30, TimeUnit.SECONDS);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testFactoryRejectsUnsupportedFieldLists() {
        assertThatThrownBy(
                        () -> GlobalIndexer.create("btree", Collections.emptyList(), new Options()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one field");
        List<DataField> fields =
                Arrays.asList(
                        new DataField(10, "category", DataTypes.STRING()),
                        new DataField(20, "item_number", DataTypes.INT()));
        for (String indexType : Arrays.asList("bitmap", "multivalue", "fm")) {
            assertThatThrownBy(() -> GlobalIndexer.create(indexType, fields, new Options()))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("exactly one index field");
        }
    }

    @Test
    void testCompositeSupportDoesNotEnablePhysicalRowIndexes() {
        DataField field = new DataField(40, "nested", RowType.of(DataTypes.INT()));
        for (String indexType : Arrays.asList("btree", "bitmap")) {
            assertThatThrownBy(
                            () ->
                                    GlobalIndexer.create(
                                            indexType,
                                            Collections.singletonList(field),
                                            new Options()))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private Predicate equal(RowType type, Object... values) {
        PredicateBuilder builder = new PredicateBuilder(type);
        List<Predicate> equalities = new ArrayList<>();
        for (int i = values.length - 1; i >= 0; i--) {
            equalities.add(builder.equal(i, values[i]));
        }
        return PredicateBuilder.and(equalities);
    }

    private GenericRow row(String category, int itemNumber, String tag) {
        return GenericRow.of(
                category == null ? null : BinaryString.fromString(category),
                itemNumber,
                tag == null ? null : BinaryString.fromString(tag));
    }
}
