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
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

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
            for (List<Object> invalid :
                    Arrays.asList(
                            Arrays.<Object>asList(BinaryString.fromString("category-a"), 7),
                            Arrays.<Object>asList(
                                    BinaryString.fromString("category-a"),
                                    7,
                                    BinaryString.fromString("tag"),
                                    BinaryString.fromString("extra")),
                            Arrays.<Object>asList(null, 7, BinaryString.fromString("tag"), null))) {
                assertThatThrownBy(() -> reader.visitCompositeEqual(invalid))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Expected 3 composite key fields");
            }
            assertThat(
                            reader.visitCompositeEqual(
                                            Arrays.asList(null, 7, BinaryString.fromString("tag")))
                                    .get()
                                    .get()
                                    .results()
                                    .toRangeList())
                    .isEmpty();
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
        for (GenericRow invalid :
                Arrays.asList(
                        GenericRow.of(BinaryString.fromString("category-a"), 7),
                        GenericRow.of(
                                BinaryString.fromString("category-a"),
                                7,
                                BinaryString.fromString("tag"),
                                BinaryString.fromString("extra")))) {
            assertThatThrownBy(() -> serializer.serialize(invalid))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Expected 3 composite key fields");
        }
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

    private GenericRow row(String category, int itemNumber, String tag) {
        return GenericRow.of(
                category == null ? null : BinaryString.fromString(category),
                itemNumber,
                tag == null ? null : BinaryString.fromString(tag));
    }
}
