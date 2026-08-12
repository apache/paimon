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

package org.apache.paimon.io;

import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndexFactory;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndex;
import org.apache.paimon.fileindex.bloomfilter.BloomFilterFileIndexFactory;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndex;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndexFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongConsumer;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileIndexEvaluator}. */
class FileIndexEvaluatorTest {

    private static final String FIELD_NAME = "value";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testLimitSkipsDeletedPositions(boolean bitmap64) throws Exception {
        DataFileMeta file = DataFileTestUtils.newFile("data.avro", 0, 0, 19, 0L);
        DeletionVector deletionVector =
                bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
        for (int position = 0; position < 5; position++) {
            deletionVector.delete(position);
        }

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null, null, Collections.emptyList(), null, 10, null, file, deletionVector);

        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get())
                .isEqualTo(RoaringBitmap32.bitmapOfRange(5, 15));
    }

    @Test
    void testLimitAbandonsBitmapPushdownForLargeFile() throws Exception {
        DataFileMeta file = fileWithRowCount(Integer.MAX_VALUE + 2L);
        DeletionVector deletionVector =
                new Bitmap64DeletionVector() {
                    @Override
                    public boolean isDeleted(long position) {
                        throw new AssertionError(
                                "Limit evaluation must not scan a large file by position.");
                    }
                };
        deletionVector.delete(0);
        deletionVector.delete(Integer.MAX_VALUE + 1L);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null, null, Collections.emptyList(), null, 10, null, file, deletionVector);

        assertThat(result).isSameAs(FileIndexResult.REMAIN);
    }

    @Test
    void testLimitDoesNotScanBitmap64DeletionVectorByPosition() throws Exception {
        DataFileMeta file = DataFileTestUtils.newFile("data.avro", 0, 0, 19, 0L);
        DeletionVector deletionVector =
                new Bitmap64DeletionVector() {
                    @Override
                    public boolean isDeleted(long position) {
                        throw new AssertionError(
                                "Limit evaluation must not scan the deletion vector by position.");
                    }
                };
        deletionVector.delete(0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null, null, Collections.emptyList(), null, 1, null, file, deletionVector);

        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get()).isEqualTo(RoaringBitmap32.bitmapOf(1));
    }

    @Test
    void testFilterProjectsBitmap64ForBroadCandidates() throws Exception {
        int rowCount = 10_000;
        TableSchema schema = tableSchema();
        FileIndexWriter indexWriter =
                new BitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
        for (int position = 0; position < rowCount; position++) {
            indexWriter.writeRecord(position == rowCount - 1 ? 1 : 0);
        }
        DataFileMeta file =
                DataFileTestUtils.newFile("data.avro", 0, 0, rowCount - 1, 0L)
                        .copy(embeddedIndex(BitmapFileIndexFactory.BITMAP_INDEX, indexWriter));
        DeletionVector deletionVector =
                new Bitmap64DeletionVector() {
                    @Override
                    public boolean isDeleted(long position) {
                        throw new AssertionError(
                                "Broad bitmap candidates must not be checked one by one.");
                    }
                };
        deletionVector.delete(0);
        deletionVector.delete(2);
        Predicate filter = new PredicateBuilder(schema.logicalRowType()).equal(0, 0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        schema,
                        Collections.singletonList(filter),
                        null,
                        null,
                        null,
                        file,
                        deletionVector);

        RoaringBitmap32 expected = RoaringBitmap32.bitmapOfRange(0, rowCount - 1);
        expected.remove(0);
        expected.remove(2);
        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get()).isEqualTo(expected);
    }

    @Test
    void testFilterAbandonsBitmapPushdownForLargeFile() throws Exception {
        TableSchema schema = tableSchema();
        FileIndexWriter indexWriter =
                new BitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
        indexWriter.writeRecord(0);
        DataFileMeta file =
                fileWithRowCount(Integer.MAX_VALUE + 2L)
                        .copy(embeddedIndex(BitmapFileIndexFactory.BITMAP_INDEX, indexWriter));
        DeletionVector deletionVector = new Bitmap64DeletionVector();
        deletionVector.delete(Integer.MAX_VALUE + 1L);
        Predicate filter = new PredicateBuilder(schema.logicalRowType()).equal(0, 0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        schema,
                        Collections.singletonList(filter),
                        null,
                        null,
                        null,
                        file,
                        deletionVector);

        assertThat(result).isSameAs(FileIndexResult.REMAIN);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFilterExcludesDeletedPositions(boolean bitmap64) throws Exception {
        TableSchema schema = tableSchema();
        FileIndexWriter indexWriter =
                new BitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
        for (int position = 0; position < 10; position++) {
            indexWriter.writeRecord(position % 2);
        }
        DataFileMeta file =
                DataFileTestUtils.newFile("data.avro", 0, 0, 9, 0L)
                        .copy(embeddedIndex(BitmapFileIndexFactory.BITMAP_INDEX, indexWriter));
        DeletionVector deletionVector =
                bitmap64
                        ? new Bitmap64DeletionVector() {
                            @Override
                            public void forEachDeletedPosition(LongConsumer consumer) {
                                throw new AssertionError(
                                        "Bitmap filter evaluation must not iterate the deletion vector.");
                            }

                            @Override
                            public RoaringBitmap32 projectToBitmap32(long maxExclusive) {
                                throw new AssertionError(
                                        "Bitmap filter evaluation must not project the deletion vector.");
                            }
                        }
                        : new BitmapDeletionVector();
        deletionVector.delete(0);
        deletionVector.delete(2);
        Predicate filter = new PredicateBuilder(schema.logicalRowType()).equal(0, 0);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        schema,
                        Collections.singletonList(filter),
                        null,
                        null,
                        null,
                        file,
                        deletionVector);

        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get()).isEqualTo(RoaringBitmap32.bitmapOf(4, 6, 8));
    }

    @Test
    void testTopNDoesNotIterateBitmap64DeletionVector() throws Exception {
        TableSchema schema = tableSchema();
        FileIndexWriter indexWriter =
                new RangeBitmapFileIndex(DataTypes.INT(), new Options()).createWriter();
        for (int value = 0; value < 5; value++) {
            indexWriter.writeRecord(value);
        }
        DataFileMeta file =
                fileWithRowCount(5)
                        .copy(embeddedIndex(RangeBitmapFileIndexFactory.RANGE_BITMAP, indexWriter));
        DeletionVector deletionVector =
                new Bitmap64DeletionVector() {
                    @Override
                    public void forEachDeletedPosition(LongConsumer consumer) {
                        throw new AssertionError(
                                "TopN evaluation must not iterate the deletion vector.");
                    }
                };
        deletionVector.delete(0);
        deletionVector.delete(4);
        FieldRef field = new FieldRef(0, FIELD_NAME, DataTypes.INT());
        TopN topN = new TopN(field, ASCENDING, NULLS_LAST, 2);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        schema,
                        Collections.emptyList(),
                        topN,
                        null,
                        null,
                        file,
                        deletionVector);

        assertThat(result).isInstanceOf(BitmapIndexResult.class);
        assertThat(((BitmapIndexResult) result).get()).isEqualTo(RoaringBitmap32.bitmapOf(1, 2));
    }

    @Test
    void testBloomRemainDoesNotIterateBitmap64DeletionVector() throws Exception {
        FileIndexWriter indexWriter =
                new BloomFilterFileIndex(DataTypes.INT(), new Options()).createWriter();
        indexWriter.writeRecord(1);

        FileIndexResult result = evaluateBloomFilter(indexWriter, 1);

        assertThat(result).isSameAs(FileIndexResult.REMAIN);
    }

    @Test
    void testBloomSkipDoesNotIterateBitmap64DeletionVector() throws Exception {
        FileIndexWriter indexWriter =
                new BloomFilterFileIndex(DataTypes.INT(), new Options()).createWriter();
        indexWriter.writeRecord(null);

        FileIndexResult result = evaluateBloomFilter(indexWriter, 1);

        assertThat(result).isSameAs(FileIndexResult.SKIP);
    }

    @Test
    void testBloomSkipForLargeFile() throws Exception {
        TableSchema schema = tableSchema();
        FileIndexWriter indexWriter =
                new BloomFilterFileIndex(DataTypes.INT(), new Options()).createWriter();
        indexWriter.writeRecord(null);
        DataFileMeta file =
                fileWithRowCount(Integer.MAX_VALUE + 2L)
                        .copy(embeddedIndex(BloomFilterFileIndexFactory.BLOOM_FILTER, indexWriter));
        Predicate filter = new PredicateBuilder(schema.logicalRowType()).equal(0, 1);

        FileIndexResult result =
                FileIndexEvaluator.evaluate(
                        null,
                        schema,
                        Collections.singletonList(filter),
                        null,
                        null,
                        null,
                        file,
                        null);

        assertThat(result).isSameAs(FileIndexResult.SKIP);
    }

    private static FileIndexResult evaluateBloomFilter(FileIndexWriter indexWriter, int value)
            throws Exception {
        TableSchema schema = tableSchema();
        DataFileMeta file =
                DataFileTestUtils.newFile("data.avro", 0, 0, 0, 0L)
                        .copy(embeddedIndex(BloomFilterFileIndexFactory.BLOOM_FILTER, indexWriter));
        DeletionVector deletionVector =
                new Bitmap64DeletionVector() {
                    @Override
                    public void forEachDeletedPosition(LongConsumer consumer) {
                        throw new AssertionError(
                                "Bloom filter evaluation must not expand the deletion vector.");
                    }

                    @Override
                    public RoaringBitmap32 projectToBitmap32(long maxExclusive) {
                        throw new AssertionError(
                                "Bloom filter evaluation must not project the deletion vector.");
                    }
                };
        deletionVector.delete(0);
        Predicate filter = new PredicateBuilder(schema.logicalRowType()).equal(0, value);

        return FileIndexEvaluator.evaluate(
                null,
                schema,
                Collections.singletonList(filter),
                null,
                null,
                null,
                file,
                deletionVector);
    }

    private static TableSchema tableSchema() {
        DataField field = new DataField(0, FIELD_NAME, DataTypes.INT());
        return new TableSchema(
                0,
                Collections.singletonList(field),
                field.id(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                null);
    }

    private static DataFileMeta fileWithRowCount(long rowCount) {
        return DataFileMeta.forAppend(
                "data.avro",
                0,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private static byte[] embeddedIndex(String indexType, FileIndexWriter indexWriter)
            throws Exception {
        Map<String, byte[]> indexes =
                Collections.singletonMap(indexType, indexWriter.serializedBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(out)) {
            writer.writeColumnIndexes(Collections.singletonMap(FIELD_NAME, indexes));
        }
        return out.toByteArray();
    }
}
