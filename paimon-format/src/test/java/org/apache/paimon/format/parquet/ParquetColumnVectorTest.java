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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Validate the {@link ColumnVector}s read by Parquet format. */
public class ParquetColumnVectorTest {

    private @TempDir java.nio.file.Path tempDir;

    private static final Random RND = ThreadLocalRandom.current();

    @Test
    public void testNormalStrings() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("s1", DataTypes.STRING())
                        .field("s2", DataTypes.STRING())
                        .field("s3", DataTypes.STRING())
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            rows.add(GenericRow.of(fromString(i + ""), fromString(i + ""), fromString(i + "")));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assert row != null;
            assertThat(row.getString(0)).isEqualTo(rows.get(i).getString(0));
            assertThat(row.getString(1)).isEqualTo(rows.get(i).getString(1));
            assertThat(row.getString(2)).isEqualTo(rows.get(i).getString(2));
        }
    }

    @Test
    public void testArrayString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("array_string", DataTypes.ARRAY(DataTypes.STRING()))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            GenericArray array =
                    new GenericArray(
                            currentStringArray.stream().map(BinaryString::fromString).toArray());
            rows.add(GenericRow.of(array));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assert row != null;
            InternalRow expectedRow = rows.get(i);
            if (expectedRow.isNullAt(0)) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalArray array = row.getArray(0);
                InternalArray expectedArray = expectedRow.getArray(0);
                testArrayStringEqual(array, expectedArray);
            }
        }
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
    }

    @Test
    public void testArrayArrayString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field(
                                "array_array_string",
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            // outer null row
            if (RND.nextBoolean()) {
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int arraySize = RND.nextInt(5);
            GenericArray[] innerArrays = new GenericArray[arraySize];
            for (int aIdx = 0; aIdx < arraySize; aIdx++) {
                // inner null array
                if (RND.nextBoolean()) {
                    innerArrays[aIdx] = null;
                    continue;
                }

                int arrayStringSize = RND.nextInt(5);
                List<String> currentStringArray =
                        IntStream.range(0, arrayStringSize)
                                .mapToObj(idx -> randomString())
                                .collect(Collectors.toList());
                innerArrays[aIdx] =
                        new GenericArray(
                                currentStringArray.stream()
                                        .map(BinaryString::fromString)
                                        .toArray());
            }
            rows.add(GenericRow.of(new GenericArray(innerArrays)));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            InternalRow expected = rows.get(i);
            if (expected.isNullAt(0)) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalArray array = row.getArray(0);
                InternalArray expectedArray = expected.getArray(0);
                testArrayArrayStringEqual(array, expectedArray);
            }
        }
        assertThat(iterator.next()).isNull();
    }

    @Test
    public void testMapString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("map_string", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            Map<Integer, BinaryString> map = new HashMap<>();
            for (int idx = 0; idx < currentSize; idx++) {
                map.put(idx, fromString(currentStringArray.get(idx)));
            }
            rows.add(GenericRow.of(new GenericMap(map)));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            InternalRow expectedRow = rows.get(i);
            if (expectedRow.isNullAt(0)) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalMap map = row.getMap(0);
                InternalMap expectedMap = expectedRow.getMap(0);
                assertThat(map.size()).isEqualTo(expectedMap.size());

                InternalArray keyArray = map.keyArray();
                InternalArray valueArray = map.valueArray();
                InternalArray expectedKeyArray = expectedMap.keyArray();
                InternalArray expectedValueArray = expectedMap.valueArray();

                testArrayIntEqual(keyArray, expectedKeyArray);
                testArrayStringEqual(valueArray, expectedValueArray);
            }
        }
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
    }

    @Test
    public void testMapArrayString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field(
                                "map_array_string",
                                DataTypes.MAP(DataTypes.INT(), DataTypes.ARRAY(DataTypes.STRING())))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            // outer null row
            if (RND.nextBoolean() || i == 0) {
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int mapSize = RND.nextInt(5);
            Map<Integer, GenericArray> map = new HashMap<>();
            for (int mIdx = 0; mIdx < mapSize; mIdx++) {
                // null array value
                if (RND.nextBoolean()) {
                    map.put(mIdx, null);
                    continue;
                }

                int currentSize = RND.nextInt(5);
                List<String> currentStringArray =
                        IntStream.range(0, currentSize)
                                .mapToObj(idx -> randomString())
                                .collect(Collectors.toList());

                map.put(
                        mIdx,
                        new GenericArray(
                                currentStringArray.stream()
                                        .map(BinaryString::fromString)
                                        .toArray()));
            }
            rows.add(GenericRow.of(new GenericMap(map)));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            InternalRow expected = rows.get(i);
            if (expected.isNullAt(0)) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalMap map = row.getMap(0);
                InternalMap expectedMap = expected.getMap(0);
                assertThat(map.keyArray().toIntArray())
                        .containsExactly(expectedMap.keyArray().toIntArray());
                InternalArray valueArray = map.valueArray();
                InternalArray expectedValueArray = expectedMap.valueArray();
                testArrayArrayStringEqual(valueArray, expectedValueArray);
            }
        }
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
    }

    private void testArrayArrayStringEqual(
            InternalArray valueArray, InternalArray expectedValueArray) {
        assertThat(valueArray.size()).isEqualTo(expectedValueArray.size());
        for (int j = 0; j < expectedValueArray.size(); j++) {
            if (expectedValueArray.isNullAt(j)) {
                assertThat(valueArray.isNullAt(j)).isTrue();
            } else {
                InternalArray valueString = valueArray.getArray(j);
                InternalArray expectedValueString = expectedValueArray.getArray(j);
                testArrayStringEqual(valueString, expectedValueString);
            }
        }
    }

    private void testArrayStringEqual(InternalArray valueArray, InternalArray expectedValueArray) {
        assertThat(valueArray.size()).isEqualTo(expectedValueArray.size());
        for (int j = 0; j < expectedValueArray.size(); j++) {
            if (expectedValueArray.isNullAt(j)) {
                assertThat(valueArray.isNullAt(j)).isTrue();
            } else {
                assertThat(valueArray.getString(j)).isEqualTo(expectedValueArray.getString(j));
            }
        }
    }

    private void testArrayIntEqual(InternalArray valueArray, InternalArray expectedValueArray) {
        assertThat(valueArray.size()).isEqualTo(expectedValueArray.size());
        for (int j = 0; j < expectedValueArray.size(); j++) {
            if (expectedValueArray.isNullAt(j)) {
                assertThat(valueArray.isNullAt(j)).isTrue();
            } else {
                assertThat(valueArray.getInt(j)).isEqualTo(expectedValueArray.getInt(j));
            }
        }
    }

    private void validateMapKeyArray(InternalArray keyArray) {
        for (int i = 0; i < keyArray.size(); i++) {
            assertThat(keyArray.getInt(i)).isEqualTo(i);
        }
    }

    @Test
    public void testRow() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field(
                                "row",
                                RowType.builder()
                                        .field("f0", DataTypes.INT())
                                        .field("f1", DataTypes.ARRAY(DataTypes.STRING()))
                                        .build())
                        .build();

        int numRows = RND.nextInt(5) + 5;
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            if (RND.nextInt(5) == 0) {
                // set f1 null
                rows.add(GenericRow.of(GenericRow.of(i, null)));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            GenericArray array =
                    new GenericArray(
                            currentStringArray.stream().map(BinaryString::fromString).toArray());
            rows.add(GenericRow.of(GenericRow.of(i, array)));
        }

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            InternalRow expectedRow = rows.get(i);

            assertThat(row).isNotNull();
            if (expectedRow.isNullAt(0)) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalRow innerRow = row.getRow(0, 2);
                InternalRow expectedInnerRow = expectedRow.getRow(0, 2);

                if (expectedInnerRow.isNullAt(0)) {
                    assertThat(innerRow.isNullAt(0)).isTrue();
                } else {
                    assertThat(innerRow.getInt(0)).isEqualTo(i);
                }

                if (expectedInnerRow.isNullAt(1)) {
                    assertThat(innerRow.isNullAt(1)).isTrue();
                } else {
                    InternalArray valueArray = innerRow.getArray(1);
                    InternalArray expectedValueArray = expectedInnerRow.getArray(1);
                    testArrayStringEqual(valueArray, expectedValueArray);
                }
            }
        }
        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
    }

    @Test
    public void testArrayRowArray() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field(
                                "array_row_array",
                                DataTypes.ARRAY(
                                        RowType.builder()
                                                .field("f0", DataTypes.STRING())
                                                .field("f1", DataTypes.ARRAY(DataTypes.INT()))
                                                .build()))
                        .build();

        List<InternalRow> rows = new ArrayList<>(4);
        List<BinaryString> f0 = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            f0.add(fromString(randomString()));
        }

        GenericRow row00 = GenericRow.of(f0.get(0), new GenericArray(new Object[] {0, null}));
        GenericRow row01 = GenericRow.of(f0.get(1), new GenericArray(new Object[] {}));
        GenericArray array0 = new GenericArray(new GenericRow[] {row00, row01});
        rows.add(GenericRow.of(array0));

        rows.add(GenericRow.of((Object) null));

        GenericRow row20 = GenericRow.of(f0.get(2), new GenericArray(new Object[] {1}));
        GenericArray array2 = new GenericArray(new GenericRow[] {row20});
        rows.add(GenericRow.of(array2));

        GenericArray array3 = new GenericArray(new GenericRow[] {});
        rows.add(GenericRow.of(array3));

        ColumnarRowIterator iterator = createRecordIterator(rowType, rows);

        // validate row by row
        InternalRow row0 = iterator.next();
        // array0
        InternalArray array = row0.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        // row00
        InternalRow row = array.getRow(0, 1);
        if (f0.get(0) == null) {
            assertThat(row.isNullAt(0)).isTrue();
        } else {
            assertThat(row.getString(0)).isEqualTo(f0.get(0));
        }
        InternalArray innerArray = row.getArray(1);
        assertThat(innerArray.size()).isEqualTo(2);
        assertThat(innerArray.getInt(0)).isEqualTo(0);
        assertThat(innerArray.isNullAt(1)).isTrue();
        // row01
        row = array.getRow(1, 1);
        if (f0.get(1) == null) {
            assertThat(row.isNullAt(0)).isTrue();
        } else {
            assertThat(row.getString(0)).isEqualTo(f0.get(1));
        }
        innerArray = row.getArray(1);
        assertThat(innerArray.size()).isEqualTo(0);

        InternalRow row1 = iterator.next();
        assertThat(row1.isNullAt(0)).isTrue();

        InternalRow row2 = iterator.next();
        // array2
        array = row2.getArray(0);
        assertThat(array.size()).isEqualTo(1);
        // row20
        row = array.getRow(0, 1);
        if (f0.get(2) == null) {
            assertThat(row.isNullAt(0)).isTrue();
        } else {
            assertThat(row.getString(0)).isEqualTo(f0.get(2));
        }
        innerArray = row.getArray(1);
        assertThat(innerArray.size()).isEqualTo(1);
        assertThat(innerArray.getInt(0)).isEqualTo(1);

        InternalRow row3 = iterator.next();
        // array2
        array = row3.getArray(0);
        assertThat(array.size()).isEqualTo(0);

        assertThat(iterator.next()).isNull();

        iterator.releaseBatch();
    }

    @Test
    public void testHighlyNestedSchema() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field(
                                "row",
                                RowType.builder()
                                        .field("f0", DataTypes.ARRAY(RowType.of(DataTypes.INT())))
                                        .field("f1", RowType.of(DataTypes.INT()))
                                        .build())
                        .build();

        InternalRow row0 = GenericRow.of((Object) null);
        InternalRow row1 = GenericRow.of(GenericRow.of(null, GenericRow.of(1)));
        InternalRow row2 =
                GenericRow.of(
                        GenericRow.of(
                                new GenericArray(
                                        new GenericRow[] {
                                            GenericRow.of((Object) null), GenericRow.of(22)
                                        }),
                                GenericRow.of((Object) null)));
        InternalRow row3 =
                GenericRow.of(GenericRow.of(new GenericArray(new GenericRow[] {null}), null));

        ColumnarRowIterator iterator =
                createRecordIterator(rowType, Arrays.asList(row0, row1, row2, row3));

        // validate per row
        InternalRow internalRow0 = iterator.next();
        assertThat(internalRow0.isNullAt(0)).isTrue();

        InternalRow internalRow1 = iterator.next();
        assertThat(internalRow1.isNullAt(0)).isFalse();
        InternalRow internalRow1InternalRow = internalRow1.getRow(0, 2);
        assertThat(internalRow1InternalRow.isNullAt(0)).isTrue();
        InternalRow internalRow1InternalRowF1 = internalRow1InternalRow.getRow(1, 1);
        assertThat(internalRow1InternalRowF1.getInt(0)).isEqualTo(1);

        InternalRow internalRow2 = iterator.next();
        assertThat(internalRow2.isNullAt(0)).isFalse();
        InternalRow internalRow2InternalRow = internalRow2.getRow(0, 2);
        InternalArray internalRow2InternalRowF0 = internalRow2InternalRow.getArray(0);
        assertThat(internalRow2InternalRowF0.size()).isEqualTo(2);
        InternalRow i0 = internalRow2InternalRowF0.getRow(0, 1);
        assertThat(i0.isNullAt(0)).isTrue();
        InternalRow i1 = internalRow2InternalRowF0.getRow(1, 1);
        assertThat(i1.getInt(0)).isEqualTo(22);
        InternalRow internalRow2InternalRowF1 = internalRow2InternalRow.getRow(1, 1);
        assertThat(internalRow2InternalRowF1.isNullAt(0)).isTrue();

        InternalRow internalRow3 = iterator.next();
        assertThat(internalRow3.isNullAt(0)).isFalse();
        InternalRow internalRow3InternalRow = internalRow3.getRow(0, 2);
        InternalArray internalRow3InternalRowF0 = internalRow3InternalRow.getArray(0);
        assertThat(internalRow3InternalRowF0.size()).isEqualTo(1);
        assertThat(internalRow3InternalRowF0.isNullAt(0)).isTrue();
        assertThat(internalRow3InternalRow.isNullAt(1)).isTrue();

        assertThat(iterator.next()).isNull();
        iterator.releaseBatch();
    }

    private ColumnarRowIterator createRecordIterator(RowType rowType, List<InternalRow> rows)
            throws IOException {
        Path path = new Path(tempDir.toString(), UUID.randomUUID().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        ParquetWriterFactory writerFactory =
                new ParquetWriterFactory(new RowDataParquetBuilder(rowType, new Options()));
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();

        ParquetReaderFactory readerFactory =
                new ParquetReaderFactory(
                        new Options(), rowType, 1024, FilterCompat.NOOP, Collections.emptyList());

        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        return (ColumnarRowIterator) iterator;
    }

    @Nullable
    private String randomString() {
        return RND.nextInt(5) == 0 ? null : StringUtils.getRandomString(RND, 1, 10);
    }
}
