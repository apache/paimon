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
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Validate the {@link ColumnVector}s read by Parquet format. */
public class ParquetColumnVectorTest {

    private @TempDir java.nio.file.Path tempDir;

    private static final Random RND = ThreadLocalRandom.current();
    private static final BiFunction<ColumnVector, Integer, String> BYTES_COLUMN_VECTOR_STRING_FUNC =
            (cv, i) ->
                    cv.isNullAt(i)
                            ? "null"
                            : new String(((BytesColumnVector) cv).getBytes(i).getBytes());

    @Test
    public void testArrayString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("array_string", DataTypes.ARRAY(DataTypes.STRING()))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        ArrayObject expectedData = new ArrayObject();
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                expectedData.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            expectedData.add(currentStringArray);
            GenericArray array =
                    new GenericArray(
                            currentStringArray.stream().map(BinaryString::fromString).toArray());
            rows.add(GenericRow.of(array));
        }

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            expectedData.validateRow(row, i, getter);
        }
        assertThat(iterator.next()).isNull();

        // validate ColumnVector
        ArrayColumnVector arrayColumnVector = (ArrayColumnVector) batch.columns[0];
        expectedData.validateColumnVector(arrayColumnVector, getter);

        expectedData.validateInnerChild(
                arrayColumnVector.getColumnVector(), BYTES_COLUMN_VECTOR_STRING_FUNC);

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
        ArrayArrayObject expectedData = new ArrayArrayObject();
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            // outer null row
            if (RND.nextBoolean()) {
                expectedData.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int arraySize = RND.nextInt(5);
            ArrayObject arrayObject = new ArrayObject();
            GenericArray[] innerArrays = new GenericArray[arraySize];
            for (int aIdx = 0; aIdx < arraySize; aIdx++) {
                // inner null array
                if (RND.nextBoolean()) {
                    arrayObject.add(null);
                    innerArrays[aIdx] = null;
                    continue;
                }

                int arrayStringSize = RND.nextInt(5);
                List<String> currentStringArray =
                        IntStream.range(0, arrayStringSize)
                                .mapToObj(idx -> randomString())
                                .collect(Collectors.toList());
                arrayObject.add(currentStringArray);
                innerArrays[aIdx] =
                        new GenericArray(
                                currentStringArray.stream()
                                        .map(BinaryString::fromString)
                                        .toArray());
            }
            expectedData.add(arrayObject);
            rows.add(GenericRow.of(new GenericArray(innerArrays)));
        }

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            expectedData.validateRow(row, i, getter);
        }
        assertThat(iterator.next()).isNull();

        // validate column vector
        ArrayColumnVector arrayColumnVector = (ArrayColumnVector) batch.columns[0];

        expectedData.validateOuterArray(arrayColumnVector, getter);

        ArrayColumnVector innerArrayColumnVector =
                (ArrayColumnVector) arrayColumnVector.getColumnVector();
        expectedData.validateInnerArray(innerArrayColumnVector, getter);

        ColumnVector columnVector = innerArrayColumnVector.getColumnVector();
        expectedData.validateInnerChild(columnVector, BYTES_COLUMN_VECTOR_STRING_FUNC);
    }

    @Test
    public void testMapString() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("map_string", DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                        .build();

        int numRows = RND.nextInt(5) + 5;
        ArrayObject expectedData = new ArrayObject();
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                expectedData.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            expectedData.add(currentStringArray);
            Map<Integer, BinaryString> map = new HashMap<>();
            for (int idx = 0; idx < currentSize; idx++) {
                map.put(idx, BinaryString.fromString(currentStringArray.get(idx)));
            }
            rows.add(GenericRow.of(new GenericMap(map)));
        }

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            List<?> expected = expectedData.data.get(i);
            if (expected == null) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalMap map = row.getMap(0);
                validateMapKeyArray(map.keyArray());
                InternalArray valueArray = map.valueArray();
                expectedData.validateNonNullArray(expected, valueArray, getter);
            }
        }
        assertThat(iterator.next()).isNull();

        // validate ColumnVector
        MapColumnVector mapColumnVector = (MapColumnVector) batch.columns[0];
        IntColumnVector keyColumnVector = (IntColumnVector) mapColumnVector.getKeyColumnVector();
        validateMapKeyColumnVector(keyColumnVector, expectedData);
        ColumnVector valueColumnVector = mapColumnVector.getValueColumnVector();
        expectedData.validateInnerChild(valueColumnVector, BYTES_COLUMN_VECTOR_STRING_FUNC);

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
        ArrayArrayObject expectedData = new ArrayArrayObject();
        List<InternalRow> rows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            // outer null row
            if (RND.nextBoolean()) {
                expectedData.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            int mapSize = RND.nextInt(5);
            ArrayObject arrayObject = new ArrayObject();
            Map<Integer, GenericArray> map = new HashMap<>();
            for (int mIdx = 0; mIdx < mapSize; mIdx++) {
                // null array value
                if (RND.nextBoolean()) {
                    arrayObject.add(null);
                    map.put(mIdx, null);
                    continue;
                }

                int currentSize = RND.nextInt(5);
                List<String> currentStringArray =
                        IntStream.range(0, currentSize)
                                .mapToObj(idx -> randomString())
                                .collect(Collectors.toList());
                arrayObject.add(currentStringArray);

                map.put(
                        mIdx,
                        new GenericArray(
                                currentStringArray.stream()
                                        .map(BinaryString::fromString)
                                        .toArray()));
            }
            expectedData.add(arrayObject);
            rows.add(GenericRow.of(new GenericMap(map)));
        }

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            ArrayObject expected = expectedData.data.get(i);
            if (expected == null) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalMap map = row.getMap(0);
                validateMapKeyArray(map.keyArray());
                InternalArray valueArray = map.valueArray();
                expected.validateArrayGetter(valueArray, getter);
            }
        }
        assertThat(iterator.next()).isNull();

        // validate column vector
        MapColumnVector mapColumnVector = (MapColumnVector) batch.columns[0];
        IntColumnVector keyColumnVector = (IntColumnVector) mapColumnVector.getKeyColumnVector();
        validateMapKeyColumnVector(keyColumnVector, expectedData);

        ArrayColumnVector valueColumnVector =
                (ArrayColumnVector) mapColumnVector.getValueColumnVector();
        expectedData.validateInnerArray(valueColumnVector, getter);
        expectedData.validateInnerChild(
                valueColumnVector.getColumnVector(), BYTES_COLUMN_VECTOR_STRING_FUNC);

        iterator.releaseBatch();
    }

    private void validateMapKeyArray(InternalArray keyArray) {
        for (int i = 0; i < keyArray.size(); i++) {
            assertThat(keyArray.getInt(i)).isEqualTo(i);
        }
    }

    private void validateMapKeyColumnVector(
            IntColumnVector columnVector, ArrayObject expectedData) {
        int idx = 0;
        for (List<?> values : expectedData.data) {
            if (values != null) {
                for (int i = 0; i < values.size(); i++) {
                    assertThat(columnVector.getInt(idx++)).isEqualTo(i);
                }
            }
        }
    }

    private void validateMapKeyColumnVector(
            IntColumnVector columnVector, ArrayArrayObject expectedData) {
        int idx = 0;
        for (ArrayObject arrayObject : expectedData.data) {
            if (arrayObject != null) {
                for (int i = 0; i < arrayObject.data.size(); i++) {
                    assertThat(columnVector.getInt(idx++)).isEqualTo(i);
                }
            }
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
        ArrayObject expectedData = new ArrayObject();
        List<InternalRow> rows = new ArrayList<>(numRows);
        List<Integer> f0 = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            if (RND.nextBoolean()) {
                expectedData.add(null);
                f0.add(null);
                rows.add(GenericRow.of((Object) null));
                continue;
            }

            if (RND.nextInt(5) == 0) {
                // set f1 null
                expectedData.add(null);
                f0.add(i);
                rows.add(GenericRow.of(GenericRow.of(i, null)));
                continue;
            }

            int currentSize = RND.nextInt(5);
            List<String> currentStringArray =
                    IntStream.range(0, currentSize)
                            .mapToObj(idx -> randomString())
                            .collect(Collectors.toList());
            expectedData.add(currentStringArray);
            f0.add(i);
            GenericArray array =
                    new GenericArray(
                            currentStringArray.stream().map(BinaryString::fromString).toArray());
            rows.add(GenericRow.of(GenericRow.of(i, array)));
        }

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());

        // validate row by row
        for (int i = 0; i < numRows; i++) {
            InternalRow row = iterator.next();
            assertThat(row).isNotNull();
            if (f0.get(i) == null && expectedData.data.get(i) == null) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalRow innerRow = row.getRow(0, 2);

                if (f0.get(i) == null) {
                    assertThat(innerRow.isNullAt(0)).isTrue();
                } else {
                    assertThat(innerRow.getInt(0)).isEqualTo(f0.get(i));
                }

                if (expectedData.data.get(i) == null) {
                    assertThat(innerRow.isNullAt(1)).isTrue();
                } else {
                    expectedData.validateNonNullArray(
                            expectedData.data.get(i), innerRow.getArray(1), getter);
                }
            }
        }
        assertThat(iterator.next()).isNull();

        // validate ColumnVector
        RowColumnVector rowColumnVector = (RowColumnVector) batch.columns[0];
        VectorizedColumnBatch innerBatch = rowColumnVector.getBatch();

        IntColumnVector intColumnVector = (IntColumnVector) innerBatch.columns[0];
        for (int i = 0; i < numRows; i++) {
            Integer f0Value = f0.get(i);
            if (f0Value == null) {
                assertThat(intColumnVector.isNullAt(i)).isTrue();
            } else {
                assertThat(intColumnVector.getInt(i)).isEqualTo(f0Value);
            }
        }

        ArrayColumnVector arrayColumnVector = (ArrayColumnVector) innerBatch.columns[1];
        expectedData.validateColumnVector(arrayColumnVector, getter);
        expectedData.validateInnerChild(
                arrayColumnVector.getColumnVector(), BYTES_COLUMN_VECTOR_STRING_FUNC);

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
            f0.add(BinaryString.fromString(randomString()));
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

        VectorizedRecordIterator iterator = createVectorizedRecordIterator(rowType, rows);
        VectorizedColumnBatch batch = iterator.batch();

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

        // validate ColumnVector
        ArrayColumnVector arrayColumnVector = (ArrayColumnVector) batch.columns[0];
        assertThat(arrayColumnVector.isNullAt(0)).isFalse();
        assertThat(arrayColumnVector.isNullAt(1)).isTrue();
        assertThat(arrayColumnVector.isNullAt(2)).isFalse();
        assertThat(arrayColumnVector.isNullAt(3)).isFalse();

        RowColumnVector rowColumnVector = (RowColumnVector) arrayColumnVector.getColumnVector();
        BytesColumnVector f0Vector = (BytesColumnVector) rowColumnVector.getBatch().columns[0];
        for (int i = 0; i < 3; i++) {
            BinaryString s = f0.get(i);
            if (s == null) {
                assertThat(f0Vector.isNullAt(i)).isTrue();
            } else {
                assertThat(new String(f0Vector.getBytes(i).getBytes())).isEqualTo(s.toString());
            }
        }
        ArrayColumnVector f1Vector = (ArrayColumnVector) rowColumnVector.getBatch().columns[1];
        InternalArray internalArray0 = f1Vector.getArray(0);
        assertThat(internalArray0.size()).isEqualTo(2);
        assertThat(internalArray0.isNullAt(0)).isFalse();
        assertThat(internalArray0.isNullAt(1)).isTrue();

        InternalArray internalArray1 = f1Vector.getArray(1);
        assertThat(internalArray1.size()).isEqualTo(0);

        InternalArray internalArray2 = f1Vector.getArray(2);
        assertThat(internalArray2.size()).isEqualTo(1);
        assertThat(internalArray2.isNullAt(0)).isFalse();

        IntColumnVector intColumnVector = (IntColumnVector) f1Vector.getColumnVector();
        assertThat(intColumnVector.getInt(0)).isEqualTo(0);
        assertThat(intColumnVector.isNullAt(1)).isTrue();
        assertThat(intColumnVector.getInt(2)).isEqualTo(1);

        iterator.releaseBatch();
    }

    private VectorizedRecordIterator createVectorizedRecordIterator(
            RowType rowType, List<InternalRow> rows) throws IOException {
        Path path = new Path(tempDir.toString(), UUID.randomUUID().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        ParquetWriterFactory writerFactory =
                new ParquetWriterFactory(new RowDataParquetBuilder(rowType, new Options()));
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.flush();
        writer.finish();

        ParquetReaderFactory readerFactory =
                new ParquetReaderFactory(new Options(), rowType, 1024, FilterCompat.NOOP);

        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        return (VectorizedRecordIterator) iterator;
    }

    @Nullable
    private String randomString() {
        return RND.nextInt(5) == 0 ? null : StringUtils.getRandomString(RND, 1, 10);
    }

    /** Store generated data of ARRAY[STRING] and provide validated methods. */
    private static class ArrayObject {

        public final List<List<?>> data;

        public ArrayObject() {
            this.data = new ArrayList<>();
        }

        public void add(List<?> objects) {
            data.add(objects);
        }

        public void validateRow(InternalRow row, int i, InternalArray.ElementGetter getter) {
            assertThat(row).isNotNull();
            List<?> expected = data.get(i);
            if (expected == null) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                validateNonNullArray(expected, row.getArray(0), getter);
            }
        }

        public void validateColumnVector(
                ArrayColumnVector arrayColumnVector, InternalArray.ElementGetter getter) {
            for (int i = 0; i < data.size(); i++) {
                List<?> expected = data.get(i);
                if (expected == null) {
                    assertThat(arrayColumnVector.isNullAt(i)).isTrue();
                } else {
                    validateNonNullArray(expected, arrayColumnVector.getArray(i), getter);
                }
            }
        }

        public void validateArrayGetter(DataGetters arrays, InternalArray.ElementGetter getter) {
            for (int i = 0; i < data.size(); i++) {
                List<?> expected = data.get(i);
                if (expected == null) {
                    assertThat(arrays.isNullAt(i)).isTrue();
                } else {
                    validateNonNullArray(expected, arrays.getArray(i), getter);
                }
            }
        }

        public void validateNonNullArray(
                List<?> expected, InternalArray array, InternalArray.ElementGetter getter) {
            int arraySize = array.size();
            assertThat(arraySize).isEqualTo(expected.size());
            for (int i = 0; i < arraySize; i++) {
                String value = String.valueOf(getter.getElementOrNull(array, i));
                assertThat(value).isEqualTo(String.valueOf(expected.get(i)));
            }
        }

        public void validateInnerChild(
                ColumnVector columnVector, BiFunction<ColumnVector, Integer, String> stringGetter) {
            // it doesn't contain null rows
            List<?> expandedData =
                    data.stream()
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            for (int i = 0; i < expandedData.size(); i++) {
                assertThat(stringGetter.apply(columnVector, i))
                        .isEqualTo(String.valueOf(expandedData.get(i)));
            }
        }
    }

    /** Store generated data of ARRAY[ARRAY[STRING]] and provide validated methods. */
    private static class ArrayArrayObject {

        public final List<ArrayObject> data;

        public ArrayArrayObject() {
            this.data = new ArrayList<>();
        }

        public void add(@Nullable ArrayObject arrayObjects) {
            data.add(arrayObjects);
        }

        private List<List<?>> expand() {
            // it doesn't contain null rows of outer array
            return data.stream()
                    .filter(Objects::nonNull)
                    .flatMap(i -> i.data.stream())
                    .collect(Collectors.toList());
        }

        private List<?> expandInner() {
            // it doesn't contain null rows of outer and inner array
            return expand().stream()
                    .filter(Objects::nonNull)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }

        public void validateRow(InternalRow row, int i, InternalArray.ElementGetter getter) {
            assertThat(row).isNotNull();
            ArrayObject expectedArray = data.get(i);
            if (expectedArray == null) {
                assertThat(row.isNullAt(0)).isTrue();
            } else {
                InternalArray outerArray = row.getArray(0);
                assertThat(outerArray.size()).isEqualTo(expectedArray.data.size());
                expectedArray.validateArrayGetter(outerArray, getter);
            }
        }

        public void validateOuterArray(
                ArrayColumnVector arrayColumnVector,
                InternalArray.ElementGetter innerElementGetter) {
            for (int i = 0; i < data.size(); i++) {
                ArrayObject expected = data.get(i);
                if (expected == null) {
                    assertThat(arrayColumnVector.isNullAt(i)).isTrue();
                } else {
                    InternalArray array = arrayColumnVector.getArray(i);
                    expected.validateArrayGetter(array, innerElementGetter);
                }
            }
        }

        public void validateInnerArray(
                ArrayColumnVector arrayColumnVector,
                InternalArray.ElementGetter innerElementGetter) {
            List<List<?>> expandedData = expand();
            for (int i = 0; i < expandedData.size(); i++) {
                List<?> expected = expandedData.get(i);
                if (expected == null) {
                    assertThat(arrayColumnVector.isNullAt(i)).isTrue();
                } else {
                    InternalArray array = arrayColumnVector.getArray(i);
                    int size = array.size();
                    assertThat(size).isEqualTo(expected.size());
                    for (int j = 0; j < size; j++) {
                        assertThat(String.valueOf(innerElementGetter.getElementOrNull(array, j)))
                                .isEqualTo(String.valueOf(expected.get(j)));
                    }
                }
            }
        }

        public void validateInnerChild(
                ColumnVector columnVector, BiFunction<ColumnVector, Integer, String> stringGetter) {
            List<?> expandedData = expandInner();
            for (int i = 0; i < expandedData.size(); i++) {
                assertThat(stringGetter.apply(columnVector, i))
                        .isEqualTo(String.valueOf(expandedData.get(i)));
            }
        }
    }
}
