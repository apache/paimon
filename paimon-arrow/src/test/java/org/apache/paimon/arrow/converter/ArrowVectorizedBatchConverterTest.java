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

package org.apache.paimon.arrow.converter;

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.columnar.AllNullColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarVec;
import org.apache.paimon.data.columnar.RowToColumnConverter;
import org.apache.paimon.data.columnar.VecColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapVectorColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowVectorizedBatchConverter}. */
public class ArrowVectorizedBatchConverterTest {

    @Test
    public void testAllNullColumnVectorForComplexTypes() {
        RowType rowType =
                RowType.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ROW(
                                DataTypes.FIELD(0, "a", DataTypes.INT()),
                                DataTypes.FIELD(1, "b", DataTypes.STRING())),
                        DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.VECTOR(3, DataTypes.FLOAT()),
                        DataTypes.VARIANT());

        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator)) {
            ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);
            int rows = 2;
            int[] pickedInColumn = new int[] {3, 1, 4};

            for (ArrowFieldWriter fieldWriter : fieldWriters) {
                fieldWriter.reset();
                fieldWriter.write(AllNullColumnVector.INSTANCE, pickedInColumn, 1, rows);
            }
            vsr.setRowCount(rows);

            for (FieldVector fieldVector : vsr.getFieldVectors()) {
                assertThat(fieldVector.getValueCount()).isEqualTo(rows);
                assertThat(fieldVector.isNull(0)).isTrue();
                assertThat(fieldVector.isNull(1)).isTrue();
            }

            MapVector mapVector = (MapVector) vsr.getVector(0);
            assertThat(mapVector.getDataVector().getValueCount()).isZero();

            ListVector arrayVector = (ListVector) vsr.getVector(1);
            assertThat(arrayVector.getDataVector().getValueCount()).isZero();

            StructVector rowVector = (StructVector) vsr.getVector(2);
            assertThat(rowVector.getChildrenFromFields())
                    .allSatisfy(child -> assertThat(child.getValueCount()).isEqualTo(rows));

            ListVector nestedArrayVector = (ListVector) vsr.getVector(3);
            assertThat(nestedArrayVector.getDataVector().getValueCount()).isZero();

            FixedSizeListVector vector = (FixedSizeListVector) vsr.getVector(4);
            assertThat(vector.getDataVector().getValueCount()).isEqualTo(rows * 3);

            StructVector variantVector = (StructVector) vsr.getVector(5);
            assertThat(variantVector.getChildrenFromFields())
                    .allSatisfy(child -> assertThat(child.getValueCount()).isEqualTo(rows));

            arrayVector.validateFull();
            rowVector.validateFull();
            variantVector.validateFull();
        }
    }

    @Test
    public void testAllNullMapColumnVectorAfterNonNullBatch() {
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator)) {
            ArrowFieldWriter fieldWriter = ArrowUtils.createArrowFieldWriters(vsr, rowType)[0];

            HeapIntVector keys = new HeapIntVector(2);
            HeapIntVector values = new HeapIntVector(2);
            keys.setInt(0, 1);
            keys.setInt(1, 2);
            values.setInt(0, 10);
            values.setInt(1, 20);
            HeapMapVector nonNullVector = new HeapMapVector(2, keys, values);
            nonNullVector.putOffsetLength(0, 0, 1);
            nonNullVector.putOffsetLength(1, 1, 1);

            fieldWriter.reset();
            fieldWriter.write(nonNullVector, null, 0, 2);

            MapVector mapVector = (MapVector) vsr.getVector(0);
            assertThat(mapVector.isNull(0)).isFalse();
            assertThat(mapVector.isNull(1)).isFalse();
            assertThat(mapVector.getDataVector().getValueCount()).isEqualTo(2);
            mapVector.getDataVector().validateFull();

            fieldWriter.reset();
            fieldWriter.write(AllNullColumnVector.INSTANCE, null, 0, 3);

            assertThat(mapVector.getValueCount()).isEqualTo(3);
            assertThat(mapVector.isNull(0)).isTrue();
            assertThat(mapVector.isNull(1)).isTrue();
            assertThat(mapVector.isNull(2)).isTrue();
            assertThat(mapVector.getDataVector().getValueCount()).isZero();
            assertThat(mapVector.getDataVector().getChildrenFromFields())
                    .allSatisfy(child -> assertThat(child.getValueCount()).isZero());
            mapVector.getDataVector().validateFull();

            fieldWriter.reset();
            fieldWriter.write(nonNullVector, null, 0, 2);

            assertThat(mapVector.isNull(0)).isFalse();
            assertThat(mapVector.isNull(1)).isFalse();
            assertThat(mapVector.getDataVector().getValueCount()).isEqualTo(2);
            mapVector.getDataVector().validateFull();
        }
    }

    @Test
    public void testEmptyBatchDoesNotRequireTypedColumnVector() {
        RowType rowType = RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.INT()));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator)) {
            ArrowFieldWriter fieldWriter = ArrowUtils.createArrowFieldWriters(vsr, rowType)[0];

            fieldWriter.reset();
            fieldWriter.write(i -> true, new int[0], 0, 0);

            assertThat(vsr.getVector(0).getValueCount()).isZero();
        }
    }

    @Test
    public void testVectorColumnWrite() {
        RowType rowType = RowType.of(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);

            int length = 3;
            int rows = 2;
            HeapFloatVector elementVector = new HeapFloatVector(rows * length);
            float[] values = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
            for (int i = 0; i < values.length; i++) {
                elementVector.setFloat(i, values[i]);
            }
            VecColumnVector vector =
                    new TestVecColumnVectorWithNulls(elementVector, length, i -> i % 2 == 1);

            VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
            batch.setNumRows(rows);

            ArrowVectorizedBatchConverter converter =
                    new ArrowVectorizedBatchConverter(vsr, fieldWriters);
            converter.reset(
                    new VectorizedRecordIterator() {
                        @Override
                        public VectorizedColumnBatch batch() {
                            return batch;
                        }

                        @Override
                        public InternalRow next() {
                            return null;
                        }

                        @Override
                        public void releaseBatch() {}
                    });
            converter.next(rows);

            FixedSizeListVector listVector = (FixedSizeListVector) vsr.getVector(0);
            assertThat(listVector.isNull(0)).isFalse();
            assertThat(listVector.isNull(1)).isTrue();

            @SuppressWarnings("unchecked")
            List<Float> row0 = (List<Float>) listVector.getObject(0);
            assertThat(row0).containsExactly(1.0f, 2.0f, 3.0f);
            assertThat(listVector.getObject(1)).isNull();

            converter.close();
        }
    }

    @Test
    public void testNullableVectorColumnFromRowToColumnConverter() {
        RowType rowType = RowType.of(DataTypes.VECTOR(3, DataTypes.FLOAT()));
        RowToColumnConverter rowToColumnConverter = new RowToColumnConverter(rowType);

        HeapFloatVector elementVector = new HeapFloatVector(6);
        HeapVectorColumnVector vectorColumn = new HeapVectorColumnVector(2, elementVector, 3);
        WritableColumnVector[] vectors = new WritableColumnVector[] {vectorColumn};
        rowToColumnConverter.convert(GenericRow.of((Object) null), vectors);
        rowToColumnConverter.convert(
                GenericRow.of(BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.0f, 3.0f})),
                vectors);

        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);
            VectorizedColumnBatch batch =
                    new VectorizedColumnBatch(new ColumnVector[] {vectorColumn});
            batch.setNumRows(2);

            ArrowVectorizedBatchConverter converter =
                    new ArrowVectorizedBatchConverter(vsr, fieldWriters);
            converter.reset(
                    new VectorizedRecordIterator() {
                        @Override
                        public VectorizedColumnBatch batch() {
                            return batch;
                        }

                        @Override
                        public InternalRow next() {
                            return null;
                        }

                        @Override
                        public void releaseBatch() {}
                    });
            converter.next(2);

            FixedSizeListVector listVector = (FixedSizeListVector) vsr.getVector(0);
            assertThat(listVector.isNull(0)).isTrue();
            assertThat(listVector.getObject(0)).isNull();
            @SuppressWarnings("unchecked")
            List<Float> row1 = (List<Float>) listVector.getObject(1);
            assertThat(row1).containsExactly(1.0f, 2.0f, 3.0f);

            converter.close();
        }
    }

    @Test
    public void testVectorColumnWriteWithPickedInColumn() {
        RowType rowType = RowType.of(DataTypes.VECTOR(2, DataTypes.FLOAT()));
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);

            int length = 2;
            int rows = 4;
            HeapFloatVector elementVector = new HeapFloatVector(rows * length);
            float[] values = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
            for (int i = 0; i < values.length; i++) {
                elementVector.setFloat(i, values[i]);
            }

            VecColumnVector vector = new TestVecColumnVectorWithNulls(elementVector, length, null);

            int[] pickedInColumn = new int[] {2, 0};
            fieldWriters[0].reset();
            fieldWriters[0].write(vector, pickedInColumn, 0, pickedInColumn.length);

            FixedSizeListVector listVector = (FixedSizeListVector) vsr.getVector(0);
            @SuppressWarnings("unchecked")
            List<Float> row0 = (List<Float>) listVector.getObject(0);
            assertThat(row0).containsExactly(5.0f, 6.0f);
            @SuppressWarnings("unchecked")
            List<Float> row1 = (List<Float>) listVector.getObject(1);
            assertThat(row1).containsExactly(1.0f, 2.0f);

            vsr.close();
        }
    }

    private static class TestVecColumnVectorWithNulls implements VecColumnVector {

        private final ColumnVector data;
        private final int length;
        private final IntPredicate nulls;

        private TestVecColumnVectorWithNulls(ColumnVector data, int length, IntPredicate nulls) {
            this.data = data;
            this.length = length;
            this.nulls = nulls;
        }

        @Override
        public InternalVector getVector(int i) {
            return ColumnarVec.DEFAULT_FACTORY.create(data, i * length, length);
        }

        @Override
        public ColumnVector getColumnVector() {
            return data;
        }

        @Override
        public int getVectorSize() {
            return length;
        }

        @Override
        public boolean isNullAt(int i) {
            return (nulls != null) && nulls.test(i);
        }
    }
}
