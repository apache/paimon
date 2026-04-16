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
import org.apache.paimon.data.BlobReference;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarVec;
import org.apache.paimon.data.columnar.VecColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowVectorizedBatchConverter}. */
public class ArrowVectorizedBatchConverterTest {

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

    @Test
    public void testBlobRefColumnWriteAndReadBack() {
        RowType rowType = RowType.of(DataTypes.BLOB_REF());
        try (RootAllocator allocator = new RootAllocator()) {
            VectorSchemaRoot vsr = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
            ArrowFieldWriter[] fieldWriters = ArrowUtils.createArrowFieldWriters(vsr, rowType);

            // Prepare serialized BlobReference bytes
            BlobReference ref0 = new BlobReference("default.upstream", 7, 100L);
            BlobReference ref1 = new BlobReference("default.upstream", 8, 200L);
            byte[] bytes0 = ref0.serialize();
            byte[] bytes1 = ref1.serialize();

            int rows = 3; // row 0 = ref0, row 1 = null, row 2 = ref1
            HeapBytesVector bytesVector = new HeapBytesVector(rows);
            bytesVector.appendByteArray(bytes0, 0, bytes0.length);
            bytesVector.appendByteArray(new byte[0], 0, 0); // placeholder for null
            bytesVector.appendByteArray(bytes1, 0, bytes1.length);
            bytesVector.setNullAt(1);

            VectorizedColumnBatch batch =
                    new VectorizedColumnBatch(new ColumnVector[] {bytesVector});
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

            // Verify the Arrow vector contains the correct binary data
            VarBinaryVector arrowVector = (VarBinaryVector) vsr.getVector(0);
            assertThat(arrowVector.isNull(0)).isFalse();
            assertThat(arrowVector.isNull(1)).isTrue();
            assertThat(arrowVector.isNull(2)).isFalse();

            // Read back and verify the bytes can be deserialized to BlobReference
            BlobReference readRef0 = BlobReference.deserialize(arrowVector.getObject(0));
            assertThat(readRef0.tableName()).isEqualTo("default.upstream");
            assertThat(readRef0.fieldId()).isEqualTo(7);
            assertThat(readRef0.rowId()).isEqualTo(100L);

            BlobReference readRef2 = BlobReference.deserialize(arrowVector.getObject(2));
            assertThat(readRef2.tableName()).isEqualTo("default.upstream");
            assertThat(readRef2.fieldId()).isEqualTo(8);
            assertThat(readRef2.rowId()).isEqualTo(200L);

            // Also verify the Arrow2Paimon round-trip
            Arrow2PaimonVectorConverter paimonConverter =
                    Arrow2PaimonVectorConverter.construct(DataTypes.BLOB_REF());
            ColumnVector paimonVector = paimonConverter.convertVector(arrowVector);
            assertThat(paimonVector.isNullAt(0)).isFalse();
            assertThat(paimonVector.isNullAt(1)).isTrue();
            assertThat(paimonVector.isNullAt(2)).isFalse();

            converter.close();
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
