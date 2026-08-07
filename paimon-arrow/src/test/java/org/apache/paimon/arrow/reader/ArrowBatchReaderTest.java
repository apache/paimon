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

package org.apache.paimon.arrow.reader;

import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArrowBatchReader}. */
class ArrowBatchReaderTest {

    @Test
    void testReadBatchWrapperIsReused() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot firstRoot = intRoot(rowType, allocator, 11);
                VectorSchemaRoot secondRoot = intRoot(rowType, allocator, 22)) {
            ArrowBatchReader reader = new ArrowBatchReader(rowType, true);

            ColumnarRow first = (ColumnarRow) reader.readBatch(firstRoot).iterator().next();
            VectorizedColumnBatch reusableBatch = first.batch();
            assertThat(first.getInt(0)).isEqualTo(11);

            ColumnarRow second = (ColumnarRow) reader.readBatch(secondRoot).iterator().next();

            assertThat(second.batch()).isSameAs(reusableBatch);
            assertThat(second.getInt(0)).isEqualTo(22);
        }
    }

    @Test
    void testVectorizedBatchWrappersAreNotReused() {
        RowType rowType = RowType.builder().field("id", DataTypes.INT()).build();
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot firstRoot = intRoot(rowType, allocator, 11);
                VectorSchemaRoot secondRoot = intRoot(rowType, allocator, 22, 33)) {
            ArrowBatchReader reader = new ArrowBatchReader(rowType, true);

            VectorizedColumnBatch first = reader.readVectorizedBatch(firstRoot);
            VectorizedColumnBatch second = reader.readVectorizedBatch(secondRoot);

            assertThat(first).isNotSameAs(second);
            assertThat(first.columns).isNotSameAs(second.columns);
            assertThat(first.getNumRows()).isEqualTo(1);
            assertThat(second.getNumRows()).isEqualTo(2);
            assertThat(first.getInt(0, 0)).isEqualTo(11);
            assertThat(second.getInt(0, 0)).isEqualTo(22);
            assertThat(second.getInt(1, 0)).isEqualTo(33);
        }
    }

    private static VectorSchemaRoot intRoot(
            RowType rowType, RootAllocator allocator, int... values) {
        VectorSchemaRoot root = ArrowUtils.createVectorSchemaRoot(rowType, allocator);
        IntVector vector = (IntVector) root.getVector(0);
        vector.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            vector.setSafe(i, values[i]);
        }
        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }
}
