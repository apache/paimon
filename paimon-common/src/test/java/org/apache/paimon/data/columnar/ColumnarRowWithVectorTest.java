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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for vector access in {@link ColumnarRow}. */
public class ColumnarRowWithVectorTest {

    @Test
    public void testVectorAccess() {
        int length = 3;
        float[] values = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f};

        HeapFloatVector elementVector = new HeapFloatVector(values.length);
        for (int i = 0; i < values.length; i++) {
            elementVector.setFloat(i, values[i]);
        }

        VecColumnVector vector =
                new VecColumnVector() {
                    @Override
                    public InternalVector getVector(int i) {
                        return new ColumnarVec(elementVector, i * length, length);
                    }

                    @Override
                    public ColumnVector getColumnVector() {
                        return elementVector;
                    }

                    @Override
                    public int getVectorSize() {
                        return length;
                    }

                    @Override
                    public boolean isNullAt(int i) {
                        return false;
                    }
                };
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {vector});
        batch.setNumRows(2);

        ColumnarRow row = new ColumnarRow(batch);
        row.setRowId(0);
        assertThat(row.getVector(0).toFloatArray()).isEqualTo(new float[] {1.0f, 2.0f, 3.0f});

        row.setRowId(1);
        assertThat(row.getVector(0).toFloatArray()).isEqualTo(new float[] {4.0f, 5.0f, 6.0f});
    }
}
