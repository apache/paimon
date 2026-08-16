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

import org.apache.paimon.data.columnar.heap.HeapBooleanVector;
import org.apache.paimon.data.columnar.heap.HeapFloatVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ColumnarVec}. */
public class ColumnarVecTest {

    @Test
    public void testIntVectorAccess() {
        HeapIntVector intVector = new HeapIntVector(6);
        intVector.setInt(0, 1);
        intVector.setInt(1, 2);
        intVector.setInt(2, 3);
        intVector.setInt(3, 4);
        intVector.setInt(4, 5);
        intVector.setInt(5, 6);

        ColumnarVec vector = ColumnarVec.DEFAULT_FACTORY.create(intVector, 1, 3);

        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.toIntArray()).isEqualTo(new int[] {2, 3, 4});
    }

    @Test
    public void testFloatVectorAccess() {
        HeapFloatVector floatVector = new HeapFloatVector(5);
        floatVector.setFloat(0, 1.0f);
        floatVector.setFloat(1, 2.0f);
        floatVector.setFloat(2, 3.0f);
        floatVector.setFloat(3, 4.0f);
        floatVector.setFloat(4, 5.0f);

        ColumnarVec vector = ColumnarVec.DEFAULT_FACTORY.create(floatVector, 2, 2);

        assertThat(vector.size()).isEqualTo(2);
        assertThat(vector.toFloatArray()).isEqualTo(new float[] {3.0f, 4.0f});
    }

    @Test
    public void testRefuseNullElements() {
        HeapBooleanVector floatVector = new HeapBooleanVector(5);
        floatVector.setBoolean(0, true);
        floatVector.setBoolean(1, false);
        floatVector.setBoolean(2, true);
        floatVector.setBoolean(3, false);
        floatVector.setNullAt(4);

        ColumnarVec vector = ColumnarVec.DEFAULT_FACTORY.create(floatVector, 1, 3);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.toBooleanArray()).isEqualTo(new boolean[] {false, true, false});

        assertThatThrownBy(() -> ColumnarVec.DEFAULT_FACTORY.create(floatVector, 2, 3))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("refuse null elements");
    }
}
