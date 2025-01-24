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

package org.apache.paimon.data.calumnar.heap;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for RowColumnVector. */
public class RowColumnVectorTest {

    @Test
    public void testRowVector() {

        HeapIntVector heapIntVector = new HeapIntVector(100);

        for (int i = 0; i < 100; i++) {
            heapIntVector.setInt(i, i);
        }

        HeapRowVector rowColumnVector = new HeapRowVector(100, heapIntVector);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(rowColumnVector.getRow(i));
        }

        for (int i = 0; i < 100; i++) {
            InternalRow row = rows.get(i);
            assertThat(row.getInt(0)).isEqualTo(i);
        }

        rows.clear();
        CastedRowColumnVector castedRowColumnVector =
                new CastedRowColumnVector(rowColumnVector, new ColumnVector[] {heapIntVector});
        for (int i = 0; i < 100; i++) {
            rows.add(castedRowColumnVector.getRow(i));
        }
        for (int i = 0; i < 100; i++) {
            InternalRow row = rows.get(i);
            assertThat(row.getInt(0)).isEqualTo(i);
        }
    }
}
