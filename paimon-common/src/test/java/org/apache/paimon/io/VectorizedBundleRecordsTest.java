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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.HeapIntVector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VectorizedBundleRecords}. */
public class VectorizedBundleRecordsTest {

    @Test
    public void testRowCountWithoutSelection() {
        VectorizedBundleRecords records = createRecords(5, null);
        assertThat(records.rowCount()).isEqualTo(5);
    }

    @Test
    public void testRowCountWithSelection() {
        VectorizedBundleRecords records = createRecords(5, new int[] {0, 2, 4});
        assertThat(records.rowCount()).isEqualTo(3);
    }

    @Test
    public void testRowCountWithEmptySelection() {
        VectorizedBundleRecords records = createRecords(5, new int[] {});
        assertThat(records.rowCount()).isEqualTo(0);
    }

    @Test
    public void testIteratorWithoutSelection() {
        VectorizedBundleRecords records = createRecords(4, null);
        List<Integer> values = collectIntValues(records);
        assertThat(values).containsExactly(10, 20, 30, 40);
    }

    @Test
    public void testIteratorWithSelection() {
        VectorizedBundleRecords records = createRecords(5, new int[] {1, 3});
        List<Integer> values = collectIntValues(records);
        assertThat(values).containsExactly(20, 40);
    }

    @Test
    public void testIteratorWithEmptySelection() {
        VectorizedBundleRecords records = createRecords(3, new int[] {});
        List<Integer> values = collectIntValues(records);
        assertThat(values).isEmpty();
    }

    @Test
    public void testIteratorWithAllSelected() {
        VectorizedBundleRecords records = createRecords(3, new int[] {0, 1, 2});
        List<Integer> values = collectIntValues(records);
        assertThat(values).containsExactly(10, 20, 30);
    }

    @Test
    public void testBatchAndSelectedAccessors() {
        HeapIntVector intVector = new HeapIntVector(3);
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(3);
        int[] selected = new int[] {0, 2};

        VectorizedBundleRecords records = new VectorizedBundleRecords(batch, selected);
        assertThat(records.batch()).isSameAs(batch);
        assertThat(records.selected()).isSameAs(selected);
    }

    @Test
    public void testNullSelected() {
        VectorizedBundleRecords records = createRecords(2, null);
        assertThat(records.selected()).isNull();
    }

    private VectorizedBundleRecords createRecords(int numRows, int[] selected) {
        HeapIntVector intVector = new HeapIntVector(numRows);
        for (int i = 0; i < numRows; i++) {
            intVector.setInt(i, (i + 1) * 10);
        }
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {intVector});
        batch.setNumRows(numRows);
        return new VectorizedBundleRecords(batch, selected);
    }

    private List<Integer> collectIntValues(VectorizedBundleRecords records) {
        List<Integer> values = new ArrayList<>();
        for (InternalRow row : records) {
            values.add(row.getInt(0));
        }
        return values;
    }
}
