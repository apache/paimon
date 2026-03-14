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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;

/**
 * Cast internal Vector to paimon readable vector(cast for Timestamp type and Decimal type) for row
 * type.
 */
public class CastedRowColumnVector implements RowColumnVector {

    private final VectorizedColumnBatch vectorizedColumnBatch;
    private final HeapRowVector heapRowVector;
    private final ColumnVector[] children;

    public CastedRowColumnVector(HeapRowVector heapRowVector, ColumnVector[] children) {
        this.heapRowVector = heapRowVector;
        this.children = children;
        this.vectorizedColumnBatch = new VectorizedColumnBatch(children);
    }

    @Override
    public InternalRow getRow(int i) {
        ColumnarRow columnarRow = new ColumnarRow(vectorizedColumnBatch);
        columnarRow.setRowId(i);
        return columnarRow;
    }

    @Override
    public VectorizedColumnBatch getBatch() {
        return vectorizedColumnBatch;
    }

    @Override
    public boolean isNullAt(int i) {
        return heapRowVector.isNullAt(i);
    }

    @Override
    public int getCapacity() {
        return heapRowVector.getCapacity();
    }

    public int getElementsAppended() {
        return heapRowVector.getElementsAppended();
    }

    @Override
    public ColumnVector[] getChildren() {
        return children;
    }
}
