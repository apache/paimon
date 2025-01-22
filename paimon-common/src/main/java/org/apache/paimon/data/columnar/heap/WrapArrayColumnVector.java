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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;

/** Wrap for ArrayColumnVector. */
public class WrapArrayColumnVector implements ArrayColumnVector {

    private final HeapArrayVector heapArrayVector;
    private final ColumnVector[] children;

    public WrapArrayColumnVector(HeapArrayVector heapArrayVector, ColumnVector[] children) {
        this.heapArrayVector = heapArrayVector;
        this.children = children;
    }

    @Override
    public InternalArray getArray(int i) {
        long offset = heapArrayVector.offsets[i];
        long length = heapArrayVector.lengths[i];
        return new ColumnarArray(children[0], (int) offset, (int) length);
    }

    @Override
    public ColumnVector getColumnVector() {
        return children[0];
    }

    @Override
    public boolean isNullAt(int i) {
        return heapArrayVector.isNullAt(i);
    }

    @Override
    public int getCapacity() {
        return heapArrayVector.getCapacity();
    }

    @Override
    public ColumnVector[] getChildren() {
        return children;
    }
}
