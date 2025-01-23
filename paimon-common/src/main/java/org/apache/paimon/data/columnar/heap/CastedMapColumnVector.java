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

import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarMap;
import org.apache.paimon.data.columnar.MapColumnVector;

/** Wrap for MapColumnVector. */
public class CastedMapColumnVector implements MapColumnVector {

    private final HeapMapVector heapMapVector;
    private final ColumnVector[] children;

    public CastedMapColumnVector(HeapMapVector heapMapVector, ColumnVector[] children) {
        this.heapMapVector = heapMapVector;
        this.children = children;
    }

    @Override
    public InternalMap getMap(int i) {
        long offset = heapMapVector.offsets[i];
        long length = heapMapVector.lengths[i];
        return new ColumnarMap(children[0], children[1], (int) offset, (int) length);
    }

    @Override
    public boolean isNullAt(int i) {
        return heapMapVector.isNullAt(i);
    }

    @Override
    public int getCapacity() {
        return heapMapVector.getCapacity();
    }

    @Override
    public ColumnVector[] getChildren() {
        return children;
    }
}
