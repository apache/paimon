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
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarArray;
import org.apache.paimon.data.columnar.ColumnarVec;
import org.apache.paimon.data.columnar.VecColumnVector;

/** This class represents a nullable heap vector column vector. */
public class HeapVecVector extends AbstractArrayBasedVector
        implements VecColumnVector, ArrayColumnVector {

    private final int vectorSize;

    public HeapVecVector(int len, int vectorSize, ColumnVector vector) {
        super(len, new ColumnVector[] {vector});
        this.vectorSize = vectorSize;
    }

    @Override
    public InternalVector getVector(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        if (length <= 0) {
            return null;
        }
        return ColumnarVec.DEFAULT_FACTORY.create(children[0], (int) offset, (int) length);
    }

    @Override
    public InternalArray getArray(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        return new ColumnarArray(children[0], (int) offset, (int) length);
    }

    @Override
    public ColumnVector getColumnVector() {
        return children[0];
    }

    @Override
    public int getVectorSize() {
        return vectorSize;
    }
}
