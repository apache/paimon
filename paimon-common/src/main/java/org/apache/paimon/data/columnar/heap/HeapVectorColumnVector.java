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

import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarVec;
import org.apache.paimon.data.columnar.VecColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;

/** Heap column vector for VectorType. */
public class HeapVectorColumnVector extends AbstractArrayBasedVector implements VecColumnVector {

    private final int vectorSize;

    public HeapVectorColumnVector(int len, ColumnVector vector, int vectorSize) {
        super(len, new ColumnVector[] {vector});
        if (vectorSize <= 0) {
            throw new IllegalArgumentException("Vector size must be positive.");
        }
        this.vectorSize = vectorSize;
    }

    public void appendVector() {
        reserve(elementsAppended + 1);
        long offset = (long) elementsAppended * vectorSize;
        reserveChild(offset + vectorSize);
        putOffsetLength(elementsAppended, offset, vectorSize);
        elementsAppended++;
    }

    @Override
    public void appendNull() {
        int index = elementsAppended;
        appendVector();
        setNullAt(index);

        ColumnVector child = children[0];
        if (child instanceof WritableColumnVector) {
            WritableColumnVector writableChild = (WritableColumnVector) child;
            int offset = (int) offsets[index];
            writableChild.setNulls(offset, vectorSize);
            writableChild.addElementsAppended(vectorSize);
        }
    }

    @Override
    public InternalVector getVector(int i) {
        long offset = offsets[i];
        long length = lengths[i];
        if (length != vectorSize) {
            throw new IllegalArgumentException(
                    "Vector length mismatch: expected " + vectorSize + " but got " + length);
        }
        return ColumnarVec.DEFAULT_FACTORY.create(children[0], (int) offset, (int) length);
    }

    @Override
    public ColumnVector getColumnVector() {
        return children[0];
    }

    @Override
    public int getVectorSize() {
        return vectorSize;
    }

    private void reserveChild(long requiredCapacity) {
        if (requiredCapacity > Integer.MAX_VALUE) {
            throw new UnsupportedOperationException(
                    "Cannot allocate " + requiredCapacity + " vector elements");
        }
        ColumnVector child = children[0];
        if (child instanceof WritableColumnVector) {
            ((WritableColumnVector) child).reserve((int) requiredCapacity);
        }
    }
}
