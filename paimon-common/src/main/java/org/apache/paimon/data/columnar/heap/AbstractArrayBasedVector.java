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

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;

import java.util.Arrays;

/** Abstract class for vectors that have offsets and lengths. */
public class AbstractArrayBasedVector extends AbstractStructVector {

    protected long[] offsets;
    protected long[] lengths;

    public AbstractArrayBasedVector(int len, ColumnVector[] children) {
        super(len, children);
        this.offsets = new long[capacity];
        this.lengths = new long[capacity];
    }

    public void putOffsetLength(int index, long offset, long length) {
        offsets[index] = offset;
        lengths[index] = length;
    }

    public long[] getOffsets() {
        return offsets;
    }

    public void setOffsets(long[] offsets) {
        this.offsets = offsets;
    }

    public long[] getLengths() {
        return lengths;
    }

    public void setLengths(long[] lengths) {
        this.lengths = lengths;
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (offsets.length < newCapacity) {
            offsets = Arrays.copyOf(offsets, newCapacity);
            lengths = Arrays.copyOf(lengths, newCapacity);
        }
    }

    public void appendArray(int length) {
        reserve(elementsAppended + 1);
        for (ColumnVector child : children) {
            if (child instanceof WritableColumnVector) {
                WritableColumnVector writableColumnVector = (WritableColumnVector) child;
                writableColumnVector.reserve(writableColumnVector.getElementsAppended() + length);
            }
        }
        if (children[0] instanceof WritableColumnVector) {
            WritableColumnVector arrayData = (WritableColumnVector) children[0];
            putOffsetLength(elementsAppended, arrayData.getElementsAppended(), length);
        }
        elementsAppended++;
    }

    @Override
    public void reset() {
        super.reset();
        if (offsets.length != capacity) {
            offsets = new long[capacity];
        } else {
            Arrays.fill(offsets, 0);
        }
        if (lengths.length != capacity) {
            lengths = new long[capacity];
        } else {
            Arrays.fill(lengths, 0);
        }
    }
}
