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

import org.apache.paimon.data.columnar.writable.WritableBooleanVector;

import java.util.Arrays;

/** This class represents a nullable heap boolean column vector. */
public class HeapBooleanVector extends AbstractHeapVector implements WritableBooleanVector {

    private static final long serialVersionUID = 4131239076731313596L;

    public boolean[] vector;

    public HeapBooleanVector(int len) {
        super(len);
        vector = new boolean[len];
    }

    @Override
    public HeapIntVector reserveDictionaryIds(int capacity) {
        throw new RuntimeException("HeapBooleanVector has no dictionary.");
    }

    @Override
    public HeapIntVector getDictionaryIds() {
        throw new RuntimeException("HeapBooleanVector has no dictionary.");
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (vector.length < newCapacity) {
            vector = Arrays.copyOf(vector, newCapacity);
        }
    }

    @Override
    public boolean getBoolean(int i) {
        return vector[i];
    }

    @Override
    public void setBoolean(int i, boolean value) {
        vector[i] = value;
    }

    @Override
    public void setBooleans(int rowId, int count, boolean value) {
        for (int i = 0; i < count; ++i) {
            vector[i + rowId] = value;
        }
    }

    @Override
    public void setBooleans(int rowId, int count, byte src, int srcIndex) {
        assert (count + srcIndex <= 8);
        for (int i = 0; i < count; i++) {
            vector[i + rowId] = (byte) (src >>> (i + srcIndex) & 1) == 1;
        }
    }

    @Override
    public void setBooleans(int rowId, byte src) {
        setBooleans(rowId, 8, src, 0);
    }

    @Override
    public void appendBoolean(boolean v) {
        reserve(elementsAppended + 1);
        setBoolean(elementsAppended, v);
        elementsAppended++;
    }

    @Override
    public void fill(boolean value) {
        Arrays.fill(vector, value);
    }

    @Override
    public void reset() {
        super.reset();
        if (vector.length != capacity) {
            vector = new boolean[capacity];
        } else {
            Arrays.fill(vector, false);
        }
    }
}
