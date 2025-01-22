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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.Dictionary;

import java.io.Serializable;

/**
 * Contains the shared structure for {@link ColumnVector}s, including NULL information and
 * dictionary. NOTE: if there are some nulls, must set {@link #noNulls} to false.
 */
public abstract class AbstractWritableVector implements WritableColumnVector, Serializable {

    private static final long serialVersionUID = 1L;

    protected final int initialCapacity;

    // If the whole column vector has no nulls, this is true, otherwise false.
    protected boolean noNulls = true;

    protected boolean isAllNull = false;

    /** Current write cursor (row index) when appending data. */
    protected int elementsAppended;

    protected int capacity;

    /**
     * The Dictionary for this column. If it's not null, will be used to decode the value in get().
     */
    protected Dictionary dictionary;

    public AbstractWritableVector(int capacity) {
        this.capacity = capacity;
        this.initialCapacity = capacity;
    }

    /** Update the dictionary. */
    @Override
    public void setDictionary(Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    /** Returns true if this column has a dictionary. */
    @Override
    public boolean hasDictionary() {
        return dictionary != null;
    }

    @Override
    public void setAllNull() {
        isAllNull = true;
        noNulls = false;
    }

    @Override
    public boolean isAllNull() {
        return isAllNull;
    }

    @Override
    public int getElementsAppended() {
        return elementsAppended;
    }

    /** Increment number of elements appended by 'num'. */
    @Override
    public final void addElementsAppended(int num) {
        elementsAppended += num;
    }

    @Override
    public int getCapacity() {
        return this.capacity;
    }

    @Override
    public void reset() {
        noNulls = true;
        isAllNull = false;
        elementsAppended = 0;
        capacity = initialCapacity;
    }

    @Override
    public void reserve(int requiredCapacity) {
        if (requiredCapacity < 0) {
            throw new IllegalArgumentException("Invalid capacity: " + requiredCapacity);
        } else if (requiredCapacity > capacity) {
            int newCapacity = (int) Math.min(Integer.MAX_VALUE, requiredCapacity * 2L);
            if (requiredCapacity <= newCapacity) {
                try {
                    reserveInternal(newCapacity);
                } catch (OutOfMemoryError outOfMemoryError) {
                    throw new RuntimeException(
                            "Failed to allocate memory for vector", outOfMemoryError);
                }
            } else {
                throw new UnsupportedOperationException(
                        "Cannot allocate :" + newCapacity + " elements");
            }
            capacity = newCapacity;
        }
    }

    protected abstract void reserveInternal(int newCapacity);
}
