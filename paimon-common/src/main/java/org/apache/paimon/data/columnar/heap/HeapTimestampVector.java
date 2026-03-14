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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.writable.WritableTimestampVector;

import java.util.Arrays;

/** This class represents a nullable byte column vector. */
public class HeapTimestampVector extends AbstractHeapVector implements WritableTimestampVector {

    private static final long serialVersionUID = 1L;

    private long[] milliseconds;
    private int[] nanoOfMilliseconds;

    public HeapTimestampVector(int len) {
        super(len);
        this.milliseconds = new long[len];
        this.nanoOfMilliseconds = new int[len];
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (milliseconds.length < newCapacity) {
            milliseconds = Arrays.copyOf(milliseconds, newCapacity);
            nanoOfMilliseconds = Arrays.copyOf(nanoOfMilliseconds, newCapacity);
        }
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        if (dictionary == null) {
            return Timestamp.fromEpochMillis(milliseconds[i], nanoOfMilliseconds[i]);
        } else {
            return dictionary.decodeToTimestamp(dictionaryIds.vector[i]);
        }
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp) {
        milliseconds[i] = timestamp.getMillisecond();
        nanoOfMilliseconds[i] = timestamp.getNanoOfMillisecond();
    }

    @Override
    public void appendTimestamp(Timestamp timestamp) {
        reserve(elementsAppended + 1);
        setTimestamp(elementsAppended, timestamp);
        elementsAppended++;
    }

    @Override
    public void fill(Timestamp value) {
        Arrays.fill(milliseconds, value.getMillisecond());
        Arrays.fill(nanoOfMilliseconds, value.getNanoOfMillisecond());
    }

    @Override
    public void reset() {
        super.reset();
        if (milliseconds.length != capacity) {
            milliseconds = new long[capacity];
        } else {
            Arrays.fill(milliseconds, 0L);
        }
        if (nanoOfMilliseconds.length != capacity) {
            nanoOfMilliseconds = new int[capacity];
        } else {
            Arrays.fill(nanoOfMilliseconds, 0);
        }
    }
}
