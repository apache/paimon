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

import org.apache.paimon.data.columnar.writable.WritableBytesVector;

import java.util.Arrays;

/**
 * This class supports string and binary data by value reference -- i.e. each field is explicitly
 * present, as opposed to provided by a dictionary reference. In some cases, all the values will be
 * in the same byte array to begin with, but this need not be the case. If each value is in a
 * separate byte array to start with, or not all of the values are in the same original byte array,
 * you can still assign data by reference into this column vector. This gives flexibility to use
 * this in multiple situations.
 *
 * <p>When setting data by reference, the caller is responsible for allocating the byte arrays used
 * to hold the data. You can also set data by value, as long as you call the initBuffer() method
 * first. You can mix "by value" and "by reference" in the same column vector, though that use is
 * probably not typical.
 */
public class HeapBytesVector extends AbstractHeapVector implements WritableBytesVector {

    private static final long serialVersionUID = -8529155738773478597L;

    /** start offset of each field. */
    public int[] start;

    /** The length of each field. */
    public int[] length;

    /** buffer to use when actually copying in data. */
    public byte[] buffer;

    private int bytesAppended;

    public HeapBytesVector(int capacity) {
        super(capacity);
        buffer = new byte[capacity * 16];
        start = new int[capacity];
        length = new int[capacity];
    }

    @Override
    public void reset() {
        super.reset();
        if (start.length != capacity) {
            start = new int[capacity];
        } else {
            Arrays.fill(start, 0);
        }

        if (length.length != capacity) {
            length = new int[capacity];
        } else {
            Arrays.fill(length, 0);
        }

        // We don't reset buffer to avoid unnecessary copy.
        Arrays.fill(buffer, (byte) 0);

        this.bytesAppended = 0;
    }

    @Override
    public void putByteArray(int elementNum, byte[] sourceBuf, int start, int length) {
        reserveBytes(bytesAppended + length);
        System.arraycopy(sourceBuf, start, buffer, bytesAppended, length);
        this.start[elementNum] = bytesAppended;
        this.length[elementNum] = length;
        bytesAppended += length;
    }

    @Override
    public void appendByteArray(byte[] value, int offset, int length) {
        reserve(elementsAppended + 1);
        putByteArray(elementsAppended, value, offset, length);
        elementsAppended++;
    }

    @Override
    public void fill(byte[] value) {
        reserveBytes(start.length * value.length);
        for (int i = 0; i < start.length; i++) {
            System.arraycopy(value, 0, buffer, i * value.length, value.length);
        }
        for (int i = 0; i < start.length; i++) {
            this.start[i] = i * value.length;
        }
        Arrays.fill(this.length, value.length);
    }

    private void reserveBytes(int newCapacity) {
        if (newCapacity > buffer.length) {
            int newBytesCapacity = newCapacity * 2;
            try {
                buffer = Arrays.copyOf(buffer, newBytesCapacity);
            } catch (NegativeArraySizeException e) {
                throw new RuntimeException(
                        String.format(
                                "The new claimed capacity %s is too large, will overflow the INTEGER.MAX after multiply by 2. "
                                        + "Try reduce `read.batch-size` to avoid this exception.",
                                newCapacity),
                        e);
            }
        }
    }

    @Override
    void reserveForHeapVector(int newCapacity) {
        if (newCapacity > capacity) {
            capacity = newCapacity;
            start = Arrays.copyOf(start, newCapacity);
            length = Arrays.copyOf(length, newCapacity);
        }
    }

    @Override
    public Bytes getBytes(int i) {
        if (dictionary == null) {
            return new Bytes(buffer, start[i], length[i]);
        } else {
            byte[] bytes = dictionary.decodeToBinary(dictionaryIds.vector[i]);
            return new Bytes(bytes, 0, bytes.length);
        }
    }
}
