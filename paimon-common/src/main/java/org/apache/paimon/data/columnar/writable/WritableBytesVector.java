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

import org.apache.paimon.data.columnar.BytesColumnVector;

import java.nio.ByteBuffer;

/** Writable {@link BytesColumnVector}. */
public interface WritableBytesVector extends WritableColumnVector, BytesColumnVector {

    /**
     * Append byte[] at rowId with the provided value. Note: Must append values according to the
     * order of rowId, can not random append.
     */
    void putByteArray(int rowId, byte[] value, int offset, int length);

    /** Puts the bytes between the buffer's position and limit without changing its position. */
    default void putByteBuffer(int rowId, ByteBuffer value) {
        if (value.hasArray()) {
            putByteArray(
                    rowId,
                    value.array(),
                    value.arrayOffset() + value.position(),
                    value.remaining());
        } else {
            ByteBuffer duplicate = value.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            putByteArray(rowId, bytes, 0, bytes.length);
        }
    }

    void appendByteArray(byte[] value, int offset, int length);

    /** Appends the bytes between the buffer's position and limit without changing its position. */
    default void appendByteBuffer(ByteBuffer value) {
        if (value.hasArray()) {
            appendByteArray(
                    value.array(), value.arrayOffset() + value.position(), value.remaining());
        } else {
            ByteBuffer duplicate = value.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            appendByteArray(bytes, 0, bytes.length);
        }
    }

    /** Fill the column vector with the provided value. */
    void fill(byte[] value);
}
