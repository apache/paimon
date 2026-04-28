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

package dev.vortex.api;

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public interface Array extends AutoCloseable {
    /**
     * Returns the number of elements in this array.
     *
     * @return the length of the array
     */
    long getLen();

    /**
     * Returns the total size in bytes of this array including all buffers.
     *
     * @return the size in bytes
     */
    long nbytes();

    /**
     * Export to an ArrowVector. The data will now be owned by the VectorSchemaRoot after this operation.
     */
    VectorSchemaRoot exportToArrow(BufferAllocator allocator, VectorSchemaRoot reuse);

    /**
     * Returns the data type of this array.
     *
     * @return the DType describing the logical type of this array
     */
    DType getDataType();

    /**
     * Returns a child array at the given field index.
     *
     * <p>This is used for accessing fields in struct arrays or elements in list arrays.</p>
     *
     * @param index the field index
     * @return the child array at the specified index
     * @throws IndexOutOfBoundsException if index is out of bounds
     */
    Array getField(int index);

    /**
     * Returns a slice of this array from start (inclusive) to stop (exclusive).
     *
     * @param start the starting index (inclusive)
     * @param stop the ending index (exclusive)
     * @return a new Array containing the sliced elements
     * @throws IndexOutOfBoundsException if start or stop are out of bounds
     */
    Array slice(int start, int stop);

    /**
     * Returns true if the value at the given index is null.
     *
     * @param index the element index
     * @return true if the value is null, false otherwise
     * @throws IndexOutOfBoundsException if index is out of bounds
     */
    boolean getNull(int index);

    /**
     * Returns the total number of null values in this array.
     *
     * @return the null count
     */
    int getNullCount();

    /**
     * Returns the byte value at the given index.
     *
     * @param index the element index
     * @return the byte value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with byte
     */
    byte getByte(int index);

    /**
     * Returns the short value at the given index.
     *
     * @param index the element index
     * @return the short value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with short
     */
    short getShort(int index);

    /**
     * Returns the int value at the given index.
     *
     * @param index the element index
     * @return the int value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with int
     */
    int getInt(int index);

    /**
     * Returns the long value at the given index.
     *
     * @param index the element index
     * @return the long value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with long
     */
    long getLong(int index);

    /**
     * Returns the boolean value at the given index.
     *
     * @param index the element index
     * @return the boolean value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with boolean
     */
    boolean getBool(int index);

    /**
     * Returns the float value at the given index.
     *
     * @param index the element index
     * @return the float value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with float
     */
    float getFloat(int index);

    /**
     * Returns the double value at the given index.
     *
     * @param index the element index
     * @return the double value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with double
     */
    double getDouble(int index);

    /**
     * Returns the BigDecimal value at the given index.
     *
     * @param index the element index
     * @return the BigDecimal value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with decimal
     */
    BigDecimal getBigDecimal(int index);

    /**
     * Returns the UTF-8 string value at the given index.
     *
     * @param index the element index
     * @return the string value
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with string
     */
    String getUTF8(int index);

    /**
     * Returns the UTF-8 string value at the given index as a pointer and length.
     *
     * <p>This is a low-level method that provides direct access to the underlying
     * string data without copying. The pointer and length are written to the
     * provided arrays.</p>
     *
     * @param index the element index
     * @param ptr array to store the pointer (first element will be set)
     * @param len array to store the length (first element will be set)
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with string
     */
    void getUTF8_ptr_len(int index, long[] ptr, int[] len);

    /**
     * Returns the binary value at the given index as a byte array.
     *
     * @param index the element index
     * @return the binary value as a byte array
     * @throws IndexOutOfBoundsException if index is out of bounds
     * @throws ClassCastException if the array type is not compatible with binary
     */
    byte[] getBinary(int index);

    @Override
    void close();
}
