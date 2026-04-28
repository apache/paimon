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

/** Vortex array interface for accessing columnar data. */
public interface Array extends AutoCloseable {
    long getLen();

    long nbytes();

    VectorSchemaRoot exportToArrow(BufferAllocator allocator, VectorSchemaRoot reuse);

    DType getDataType();

    Array getField(int index);

    Array slice(int start, int stop);

    boolean getNull(int index);

    int getNullCount();

    byte getByte(int index);

    short getShort(int index);

    int getInt(int index);

    long getLong(int index);

    boolean getBool(int index);

    float getFloat(int index);

    double getDouble(int index);

    BigDecimal getBigDecimal(int index);

    String getUTF8(int index);

    void getUTF8_ptr_len(int index, long[] ptr, int[] len);

    byte[] getBinary(int index);

    @Override
    void close();
}
