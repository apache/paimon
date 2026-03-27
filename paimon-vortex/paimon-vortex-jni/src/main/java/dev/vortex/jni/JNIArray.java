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

package dev.vortex.jni;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.util.OptionalLong;

/** JNI implementation of the Array interface. */
public final class JNIArray implements Array {
    static {
        NativeLoader.loadJni();
    }

    private final ThreadLocal<long[]> schemaPtr = ThreadLocal.withInitial(() -> new long[1]);
    private final ThreadLocal<long[]> arrayPtr = ThreadLocal.withInitial(() -> new long[1]);

    private OptionalLong pointer;

    public JNIArray(long pointer) {
        Preconditions.checkArgument(pointer > 0, "Invalid pointer address: " + pointer);
        this.pointer = OptionalLong.of(pointer);
    }

    @Override
    public long getLen() {
        return NativeArrayMethods.getLen(pointer.getAsLong());
    }

    @Override
    public long nbytes() {
        return NativeArrayMethods.nbytes(pointer.getAsLong());
    }

    @Override
    public VectorSchemaRoot exportToArrow(BufferAllocator allocator, VectorSchemaRoot reuse) {
        NativeArrayMethods.exportToArrow(pointer.getAsLong(), schemaPtr.get(), arrayPtr.get());
        try (ArrowSchema arrowSchema = ArrowSchema.wrap(schemaPtr.get()[0]);
                ArrowArray arrowArray = ArrowArray.wrap(arrayPtr.get()[0]);
                CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
            if (reuse != null) {
                Data.importIntoVectorSchemaRoot(allocator, arrowArray, reuse, provider);
                return reuse;
            } else {
                return Data.importVectorSchemaRoot(
                        allocator, arrowArray, arrowSchema, new CDataDictionaryProvider());
            }
        } finally {
            NativeArrayMethods.dropArrowSchema(schemaPtr.get()[0]);
            NativeArrayMethods.dropArrowArray(arrayPtr.get()[0]);
        }
    }

    @Override
    public DType getDataType() {
        return new JNIDType(NativeArrayMethods.getDataType(pointer.getAsLong()));
    }

    @Override
    public Array getField(int index) {
        return new JNIArray(NativeArrayMethods.getField(pointer.getAsLong(), index));
    }

    @Override
    public Array slice(int start, int stop) {
        return new JNIArray(NativeArrayMethods.slice(pointer.getAsLong(), start, stop));
    }

    @Override
    public boolean getNull(int index) {
        return NativeArrayMethods.getNull(pointer.getAsLong(), index);
    }

    @Override
    public int getNullCount() {
        return NativeArrayMethods.getNullCount(pointer.getAsLong());
    }

    @Override
    public byte getByte(int index) {
        return NativeArrayMethods.getByte(pointer.getAsLong(), index);
    }

    @Override
    public short getShort(int index) {
        return NativeArrayMethods.getShort(pointer.getAsLong(), index);
    }

    @Override
    public int getInt(int index) {
        return NativeArrayMethods.getInt(pointer.getAsLong(), index);
    }

    @Override
    public long getLong(int index) {
        return NativeArrayMethods.getLong(pointer.getAsLong(), index);
    }

    @Override
    public boolean getBool(int index) {
        return NativeArrayMethods.getBool(pointer.getAsLong(), index);
    }

    @Override
    public float getFloat(int index) {
        return NativeArrayMethods.getFloat(pointer.getAsLong(), index);
    }

    @Override
    public double getDouble(int index) {
        return NativeArrayMethods.getDouble(pointer.getAsLong(), index);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return NativeArrayMethods.getBigDecimal(pointer.getAsLong(), index);
    }

    @Override
    public String getUTF8(int index) {
        return NativeArrayMethods.getUTF8(pointer.getAsLong(), index);
    }

    @Override
    public void getUTF8_ptr_len(int index, long[] ptr, int[] len) {
        NativeArrayMethods.getUTF8_ptr_len(pointer.getAsLong(), index, ptr, len);
    }

    @Override
    public byte[] getBinary(int index) {
        return NativeArrayMethods.getBinary(pointer.getAsLong(), index);
    }

    @Override
    public void close() {
        if (!pointer.isPresent()) {
            return;
        }
        NativeArrayMethods.free(pointer.getAsLong());
        pointer = OptionalLong.empty();
    }
}
