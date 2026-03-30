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

import java.math.BigDecimal;

/** Native JNI methods for array operations. */
public final class NativeArrayMethods {
    static {
        NativeLoader.loadJni();
    }

    private NativeArrayMethods() {}

    public static native long nbytes(long pointer);

    public static native void exportToArrow(
            long pointer, long[] schemaPointer, long[] arrayPointer);

    public static native void dropArrowSchema(long arrowSchemaPtr);

    public static native void dropArrowArray(long arrowArrayPtr);

    public static native void free(long pointer);

    public static native long getLen(long pointer);

    public static native long getDataType(long pointer);

    public static native long getField(long pointer, int index);

    public static native long slice(long pointer, int start, int stop);

    public static native boolean getNull(long pointer, int index);

    public static native int getNullCount(long pointer);

    public static native byte getByte(long pointer, int index);

    public static native short getShort(long pointer, int index);

    public static native int getInt(long pointer, int index);

    public static native long getLong(long pointer, int index);

    public static native boolean getBool(long pointer, int index);

    public static native float getFloat(long pointer, int index);

    public static native double getDouble(long pointer, int index);

    public static native BigDecimal getBigDecimal(long pointer, int index);

    public static native String getUTF8(long pointer, int index);

    public static native void getUTF8_ptr_len(long pointer, int index, long[] outPtr, int[] outLen);

    public static native byte[] getBinary(long pointer, int index);
}
