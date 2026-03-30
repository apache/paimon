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

import dev.vortex.jni.JNIDType;
import dev.vortex.jni.NativeDTypeMethods;

import java.util.List;
import java.util.Optional;

/** Vortex logical type interface representing the schema and metadata for array data. */
public interface DType extends AutoCloseable {

    Variant getVariant();

    boolean isNullable();

    List<String> getFieldNames();

    List<DType> getFieldTypes();

    DType getElementType();

    int getFixedSizeListSize();

    boolean isDate();

    boolean isTime();

    boolean isTimestamp();

    TimeUnit getTimeUnit();

    Optional<String> getTimeZone();

    boolean isDecimal();

    int getPrecision();

    byte getScale();

    @Override
    void close();

    static DType newByte(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newByte(isNullable), true);
    }

    static DType newShort(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newShort(isNullable), true);
    }

    static DType newInt(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newInt(isNullable), true);
    }

    static DType newLong(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newLong(isNullable), true);
    }

    static DType newFloat(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newFloat(isNullable), true);
    }

    static DType newDouble(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newDouble(isNullable), true);
    }

    static DType newDecimal(int precision, int scale, boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newDecimal(precision, scale, isNullable), true);
    }

    static DType newUtf8(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newUtf8(isNullable), true);
    }

    static DType newBinary(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newBinary(isNullable), true);
    }

    static DType newBool(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newBool(isNullable), true);
    }

    static DType newList(DType element, boolean isNullable) {
        JNIDType jniType = (JNIDType) element;
        return new JNIDType(NativeDTypeMethods.newList(jniType.getPointer(), isNullable), true);
    }

    static DType newFixedSizeList(DType element, int size, boolean isNullable) {
        JNIDType jniType = (JNIDType) element;
        return new JNIDType(
                NativeDTypeMethods.newFixedSizeList(jniType.getPointer(), size, isNullable), true);
    }

    static DType newStruct(String[] fieldNames, DType[] fieldTypes, boolean isNullable) {
        long[] ptrs = new long[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            ptrs[i] = ((JNIDType) fieldTypes[i]).getPointer();
        }
        return new JNIDType(NativeDTypeMethods.newStruct(fieldNames, ptrs, isNullable), true);
    }

    static DType newTimestamp(TimeUnit unit, Optional<String> timeZone, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(
                NativeDTypeMethods.newTimestamp(timeUnit, timeZone.orElse(null), isNullable), true);
    }

    static DType newDate(TimeUnit unit, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(NativeDTypeMethods.newDate(timeUnit, isNullable), true);
    }

    static DType newTime(TimeUnit unit, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(NativeDTypeMethods.newTime(timeUnit, isNullable), true);
    }

    /** Time units supported by Vortex temporal data types. */
    enum TimeUnit {
        NANOSECONDS,
        MICROSECONDS,
        MILLISECONDS,
        SECONDS,
        DAYS;

        public static TimeUnit from(byte unit) {
            switch (unit) {
                case 0:
                    return NANOSECONDS;
                case 1:
                    return MICROSECONDS;
                case 2:
                    return MILLISECONDS;
                case 3:
                    return SECONDS;
                case 4:
                    return DAYS;
                default:
                    throw new IllegalArgumentException("Unknown TimeUnit: " + unit);
            }
        }

        public byte asByte() {
            switch (this) {
                case NANOSECONDS:
                    return 0;
                case MICROSECONDS:
                    return 1;
                case MILLISECONDS:
                    return 2;
                case SECONDS:
                    return 3;
                case DAYS:
                    return 4;
                default:
                    throw new IllegalArgumentException("Unknown TimeUnit: " + this);
            }
        }
    }

    /** All supported data type variants in Vortex. */
    enum Variant {
        NULL,
        BOOL,
        PRIMITIVE_U8,
        PRIMITIVE_U16,
        PRIMITIVE_U32,
        PRIMITIVE_U64,
        PRIMITIVE_I8,
        PRIMITIVE_I16,
        PRIMITIVE_I32,
        PRIMITIVE_I64,
        PRIMITIVE_F16,
        PRIMITIVE_F32,
        PRIMITIVE_F64,
        UTF8,
        BINARY,
        STRUCT,
        LIST,
        EXTENSION,
        DECIMAL,
        FIXED_SIZE_LIST;

        public static Variant from(byte variant) {
            switch (variant) {
                case 0:
                    return NULL;
                case 1:
                    return BOOL;
                case 2:
                    return PRIMITIVE_U8;
                case 3:
                    return PRIMITIVE_U16;
                case 4:
                    return PRIMITIVE_U32;
                case 5:
                    return PRIMITIVE_U64;
                case 6:
                    return PRIMITIVE_I8;
                case 7:
                    return PRIMITIVE_I16;
                case 8:
                    return PRIMITIVE_I32;
                case 9:
                    return PRIMITIVE_I64;
                case 10:
                    return PRIMITIVE_F16;
                case 11:
                    return PRIMITIVE_F32;
                case 12:
                    return PRIMITIVE_F64;
                case 13:
                    return UTF8;
                case 14:
                    return BINARY;
                case 15:
                    return STRUCT;
                case 16:
                    return LIST;
                case 17:
                    return EXTENSION;
                case 18:
                    return DECIMAL;
                case 19:
                    return FIXED_SIZE_LIST;
                default:
                    throw new IllegalArgumentException("Unknown DType variant: " + variant);
            }
        }
    }
}
