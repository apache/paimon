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

/**
 * Vortex logical type interface representing the schema and metadata for array data.
 *
 * <p>DType defines the logical type system used by Vortex arrays, including primitive types,
 * complex types like structs and lists, and temporal types with associated metadata.
 * This interface provides methods to inspect type variants, nullability, temporal properties,
 * decimal precision, and structural information for complex types.
 *
 * <p>Implementations of this interface are typically obtained from Vortex arrays and
 * should be properly closed when no longer needed to free native resources.
 */
public interface DType extends AutoCloseable {

    /**
     * Returns the variant of this data type.
     *
     * @return the {@link Variant} enum value representing the specific type category
     */
    Variant getVariant();

    /**
     * Checks if this data type allows null values.
     *
     * @return {@code true} if the type is nullable, {@code false} otherwise
     */
    boolean isNullable();

    /**
     * Get the field names for a STRUCT type.
     */
    List<String> getFieldNames();

    /**
     * Get the field types for a STRUCT type.
     */
    List<DType> getFieldTypes();

    /**
     * Get the element type for a LIST or FIXED_SIZE_LIST type.
     */
    DType getElementType();

    /**
     * Get the fixed size for a FIXED_SIZE_LIST type.
     */
    int getFixedSizeListSize();

    /**
     * Checks if this data type represents a date.
     *
     * @return {@code true} if this is a date type, {@code false} otherwise
     */
    boolean isDate();

    /**
     * Checks if this data type represents a time.
     *
     * @return {@code true} if this is a time type, {@code false} otherwise
     */
    boolean isTime();

    /**
     * Checks if this data type represents a timestamp.
     *
     * @return {@code true} if this is a timestamp type, {@code false} otherwise
     */
    boolean isTimestamp();

    /**
     * Returns the time unit for temporal data types.
     *
     * @return the {@link TimeUnit} for this temporal type
     * @throws IllegalStateException if this is not a temporal type
     */
    TimeUnit getTimeUnit();

    /**
     * Returns the timezone for timestamp data types.
     *
     * @return an {@link Optional} containing the timezone string if present,
     * or empty if no timezone is specified
     */
    Optional<String> getTimeZone();

    /**
     * Checks if this data type represents a decimal number.
     *
     * @return {@code true} if this is a decimal type, {@code false} otherwise
     */
    boolean isDecimal();

    /**
     * Returns the precision for decimal data types.
     *
     * @return the precision (total number of digits) for decimal types
     * @throws IllegalStateException if this is not a decimal type
     */
    int getPrecision();

    /**
     * Returns the scale for decimal data types.
     *
     * @return the scale (number of digits after the decimal point) for decimal types
     * @throws IllegalStateException if this is not a decimal type
     */
    byte getScale();

    /**
     * Closes this DType and releases any associated native resources.
     *
     * <p>After calling this method, the DType should not be used again.
     * This method is idempotent and can be called multiple times safely.
     */
    @Override
    void close();

    /**
     * Create a new signed INT8 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newByte(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newByte(isNullable), true);
    }

    /**
     * Create a new signed INT16 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newShort(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newShort(isNullable), true);
    }

    /**
     * Create a new signed INT32 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newInt(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newInt(isNullable), true);
    }

    /**
     * Create a new signed INT64 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newLong(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newLong(isNullable), true);
    }

    /**
     * Create a new signed FLOAT32 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newFloat(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newFloat(isNullable), true);
    }

    /**
     * Create a new signed FLOAT64 data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newDouble(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newDouble(isNullable), true);
    }

    /**
     * Create a new Decimal data type.
     *
     * @param precision  Decimal values precision
     * @param scale      Decimal values scale
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newDecimal(int precision, int scale, boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newDecimal(precision, scale, isNullable), true);
    }

    /**
     * Create a new UTF-8 string data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newUtf8(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newUtf8(isNullable), true);
    }

    /**
     * Create a new Binary data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newBinary(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newBinary(isNullable), true);
    }

    /**
     * Create a new Binary data type.
     *
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newBool(boolean isNullable) {
        return new JNIDType(NativeDTypeMethods.newBool(isNullable), true);
    }

    /**
     * Create a new List data type.
     *
     * @param element    DType of the list elements
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newList(DType element, boolean isNullable) {
        // Get the pointer
        JNIDType jniType = (JNIDType) element;
        return new JNIDType(NativeDTypeMethods.newList(jniType.getPointer(), isNullable), true);
    }

    /**
     * Create a new FixedSizeList data type.
     *
     * @param element    DType of the list elements
     * @param size       The fixed size of each list
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newFixedSizeList(DType element, int size, boolean isNullable) {
        JNIDType jniType = (JNIDType) element;
        return new JNIDType(NativeDTypeMethods.newFixedSizeList(jniType.getPointer(), size, isNullable), true);
    }

    /**
     * Create a new Struct data type.
     *
     * @param fieldNames Name of each of the fields
     * @param fieldTypes DType for each field
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newStruct(String[] fieldNames, DType[] fieldTypes, boolean isNullable) {
        long[] ptrs = new long[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            ptrs[i] = ((JNIDType) fieldTypes[i]).getPointer();
        }
        return new JNIDType(NativeDTypeMethods.newStruct(fieldNames, ptrs, isNullable), true);
    }

    /**
     * Create a new Timestamp data type.
     *
     * @param unit       Time unit for the timestamp values
     * @param timeZone   Optional time zone for the timestamp values
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newTimestamp(TimeUnit unit, Optional<String> timeZone, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(NativeDTypeMethods.newTimestamp(timeUnit, timeZone.orElse(null), isNullable), true);
    }

    /**
     * Create a new Date data type.
     *
     * @param unit       Time unit for the value
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newDate(TimeUnit unit, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(NativeDTypeMethods.newDate(timeUnit, isNullable), true);
    }

    /**
     * Create a new Time data type.
     *
     * @param unit       Time unit for the value
     * @param isNullable True if the values can be null
     * @return The new DType instance, allocated in native heap memory
     */
    static DType newTime(TimeUnit unit, boolean isNullable) {
        byte timeUnit = unit.asByte();
        return new JNIDType(NativeDTypeMethods.newTime(timeUnit, isNullable), true);
    }

    /**
     * Enumeration of time units supported by Vortex temporal data types.
     *
     * <p>Time units define the granularity of temporal values and are used
     * by date, time, and timestamp data types to specify their precision.
     */
    enum TimeUnit {
        /**
         * Nanosecond precision (10^-9 seconds)
         */
        NANOSECONDS,

        /**
         * Microsecond precision (10^-6 seconds)
         */
        MICROSECONDS,

        /**
         * Millisecond precision (10^-3 seconds)
         */
        MILLISECONDS,

        /**
         * Second precision
         */
        SECONDS,

        /**
         * Day precision (24-hour periods)
         */
        DAYS,
        ;

        /**
         * Converts a byte value to the corresponding TimeUnit enum.
         *
         * @param unit the byte value representing the time unit (0-4)
         * @return the corresponding {@link TimeUnit} enum value
         * @throws RuntimeException if the unit value is not recognized
         */
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

        /**
         * Get the byte value of this TimeUnit.
         */
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

    /**
     * Enumeration of all supported data type variants in Vortex.
     *
     * <p>Each variant represents a different category of data type, from primitive
     * numeric types to complex structured types. This enum provides a way to
     * categorize and identify the specific type of data stored in a Vortex array.
     */
    enum Variant {
        /**
         * Null type representing absence of value
         */
        NULL,

        /**
         * Boolean type for true/false values
         */
        BOOL,

        /**
         * Unsigned 8-bit integer type
         */
        PRIMITIVE_U8,

        /**
         * Unsigned 16-bit integer type
         */
        PRIMITIVE_U16,

        /**
         * Unsigned 32-bit integer type
         */
        PRIMITIVE_U32,

        /**
         * Unsigned 64-bit integer type
         */
        PRIMITIVE_U64,

        /**
         * Signed 8-bit integer type
         */
        PRIMITIVE_I8,

        /**
         * Signed 16-bit integer type
         */
        PRIMITIVE_I16,

        /**
         * Signed 32-bit integer type
         */
        PRIMITIVE_I32,

        /**
         * Signed 64-bit integer type
         */
        PRIMITIVE_I64,

        /**
         * 16-bit floating point type
         */
        PRIMITIVE_F16,

        /**
         * 32-bit floating point type
         */
        PRIMITIVE_F32,

        /**
         * 64-bit floating point type
         */
        PRIMITIVE_F64,

        /**
         * UTF-8 encoded string type
         */
        UTF8,

        /**
         * Binary data type for arbitrary byte sequences
         */
        BINARY,

        /**
         * Structured type containing named fields
         */
        STRUCT,

        /**
         * List type containing elements of a single type
         */
        LIST,

        /**
         * Extension type for custom or domain-specific types
         */
        EXTENSION,

        /**
         * Decimal type for precise numeric values
         */
        DECIMAL,

        /**
         * Fixed-size list type containing a fixed number of elements of a single type
         */
        FIXED_SIZE_LIST,
        ;

        /**
         * Converts a byte value to the corresponding Variant enum.
         *
         * @param variant the byte value representing the variant (0-19)
         * @return the corresponding {@link Variant} enum value
         * @throws RuntimeException if the variant value is not recognized
         */
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
