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

package org.apache.paimon.data.variant;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;

import static org.apache.paimon.data.variant.GenericVariantUtil.BINARY_SEARCH_THRESHOLD;
import static org.apache.paimon.data.variant.GenericVariantUtil.SIZE_LIMIT;
import static org.apache.paimon.data.variant.GenericVariantUtil.Type;
import static org.apache.paimon.data.variant.GenericVariantUtil.VERSION;
import static org.apache.paimon.data.variant.GenericVariantUtil.VERSION_MASK;
import static org.apache.paimon.data.variant.GenericVariantUtil.checkIndex;
import static org.apache.paimon.data.variant.GenericVariantUtil.getMetadataKey;
import static org.apache.paimon.data.variant.GenericVariantUtil.handleArray;
import static org.apache.paimon.data.variant.GenericVariantUtil.handleObject;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;
import static org.apache.paimon.data.variant.GenericVariantUtil.readUnsigned;
import static org.apache.paimon.data.variant.GenericVariantUtil.valueSize;
import static org.apache.paimon.data.variant.GenericVariantUtil.variantConstructorSizeLimit;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An internal data structure implementing {@link Variant}. */
public final class GenericVariant implements Variant {

    private final byte[] value;
    private final byte[] metadata;
    // The variant value doesn't use the whole `value` binary, but starts from its `pos` index and
    // spans a size of `valueSize(value, pos)`. This design avoids frequent copies of the value
    // binary when reading a sub-variant in the array/object element.
    private final int pos;

    public GenericVariant(byte[] value, byte[] metadata) {
        this(value, metadata, 0);
    }

    private GenericVariant(byte[] value, byte[] metadata, int pos) {
        this.value = value;
        this.metadata = metadata;
        this.pos = pos;
        // There is currently only one allowed version.
        if (metadata.length < 1 || (metadata[0] & VERSION_MASK) != VERSION) {
            throw malformedVariant();
        }
        // Don't attempt to use a Variant larger than 16 MiB. We'll never produce one, and it risks
        // memory instability.
        if (metadata.length > SIZE_LIMIT || value.length > SIZE_LIMIT) {
            throw variantConstructorSizeLimit();
        }
    }

    @Override
    public byte[] value() {
        if (pos == 0) {
            return value;
        }
        int size = valueSize(value, pos);
        checkIndex(pos + size - 1, value.length);
        return Arrays.copyOfRange(value, pos, pos + size);
    }

    public byte[] rawValue() {
        return value;
    }

    @Override
    public byte[] metadata() {
        return metadata;
    }

    public int pos() {
        return pos;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericVariant that = (GenericVariant) o;
        return pos == that.pos
                && Objects.deepEquals(value, that.value)
                && Objects.deepEquals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.hashCode(metadata), pos);
    }

    public static Variant fromJson(String json) {
        try {
            return GenericVariantBuilder.parseJson(json, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toJson() {
        return toJson(ZoneOffset.UTC);
    }

    // Stringify the variant in JSON format.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public String toJson(ZoneId zoneId) {
        StringBuilder sb = new StringBuilder();
        toJsonImpl(value, metadata, pos, sb, zoneId);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public Object variantGet(String path) {
        GenericVariant v = this;
        PathSegment[] parsedPath = PathSegment.parse(path);
        for (PathSegment pathSegment : parsedPath) {
            if (pathSegment.isKey() && v.getType() == Type.OBJECT) {
                v = v.getFieldByKey(pathSegment.getKey());
            } else if (pathSegment.isIndex() && v.getType() == Type.ARRAY) {
                v = v.getElementAtIndex(pathSegment.getIndex());
            } else {
                return null;
            }
        }

        switch (v.getType()) {
            case OBJECT:
            case ARRAY:
                return v.toJson();
            case STRING:
                return v.getString();
            case LONG:
                return v.getLong();
            case DOUBLE:
                return v.getDouble();
            case DECIMAL:
                return v.getDecimal();
            case BOOLEAN:
                return v.getBoolean();
            case NULL:
                return null;
            default:
                // todo: support other types
                throw new IllegalArgumentException("Unsupported type: " + v.getType());
        }
    }

    // Get a boolean value from the variant.
    public boolean getBoolean() {
        return GenericVariantUtil.getBoolean(value, pos);
    }

    // Get a long value from the variant.
    public long getLong() {
        return GenericVariantUtil.getLong(value, pos);
    }

    // Get a double value from the variant.
    public double getDouble() {
        return GenericVariantUtil.getDouble(value, pos);
    }

    // Get a decimal value from the variant.
    public BigDecimal getDecimal() {
        return GenericVariantUtil.getDecimal(value, pos);
    }

    // Get a float value from the variant.
    public float getFloat() {
        return GenericVariantUtil.getFloat(value, pos);
    }

    // Get a binary value from the variant.
    public byte[] getBinary() {
        return GenericVariantUtil.getBinary(value, pos);
    }

    // Get a string value from the variant.
    public String getString() {
        return GenericVariantUtil.getString(value, pos);
    }

    // Get the type info bits from a variant value.
    public int getTypeInfo() {
        return GenericVariantUtil.getTypeInfo(value, pos);
    }

    // Get the value type of the variant.
    public Type getType() {
        return GenericVariantUtil.getType(value, pos);
    }

    // Get the number of object fields in the variant.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public int objectSize() {
        return handleObject(
                value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> size);
    }

    // Find the field value whose key is equal to `key`. Return null if the key is not found.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public GenericVariant getFieldByKey(String key) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    // Use linear search for a short list. Switch to binary search when the length
                    // reaches `BINARY_SEARCH_THRESHOLD`.
                    if (size < BINARY_SEARCH_THRESHOLD) {
                        for (int i = 0; i < size; ++i) {
                            int id = readUnsigned(value, idStart + idSize * i, idSize);
                            if (key.equals(getMetadataKey(metadata, id))) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                return new GenericVariant(value, metadata, dataStart + offset);
                            }
                        }
                    } else {
                        int low = 0;
                        int high = size - 1;
                        while (low <= high) {
                            // Use unsigned right shift to compute the middle of `low` and `high`.
                            // This is not only a performance optimization, because it can properly
                            // handle the case where `low + high` overflows int.
                            int mid = (low + high) >>> 1;
                            int id = readUnsigned(value, idStart + idSize * mid, idSize);
                            int cmp = getMetadataKey(metadata, id).compareTo(key);
                            if (cmp < 0) {
                                low = mid + 1;
                            } else if (cmp > 0) {
                                high = mid - 1;
                            } else {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * mid, offsetSize);
                                return new GenericVariant(value, metadata, dataStart + offset);
                            }
                        }
                    }
                    return null;
                });
    }

    /** Variant object field. */
    public static final class ObjectField {
        public final String key;
        public final Variant value;

        public ObjectField(String key, Variant value) {
            this.key = key;
            this.value = value;
        }
    }

    // Get the object field at the `index` slot. Return null if `index` is out of the bound of
    // `[0, objectSize())`.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public ObjectField getFieldAtIndex(int index) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int id = readUnsigned(value, idStart + idSize * index, idSize);
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    String key = getMetadataKey(metadata, id);
                    Variant v = new GenericVariant(value, metadata, dataStart + offset);
                    return new ObjectField(key, v);
                });
    }

    // Get the dictionary ID for the object field at the `index` slot. Throws malformedVariant if
    // `index` is out of the bound of `[0, objectSize())`.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public int getDictionaryIdAtIndex(int index) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        throw malformedVariant();
                    }
                    return readUnsigned(value, idStart + idSize * index, idSize);
                });
    }

    // Get the number of array elements in the variant.
    // It is only legal to call it when `getType()` is `Type.ARRAY`.
    public int arraySize() {
        return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> size);
    }

    // Get the array element at the `index` slot. Return null if `index` is out of the bound of
    // `[0, arraySize())`.
    // It is only legal to call it when `getType()` is `Type.ARRAY`.
    public GenericVariant getElementAtIndex(int index) {
        return handleArray(
                value,
                pos,
                (size, offsetSize, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    return new GenericVariant(value, metadata, dataStart + offset);
                });
    }

    // Escape a string so that it can be pasted into JSON structure.
    // For example, if `str` only contains a new-line character, then the result content is "\n"
    // (4 characters).
    private static String escapeJson(String str) {
        try (CharArrayWriter writer = new CharArrayWriter();
                JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
            gen.writeString(str);
            gen.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // A simplified and more performant version of `sb.append(escapeJson(str))`. It is used when we
    // know `str` doesn't contain any special character that needs escaping.
    static void appendQuoted(StringBuilder sb, String str) {
        sb.append('"');
        sb.append(str);
        sb.append('"');
    }

    private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter(Locale.US);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(TIMESTAMP_NTZ_FORMATTER)
                    .appendOffset("+HH:MM", "+00:00")
                    .toFormatter(Locale.US);

    private static Instant microsToInstant(long timestamp) {
        return Instant.EPOCH.plus(timestamp, ChronoUnit.MICROS);
    }

    private static void toJsonImpl(
            byte[] value, byte[] metadata, int pos, StringBuilder sb, ZoneId zoneId) {
        switch (GenericVariantUtil.getType(value, pos)) {
            case OBJECT:
                handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                            sb.append('{');
                            for (int i = 0; i < size; ++i) {
                                int id = readUnsigned(value, idStart + idSize * i, idSize);
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                sb.append(escapeJson(getMetadataKey(metadata, id)));
                                sb.append(':');
                                toJsonImpl(value, metadata, elementPos, sb, zoneId);
                            }
                            sb.append('}');
                            return null;
                        });
                break;
            case ARRAY:
                handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) -> {
                            sb.append('[');
                            for (int i = 0; i < size; ++i) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                toJsonImpl(value, metadata, elementPos, sb, zoneId);
                            }
                            sb.append(']');
                            return null;
                        });
                break;
            case NULL:
                sb.append("null");
                break;
            case BOOLEAN:
                sb.append(GenericVariantUtil.getBoolean(value, pos));
                break;
            case LONG:
                sb.append(GenericVariantUtil.getLong(value, pos));
                break;
            case STRING:
                sb.append(escapeJson(GenericVariantUtil.getString(value, pos)));
                break;
            case DOUBLE:
                sb.append(GenericVariantUtil.getDouble(value, pos));
                break;
            case DECIMAL:
                sb.append(GenericVariantUtil.getDecimal(value, pos).toPlainString());
                break;
            case DATE:
                appendQuoted(
                        sb,
                        LocalDate.ofEpochDay((int) GenericVariantUtil.getLong(value, pos))
                                .toString());
                break;
            case TIMESTAMP:
                appendQuoted(
                        sb,
                        TIMESTAMP_FORMATTER.format(
                                microsToInstant(GenericVariantUtil.getLong(value, pos))
                                        .atZone(zoneId)));
                break;
            case TIMESTAMP_NTZ:
                appendQuoted(
                        sb,
                        TIMESTAMP_NTZ_FORMATTER.format(
                                microsToInstant(GenericVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case FLOAT:
                sb.append(GenericVariantUtil.getFloat(value, pos));
                break;
            case BINARY:
                appendQuoted(
                        sb,
                        Base64.getEncoder()
                                .encodeToString(GenericVariantUtil.getBinary(value, pos)));
                break;
        }
    }
}
