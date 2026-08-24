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

import org.apache.paimon.data.variant.VariantPathSegment.ArrayExtraction;
import org.apache.paimon.data.variant.VariantPathSegment.ObjectExtraction;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

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
import static org.apache.paimon.data.variant.GenericVariantUtil.slice;
import static org.apache.paimon.data.variant.GenericVariantUtil.valueSize;
import static org.apache.paimon.data.variant.GenericVariantUtil.variantConstructorSizeLimit;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An internal data structure implementing {@link Variant}. */
public final class GenericVariant implements Variant, Serializable {

    private static final long serialVersionUID = 2L;

    private final ByteBuffer value;
    private final ByteBuffer metadata;

    public GenericVariant(byte[] value, byte[] metadata) {
        this(ByteBuffer.wrap(value), ByteBuffer.wrap(metadata));
    }

    /** Creates a Variant backed by the bytes between each buffer's position and limit. */
    public GenericVariant(ByteBuffer value, ByteBuffer metadata) {
        ByteBuffer normalizedValue = normalize(value);
        this.metadata = normalize(metadata);
        validate(normalizedValue, this.metadata);
        int size = valueSize(normalizedValue, 0);
        checkIndex(size - 1, normalizedValue.remaining());
        this.value = slice(normalizedValue, 0, size);
    }

    /** Returns this variant as a GenericVariant without copying its serialized buffers. */
    static GenericVariant fromVariant(Variant variant) {
        return variant instanceof GenericVariant
                ? (GenericVariant) variant
                : new GenericVariant(variant.valueBuffer(), variant.metadataBuffer());
    }

    private GenericVariant(ByteBuffer value, ByteBuffer metadata, int offset, int size) {
        this.value = slice(value, offset, size);
        this.metadata = metadata;
    }

    private static void validate(ByteBuffer value, ByteBuffer metadata) {
        // There is currently only one allowed version.
        if (metadata.remaining() < 1
                || (GenericVariantUtil.getByte(metadata, 0) & VERSION_MASK) != VERSION) {
            throw malformedVariant();
        }
        // Don't attempt to use a Variant larger than 128 MiB. We'll never produce one, and it risks
        // memory instability.
        if (metadata.remaining() > SIZE_LIMIT || value.remaining() > SIZE_LIMIT) {
            throw variantConstructorSizeLimit();
        }
    }

    private static ByteBuffer normalize(ByteBuffer buffer) {
        return buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public byte[] value() {
        return toByteArray(value, 0, value.remaining());
    }

    @Override
    public ByteBuffer valueBuffer() {
        return value.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    /** @deprecated Use {@link #value()} for the current logical value. */
    @Deprecated
    public byte[] rawValue() {
        return value();
    }

    @Override
    public byte[] metadata() {
        return toByteArray(metadata, 0, metadata.remaining());
    }

    @Override
    public ByteBuffer metadataBuffer() {
        return metadata.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte[] toByteArray(ByteBuffer buffer, int offset, int length) {
        if (offset == 0
                && buffer.hasArray()
                && buffer.position() == 0
                && buffer.arrayOffset() == 0
                && length == buffer.array().length) {
            return buffer.array();
        }
        return GenericVariantUtil.copyBytes(buffer, offset, length);
    }

    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    private void readObject(ObjectInputStream in) throws InvalidObjectException {
        throw new InvalidObjectException("Serialization proxy required.");
    }

    private static final class SerializationProxy implements Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] value;
        private final byte[] metadata;

        private SerializationProxy(GenericVariant variant) {
            this.value = GenericVariantUtil.copyBytes(variant.value, 0, variant.value.remaining());
            this.metadata =
                    GenericVariantUtil.copyBytes(variant.metadata, 0, variant.metadata.remaining());
        }

        private Object readResolve() {
            return new GenericVariant(value, metadata);
        }
    }

    /** @deprecated Each GenericVariant now stores a bounded value buffer starting at position 0. */
    @Deprecated
    public int pos() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericVariant that = (GenericVariant) o;
        return value.equals(that.value) && metadata.equals(that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, metadata);
    }

    public static GenericVariant fromJson(String json) {
        try {
            return GenericVariantBuilder.parseJson(json, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Stringify the variant in JSON format.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    @Override
    public String toJson(ZoneId zoneId) {
        StringBuilder sb = new StringBuilder();
        toJsonImpl(value, metadata, 0, sb, zoneId);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    public Object variantGet(String path, DataType dataType, VariantCastArgs castArgs) {
        GenericVariant v = this;
        VariantPathSegment[] parsedPath = VariantPathSegment.parse(path);
        for (VariantPathSegment pathSegment : parsedPath) {
            if (pathSegment instanceof ObjectExtraction && v.getType() == Type.OBJECT) {
                v = v.getFieldByKey(((ObjectExtraction) pathSegment).getKey());
            } else if (pathSegment instanceof ArrayExtraction && v.getType() == Type.ARRAY) {
                v = v.getElementAtIndex(((ArrayExtraction) pathSegment).getIndex());
            } else {
                return null;
            }
        }
        return VariantGet.cast(v, dataType, castArgs);
    }

    @Override
    public long sizeInBytes() {
        return metadata.remaining() + value.remaining();
    }

    @Override
    public Variant copy() {
        return new GenericVariant(
                ByteBuffer.wrap(GenericVariantUtil.copyBytes(value, 0, value.remaining())),
                ByteBuffer.wrap(GenericVariantUtil.copyBytes(metadata, 0, metadata.remaining())));
    }

    // Get a boolean value from the variant.
    public boolean getBoolean() {
        return GenericVariantUtil.getBoolean(value, 0);
    }

    // Get a long value from the variant.
    public long getLong() {
        return GenericVariantUtil.getLong(value, 0);
    }

    // Get a double value from the variant.
    public double getDouble() {
        return GenericVariantUtil.getDouble(value, 0);
    }

    // Get a decimal value from the variant.
    public BigDecimal getDecimal() {
        return GenericVariantUtil.getDecimal(value, 0);
    }

    // Get a float value from the variant.
    public float getFloat() {
        return GenericVariantUtil.getFloat(value, 0);
    }

    // Get a binary value from the variant.
    public byte[] getBinary() {
        return GenericVariantUtil.getBinary(value, 0);
    }

    // Get a string value from the variant.
    public String getString() {
        return GenericVariantUtil.getString(value, 0);
    }

    // Get the type info bits from a variant value.
    public int getTypeInfo() {
        return GenericVariantUtil.getTypeInfo(value, 0);
    }

    // Get the value type of the variant.
    public Type getType() {
        return GenericVariantUtil.getType(value, 0);
    }

    // Get a UUID value from the variant.
    public UUID getUuid() {
        return GenericVariantUtil.getUuid(value, 0);
    }

    // Get the number of object fields in the variant.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public int objectSize() {
        return handleObject(
                value, 0, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> size);
    }

    // Find the field value whose key is equal to `key`. Return null if the key is not found.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public GenericVariant getFieldByKey(String key) {
        return handleObject(
                value,
                0,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    MetadataKeyLookup keyLookup = new MetadataKeyLookup(metadata, key);
                    // Use linear search for a short list. Switch to binary search when the length
                    // reaches `BINARY_SEARCH_THRESHOLD`.
                    if (size < BINARY_SEARCH_THRESHOLD) {
                        for (int i = 0; i < size; ++i) {
                            int id = readUnsigned(value, idStart + idSize * i, idSize);
                            if (keyLookup.compareUtf8(id) == 0) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                return variantAt(dataStart + offset);
                            }
                        }
                    } else {
                        GenericVariant result =
                                binarySearchObjectField(
                                        size,
                                        idSize,
                                        offsetSize,
                                        idStart,
                                        offsetStart,
                                        dataStart,
                                        keyLookup,
                                        true);
                        return result != null
                                ? result
                                : binarySearchObjectField(
                                        size,
                                        idSize,
                                        offsetSize,
                                        idStart,
                                        offsetStart,
                                        dataStart,
                                        keyLookup,
                                        false);
                    }
                    return null;
                });
    }

    private GenericVariant binarySearchObjectField(
            int size,
            int idSize,
            int offsetSize,
            int idStart,
            int offsetStart,
            int dataStart,
            MetadataKeyLookup keyLookup,
            boolean utf8Order) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int id = readUnsigned(value, idStart + idSize * mid, idSize);
            int comparison =
                    utf8Order
                            ? keyLookup.compareUtf8(id)
                            : getMetadataKey(metadata, id).compareTo(keyLookup.key);
            if (comparison < 0) {
                low = mid + 1;
            } else if (comparison > 0) {
                high = mid - 1;
            } else {
                int offset = readUnsigned(value, offsetStart + offsetSize * mid, offsetSize);
                return variantAt(dataStart + offset);
            }
        }
        return null;
    }

    private static final class MetadataKeyLookup {
        private final ByteBuffer metadata;
        private final String key;
        private final byte[] utf8Key;

        private MetadataKeyLookup(ByteBuffer metadata, String key) {
            this.metadata = metadata;
            this.key = key;
            this.utf8Key = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        private int compareUtf8(int id) {
            return GenericVariantUtil.compareMetadataKey(metadata, id, utf8Key);
        }
    }

    /** Variant object field. */
    public static final class ObjectField {
        public final String key;
        public final GenericVariant value;

        public ObjectField(String key, GenericVariant value) {
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
                0,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int id = readUnsigned(value, idStart + idSize * index, idSize);
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    String key = getMetadataKey(metadata, id);
                    GenericVariant v = variantAt(dataStart + offset);
                    return new ObjectField(key, v);
                });
    }

    // Get the dictionary ID for the object field at the `index` slot. Throws malformedVariant if
    // `index` is out of the bound of `[0, objectSize())`.
    // It is only legal to call it when `getType()` is `Type.OBJECT`.
    public int getDictionaryIdAtIndex(int index) {
        return handleObject(
                value,
                0,
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
        return handleArray(value, 0, (size, offsetSize, offsetStart, dataStart) -> size);
    }

    // Get the array element at the `index` slot. Return null if `index` is out of the bound of
    // `[0, arraySize())`.
    // It is only legal to call it when `getType()` is `Type.ARRAY`.
    public GenericVariant getElementAtIndex(int index) {
        return handleArray(
                value,
                0,
                (size, offsetSize, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    return variantAt(dataStart + offset);
                });
    }

    private GenericVariant variantAt(int offset) {
        int size = valueSize(value, offset);
        checkIndex(offset + size - 1, value.remaining());
        return new GenericVariant(value, metadata, offset, size);
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
            ByteBuffer value, ByteBuffer metadata, int pos, StringBuilder sb, ZoneId zoneId) {
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
                {
                    double d = GenericVariantUtil.getDouble(value, pos);
                    if (Double.isFinite(d)) {
                        sb.append(d);
                    } else {
                        appendQuoted(sb, Double.toString(d));
                    }
                    break;
                }
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
                {
                    float f = GenericVariantUtil.getFloat(value, pos);
                    if (Float.isFinite(f)) {
                        sb.append(f);
                    } else {
                        appendQuoted(sb, Float.toString(f));
                    }
                    break;
                }
            case BINARY:
                appendQuoted(
                        sb,
                        Base64.getEncoder()
                                .encodeToString(GenericVariantUtil.getBinary(value, pos)));
                break;
            case UUID:
                appendQuoted(sb, GenericVariantUtil.getUuid(value, pos).toString());
                break;
        }
    }
}
