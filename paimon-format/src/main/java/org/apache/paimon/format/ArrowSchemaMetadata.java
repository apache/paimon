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

package org.apache.paimon.format;

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;

import com.google.flatbuffers.FlatBufferBuilder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Minimal Arrow IPC schema metadata encoder and decoder used by format metadata.
 *
 * <p>NOTE: The RowType-to-Arrow-field conversion in this class is copied from {@code
 * org.apache.paimon.arrow.ArrowUtils} and must be kept in sync with that class. The Arrow IPC
 * FlatBuffers layout, enum values, and defaults are adapted from Apache Arrow Java / Arrow format
 * generated classes. This class implements only the subset needed by Paimon field metadata so that
 * {@code paimon-format} can stay compatible with {@code ARROW:schema} without depending on the
 * Arrow runtime.
 */
class ArrowSchemaMetadata {

    private static final String LIST_DATA_VECTOR_NAME = "$data$";
    private static final String MAP_DATA_VECTOR_NAME = "entries";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";

    private static final int CONTINUATION_TOKEN = -1;

    private static final short METADATA_VERSION_V5 = 4;
    private static final short ENDIANNESS_LITTLE = 0;

    private static final byte MESSAGE_HEADER_SCHEMA = 1;

    private static final byte TYPE_NULL = 1;
    private static final byte TYPE_INT = 2;
    private static final byte TYPE_FLOATING_POINT = 3;
    private static final byte TYPE_BINARY = 4;
    private static final byte TYPE_UTF8 = 5;
    private static final byte TYPE_BOOL = 6;
    private static final byte TYPE_DECIMAL = 7;
    private static final byte TYPE_DATE = 8;
    private static final byte TYPE_TIME = 9;
    private static final byte TYPE_TIMESTAMP = 10;
    private static final byte TYPE_LIST = 12;
    private static final byte TYPE_STRUCT = 13;
    private static final byte TYPE_FIXED_SIZE_LIST = 16;
    private static final byte TYPE_MAP = 17;

    private static final short PRECISION_SINGLE = 1;
    private static final short PRECISION_DOUBLE = 2;

    private static final short DATE_UNIT_DAY = 0;
    private static final short DATE_UNIT_MILLISECOND = 1;

    private static final short TIME_UNIT_SECOND = 0;
    private static final short TIME_UNIT_MILLISECOND = 1;
    private static final short TIME_UNIT_MICROSECOND = 2;
    private static final short TIME_UNIT_NANOSECOND = 3;

    private static final int TIME_BIT_WIDTH_MILLISECOND = 32;
    private static final int DECIMAL_BIT_WIDTH_128 = 128;

    private ArrowSchemaMetadata() {}

    static byte[] serialize(
            RowType rowType, Map<String, Map<String, String>> fieldMetadata, String fieldIdKey) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int schemaOffset =
                buildSchema(
                        builder,
                        rowType.getFields().stream()
                                .map(
                                        field ->
                                                withMetadata(
                                                        toArrowField(
                                                                field.name(),
                                                                field.id(),
                                                                field.type(),
                                                                0,
                                                                fieldIdKey),
                                                        fieldMetadata.get(field.name())))
                                .collect(Collectors.toList()));
        int messageOffset =
                buildMessage(builder, MESSAGE_HEADER_SCHEMA, schemaOffset, 0L, METADATA_VERSION_V5);
        builder.finish(messageOffset);
        return withIpcMessagePrefix(builder.sizedByteArray());
    }

    /** Reads top-level field metadata from serialized Arrow IPC schema-message bytes. */
    static Map<String, Map<String, String>> readFieldMetadata(byte[] bytes) {
        ByteBuffer buffer = metadataBuffer(bytes);
        int messageTable = rootTable(buffer);
        if (readByte(buffer, messageTable, 1, (byte) 0) != MESSAGE_HEADER_SCHEMA) {
            return Collections.emptyMap();
        }
        int schemaTable = readTableUnion(buffer, messageTable, 2);
        if (schemaTable == 0) {
            return Collections.emptyMap();
        }
        int fieldsVector = readVector(buffer, schemaTable, 1);
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (int i = 0; i < vectorLength(buffer, fieldsVector); i++) {
            int fieldTable = vectorTable(buffer, fieldsVector, i);
            Map<String, String> metadata = readMetadata(buffer, fieldTable);
            if (!metadata.isEmpty()) {
                result.put(
                        readString(buffer, fieldTable, 0), Collections.unmodifiableMap(metadata));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static int buildSchema(FlatBufferBuilder builder, List<ArrowField> fields) {
        int[] fieldOffsets = new int[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldOffsets[i] = buildField(builder, fields.get(i));
        }
        int fieldsVector = createVectorOfTables(builder, fieldOffsets);

        builder.startTable(4);
        builder.addShort(0, ENDIANNESS_LITTLE, 0);
        builder.addOffset(1, fieldsVector, 0);
        return builder.endTable();
    }

    private static int buildField(FlatBufferBuilder builder, ArrowField field) {
        int name = builder.createString(field.name);
        int type = buildType(builder, field.type);
        int[] childOffsets = new int[field.children.size()];
        for (int i = 0; i < field.children.size(); i++) {
            childOffsets[i] = buildField(builder, field.children.get(i));
        }
        int children = createVectorOfTables(builder, childOffsets);
        int metadata = buildMetadata(builder, field.metadata);

        builder.startTable(7);
        builder.addOffset(0, name, 0);
        builder.addBoolean(1, field.nullable, false);
        builder.addByte(2, field.type.typeType, 0);
        builder.addOffset(3, type, 0);
        builder.addOffset(5, children, 0);
        builder.addOffset(6, metadata, 0);
        return builder.endTable();
    }

    private static int buildMetadata(FlatBufferBuilder builder, Map<String, String> metadata) {
        int[] offsets = new int[metadata.size()];
        int index = 0;
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            int key = builder.createString(entry.getKey());
            int value = builder.createString(entry.getValue());
            builder.startTable(2);
            builder.addOffset(0, key, 0);
            builder.addOffset(1, value, 0);
            offsets[index++] = builder.endTable();
        }
        return createVectorOfTables(builder, offsets);
    }

    private static int buildMessage(
            FlatBufferBuilder builder,
            byte headerType,
            int header,
            long bodyLength,
            short version) {
        builder.startTable(5);
        builder.addShort(0, version, 0);
        builder.addByte(1, headerType, 0);
        builder.addOffset(2, header, 0);
        builder.addLong(3, bodyLength, 0L);
        return builder.endTable();
    }

    private static int buildType(FlatBufferBuilder builder, ArrowTypeInfo type) {
        switch (type.typeType) {
            case TYPE_NULL:
            case TYPE_BINARY:
            case TYPE_UTF8:
            case TYPE_BOOL:
            case TYPE_LIST:
            case TYPE_STRUCT:
                builder.startTable(0);
                return builder.endTable();
            case TYPE_INT:
                builder.startTable(2);
                builder.addInt(0, type.bitWidth, 0);
                builder.addBoolean(1, type.signed, false);
                return builder.endTable();
            case TYPE_FLOATING_POINT:
                builder.startTable(1);
                builder.addShort(0, type.precision, 0);
                return builder.endTable();
            case TYPE_DECIMAL:
                builder.startTable(3);
                builder.addInt(0, type.precisionValue, 0);
                builder.addInt(1, type.scale, 0);
                builder.addInt(2, type.bitWidth, DECIMAL_BIT_WIDTH_128);
                return builder.endTable();
            case TYPE_DATE:
                builder.startTable(1);
                builder.addShort(0, DATE_UNIT_DAY, DATE_UNIT_MILLISECOND);
                return builder.endTable();
            case TYPE_TIME:
                builder.startTable(2);
                builder.addShort(0, type.unit, TIME_UNIT_MILLISECOND);
                builder.addInt(1, type.bitWidth, TIME_BIT_WIDTH_MILLISECOND);
                return builder.endTable();
            case TYPE_TIMESTAMP:
                int timezone = type.timezone == null ? 0 : builder.createString(type.timezone);
                builder.startTable(2);
                builder.addShort(0, type.unit, 0);
                if (timezone != 0) {
                    builder.addOffset(1, timezone, 0);
                }
                return builder.endTable();
            case TYPE_FIXED_SIZE_LIST:
                builder.startTable(1);
                builder.addInt(0, type.listSize, 0);
                return builder.endTable();
            case TYPE_MAP:
                builder.startTable(1);
                builder.addBoolean(0, false, false);
                return builder.endTable();
            default:
                throw new UnsupportedOperationException("Unsupported Arrow type " + type.typeType);
        }
    }

    private static int createVectorOfTables(FlatBufferBuilder builder, int[] offsets) {
        builder.startVector(4, offsets.length, 4);
        for (int i = offsets.length - 1; i >= 0; i--) {
            builder.addOffset(offsets[i]);
        }
        return builder.endVector();
    }

    private static byte[] withIpcMessagePrefix(byte[] metadata) {
        int paddedLength = align(metadata.length, 8);
        ByteBuffer buffer = ByteBuffer.allocate(8 + paddedLength).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(CONTINUATION_TOKEN);
        buffer.putInt(paddedLength);
        buffer.put(metadata);
        return buffer.array();
    }

    private static int align(int value, int alignment) {
        int remainder = value % alignment;
        return remainder == 0 ? value : value + alignment - remainder;
    }

    private static ByteBuffer metadataBuffer(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() >= 8 && buffer.getInt(0) == CONTINUATION_TOKEN) {
            int metadataLength = buffer.getInt(4);
            ByteBuffer metadata = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
            metadata.position(8);
            metadata.limit(8 + metadataLength);
            return metadata.slice().order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer;
    }

    private static int rootTable(ByteBuffer buffer) {
        return buffer.position() + buffer.getInt(buffer.position());
    }

    private static int vtable(ByteBuffer buffer, int table) {
        return table - buffer.getInt(table);
    }

    private static int fieldOffset(ByteBuffer buffer, int table, int field) {
        int vtable = vtable(buffer, table);
        int offset = 4 + field * 2;
        return offset < buffer.getShort(vtable) ? buffer.getShort(vtable + offset) : 0;
    }

    private static byte readByte(ByteBuffer buffer, int table, int field, byte defaultValue) {
        int offset = fieldOffset(buffer, table, field);
        return offset == 0 ? defaultValue : buffer.get(table + offset);
    }

    private static int readVector(ByteBuffer buffer, int table, int field) {
        int offset = fieldOffset(buffer, table, field);
        if (offset == 0) {
            return 0;
        }
        int vectorOffset = table + offset;
        return vectorOffset + buffer.getInt(vectorOffset);
    }

    private static int readTableUnion(ByteBuffer buffer, int table, int field) {
        int offset = fieldOffset(buffer, table, field);
        if (offset == 0) {
            return 0;
        }
        int unionOffset = table + offset;
        return unionOffset + buffer.getInt(unionOffset);
    }

    private static int vectorLength(ByteBuffer buffer, int vector) {
        return vector == 0 ? 0 : buffer.getInt(vector);
    }

    private static int vectorTable(ByteBuffer buffer, int vector, int index) {
        int element = vector + 4 + index * 4;
        return element + buffer.getInt(element);
    }

    private static String readString(ByteBuffer buffer, int table, int field) {
        int offset = fieldOffset(buffer, table, field);
        if (offset == 0) {
            return null;
        }
        int stringOffset = table + offset;
        int string = stringOffset + buffer.getInt(stringOffset);
        int length = buffer.getInt(string);
        byte[] bytes = new byte[length];
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(string + 4);
        duplicate.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static Map<String, String> readMetadata(ByteBuffer buffer, int fieldTable) {
        int metadataVector = readVector(buffer, fieldTable, 6);
        Map<String, String> metadata = new LinkedHashMap<>();
        for (int i = 0; i < vectorLength(buffer, metadataVector); i++) {
            int keyValue = vectorTable(buffer, metadataVector, i);
            metadata.put(readString(buffer, keyValue, 0), readString(buffer, keyValue, 1));
        }
        return metadata;
    }

    private static ArrowField withMetadata(ArrowField field, Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return field;
        }
        Map<String, String> result = new LinkedHashMap<>();
        result.putAll(metadata);
        result.putAll(field.metadata);
        return new ArrowField(field.name, field.nullable, field.type, field.children, result);
    }

    private static ArrowField toArrowField(
            String fieldName, int fieldId, DataType dataType, int depth, String fieldIdKey) {
        ArrowTypeInfo type = dataType.accept(ArrowFieldTypeVisitor.INSTANCE);
        Map<String, String> metadata = fieldIdMetadata(fieldId, fieldIdKey);
        List<ArrowField> children = Collections.emptyList();
        if (dataType instanceof ArrayType || dataType instanceof VectorType) {
            DataType elementType =
                    dataType instanceof VectorType
                            ? ((VectorType) dataType).getElementType()
                            : ((ArrayType) dataType).getElementType();
            ArrowField field =
                    toArrowField(
                            LIST_DATA_VECTOR_NAME, fieldId, elementType, depth + 1, fieldIdKey);
            if (fieldIdKey != null) {
                field =
                        field.withMetadata(
                                Collections.singletonMap(
                                        fieldIdKey,
                                        String.valueOf(
                                                SpecialFields.getArrayElementFieldId(
                                                        fieldId, depth + 1))));
            }
            children = Collections.singletonList(field);
        } else if (dataType instanceof MapType) {
            children =
                    Collections.singletonList(
                            toArrowMapEntryField(fieldId, (MapType) dataType, depth, fieldIdKey));
        } else if (dataType instanceof VariantType) {
            children =
                    Arrays.asList(
                            new ArrowField(
                                    Variant.VALUE,
                                    false,
                                    ArrowTypeInfo.simple(TYPE_BINARY),
                                    Collections.emptyList(),
                                    Collections.emptyMap()),
                            new ArrowField(
                                    Variant.METADATA,
                                    false,
                                    ArrowTypeInfo.simple(TYPE_BINARY),
                                    Collections.emptyList(),
                                    Collections.emptyMap()));
        } else if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            List<ArrowField> rowChildren = new ArrayList<>();
            for (DataField field : rowType.getFields()) {
                rowChildren.add(
                        toArrowField(field.name(), field.id(), field.type(), 0, fieldIdKey));
            }
            children = rowChildren;
        }
        return new ArrowField(fieldName, dataType.isNullable(), type, children, metadata);
    }

    private static ArrowField toArrowMapEntryField(
            int fieldId, MapType mapType, int depth, String fieldIdKey) {
        ArrowField keyField =
                toArrowField(
                        MAP_KEY_NAME,
                        fieldId,
                        mapType.getKeyType().notNull(),
                        depth + 1,
                        fieldIdKey);
        if (fieldIdKey != null) {
            keyField =
                    keyField.withMetadata(
                            Collections.singletonMap(
                                    fieldIdKey,
                                    String.valueOf(
                                            SpecialFields.getMapKeyFieldId(fieldId, depth + 1))));
        }

        ArrowField valueField =
                toArrowField(
                        MAP_VALUE_NAME, fieldId, mapType.getValueType(), depth + 1, fieldIdKey);
        if (fieldIdKey != null) {
            valueField =
                    valueField.withMetadata(
                            Collections.singletonMap(
                                    fieldIdKey,
                                    String.valueOf(
                                            SpecialFields.getMapValueFieldId(fieldId, depth + 1))));
        }

        return new ArrowField(
                MAP_DATA_VECTOR_NAME,
                false,
                ArrowTypeInfo.simple(TYPE_STRUCT),
                Arrays.asList(keyField, valueField),
                fieldIdMetadata(fieldId, fieldIdKey));
    }

    private static Map<String, String> fieldIdMetadata(int fieldId, String fieldIdKey) {
        return fieldIdKey == null
                ? Collections.emptyMap()
                : Collections.singletonMap(fieldIdKey, String.valueOf(fieldId));
    }

    private static class ArrowField {
        private final String name;
        private final boolean nullable;
        private final ArrowTypeInfo type;
        private final List<ArrowField> children;
        private final Map<String, String> metadata;

        private ArrowField(
                String name,
                boolean nullable,
                ArrowTypeInfo type,
                List<ArrowField> children,
                Map<String, String> metadata) {
            this.name = name;
            this.nullable = nullable;
            this.type = type;
            this.children = children;
            this.metadata = metadata;
        }

        private ArrowField withMetadata(Map<String, String> metadata) {
            return new ArrowField(name, nullable, type, children, metadata);
        }
    }

    private static class ArrowTypeInfo {
        private final byte typeType;
        private int bitWidth;
        private boolean signed;
        private short precision;
        private int precisionValue;
        private int scale;
        private short unit;
        private String timezone;
        private int listSize;

        private ArrowTypeInfo(byte typeType) {
            this.typeType = typeType;
        }

        private static ArrowTypeInfo simple(byte typeType) {
            return new ArrowTypeInfo(typeType);
        }
    }

    private static class ArrowFieldTypeVisitor implements DataTypeVisitor<ArrowTypeInfo> {

        private static final ArrowFieldTypeVisitor INSTANCE = new ArrowFieldTypeVisitor();

        @Override
        public ArrowTypeInfo visit(CharType charType) {
            return ArrowTypeInfo.simple(TYPE_UTF8);
        }

        @Override
        public ArrowTypeInfo visit(VarCharType varCharType) {
            return ArrowTypeInfo.simple(TYPE_UTF8);
        }

        @Override
        public ArrowTypeInfo visit(BooleanType booleanType) {
            return ArrowTypeInfo.simple(TYPE_BOOL);
        }

        @Override
        public ArrowTypeInfo visit(BinaryType binaryType) {
            return ArrowTypeInfo.simple(TYPE_BINARY);
        }

        @Override
        public ArrowTypeInfo visit(VarBinaryType varBinaryType) {
            return ArrowTypeInfo.simple(TYPE_BINARY);
        }

        @Override
        public ArrowTypeInfo visit(DecimalType decimalType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_DECIMAL);
            type.precisionValue = decimalType.getPrecision();
            type.scale = decimalType.getScale();
            type.bitWidth = DECIMAL_BIT_WIDTH_128;
            return type;
        }

        @Override
        public ArrowTypeInfo visit(TinyIntType tinyIntType) {
            return integer(8);
        }

        @Override
        public ArrowTypeInfo visit(SmallIntType smallIntType) {
            return integer(16);
        }

        @Override
        public ArrowTypeInfo visit(IntType intType) {
            return integer(32);
        }

        @Override
        public ArrowTypeInfo visit(BigIntType bigIntType) {
            return integer(64);
        }

        @Override
        public ArrowTypeInfo visit(FloatType floatType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_FLOATING_POINT);
            type.precision = PRECISION_SINGLE;
            return type;
        }

        @Override
        public ArrowTypeInfo visit(DoubleType doubleType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_FLOATING_POINT);
            type.precision = PRECISION_DOUBLE;
            return type;
        }

        @Override
        public ArrowTypeInfo visit(DateType dateType) {
            return ArrowTypeInfo.simple(TYPE_DATE);
        }

        @Override
        public ArrowTypeInfo visit(TimeType timeType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_TIME);
            type.unit = TIME_UNIT_MILLISECOND;
            type.bitWidth = TIME_BIT_WIDTH_MILLISECOND;
            return type;
        }

        @Override
        public ArrowTypeInfo visit(TimestampType timestampType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_TIMESTAMP);
            type.unit = getTimeUnit(timestampType.getPrecision());
            return type;
        }

        @Override
        public ArrowTypeInfo visit(LocalZonedTimestampType localZonedTimestampType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_TIMESTAMP);
            type.unit = getTimeUnit(localZonedTimestampType.getPrecision());
            type.timezone = "UTC";
            return type;
        }

        @Override
        public ArrowTypeInfo visit(VariantType variantType) {
            return ArrowTypeInfo.simple(TYPE_STRUCT);
        }

        @Override
        public ArrowTypeInfo visit(BlobType blobType) {
            throw new UnsupportedOperationException("Doesn't support BlobType.");
        }

        @Override
        public ArrowTypeInfo visit(ArrayType arrayType) {
            return ArrowTypeInfo.simple(TYPE_LIST);
        }

        @Override
        public ArrowTypeInfo visit(VectorType vectorType) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_FIXED_SIZE_LIST);
            type.listSize = vectorType.getLength();
            return type;
        }

        @Override
        public ArrowTypeInfo visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Doesn't support MultisetType.");
        }

        @Override
        public ArrowTypeInfo visit(MapType mapType) {
            return ArrowTypeInfo.simple(TYPE_MAP);
        }

        @Override
        public ArrowTypeInfo visit(RowType rowType) {
            return ArrowTypeInfo.simple(TYPE_STRUCT);
        }

        private ArrowTypeInfo integer(int bitWidth) {
            ArrowTypeInfo type = ArrowTypeInfo.simple(TYPE_INT);
            type.bitWidth = bitWidth;
            type.signed = true;
            return type;
        }

        private short getTimeUnit(int precision) {
            if (precision == 0) {
                return TIME_UNIT_SECOND;
            } else if (precision >= 1 && precision <= 3) {
                return TIME_UNIT_MILLISECOND;
            } else if (precision >= 4 && precision <= 6) {
                return TIME_UNIT_MICROSECOND;
            } else {
                return TIME_UNIT_NANOSECOND;
            }
        }
    }
}
