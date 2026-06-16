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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Conversions between Java object and bytes.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#binary-single-value-serialization">Iceberg
 * spec</a>.
 */
public class IcebergConversions {

    private IcebergConversions() {}

    private static final ThreadLocal<CharsetEncoder> ENCODER =
            ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);
    private static final ThreadLocal<CharsetDecoder> DECODER =
            ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);

    public static ByteBuffer toByteBuffer(DataType type, Object value) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return ByteBuffer.allocate(1).put(0, (Boolean) value ? (byte) 0x01 : (byte) 0x00);
            case INTEGER:
            case DATE:
                return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, (int) value);
            case TINYINT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, ((byte) value));
            case SMALLINT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, ((Short) value).intValue());
            case BIGINT:
                return ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(0, (long) value);
            case FLOAT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putFloat(0, (float) value);
            case DOUBLE:
                return ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putDouble(0, (double) value);
            case CHAR:
            case VARCHAR:
                CharBuffer buffer = CharBuffer.wrap(value.toString());
                try {
                    ByteBuffer encoded = ENCODER.get().encode(buffer);
                    // ByteBuffer and CharBuffer allocate space based on capacity
                    // not actual content length. so we need to create a new ByteBuffer
                    // with the exact length of the encoded content
                    // to avoid padding the output with \u0000
                    if (encoded.limit() != encoded.capacity()) {
                        ByteBuffer exact = ByteBuffer.allocate(encoded.limit());
                        encoded.position(0);
                        exact.put(encoded);
                        exact.flip();
                        return exact;
                    }
                    return encoded;
                } catch (CharacterCodingException e) {
                    throw new RuntimeException("Failed to encode value as UTF-8: " + value, e);
                }
            case BINARY:
            case VARBINARY:
                return ByteBuffer.wrap((byte[]) value);
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return ByteBuffer.wrap((decimal.toUnscaledBytes()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampToByteBuffer(
                        (Timestamp) value, ((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampToByteBuffer(
                        (Timestamp) value, ((LocalZonedTimestampType) type).getPrecision());
            case TIME_WITHOUT_TIME_ZONE:
                return timeToByteBuffer((Integer) value, ((TimeType) type).getPrecision());
            case ARRAY:
                return arrayToByteBuffer((ArrayType) type, (InternalArray) value);
            case MAP:
                return mapToByteBuffer((MapType) type, (InternalMap) value);
            case ROW:
                return rowToByteBuffer((RowType) type, (InternalRow) value);
            default:
                throw new UnsupportedOperationException("Cannot serialize type: " + type);
        }
    }

    private static ByteBuffer timestampToByteBuffer(Timestamp timestamp, int precision) {
        Preconditions.checkArgument(
                precision >= 3 && precision <= 6,
                "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(0, timestamp.toMicros());
    }

    private static Timestamp timestampFromBytes(byte[] bytes, int precision) {
        Preconditions.checkArgument(
                precision >= 3 && precision <= 6,
                "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
        long encoded = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        return Timestamp.fromMicros(encoded);
    }

    private static ByteBuffer timeToByteBuffer(int millisOfDay, int precision) {
        Preconditions.checkArgument(
                precision >= 0 && precision <= 3,
                "Paimon Iceberg compatibility only support time type with precision from 0 to 3.");
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(0, millisOfDay * 1000L);
    }

    private static ByteBuffer arrayToByteBuffer(ArrayType arrayType, InternalArray array) {
        DataType elementType = arrayType.getElementType();
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
        List<ByteBuffer> elementBufs = new ArrayList<>();
        int dataSize = 0;
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                elementBufs.add(null);
            } else {
                Object element = getter.getElementOrNull(array, i);
                ByteBuffer buf = toByteBuffer(elementType, element);
                elementBufs.add(buf);
                dataSize += buf.limit();
            }
        }
        ByteBuffer result =
                ByteBuffer.allocate(4 + array.size() * 4 + dataSize).order(ByteOrder.LITTLE_ENDIAN);
        result.putInt(array.size());
        for (ByteBuffer buf : elementBufs) {
            if (buf == null) {
                result.putInt(-1);
            } else {
                result.putInt(buf.limit());
                result.put(buf);
            }
        }
        result.flip();
        return result;
    }

    private static ByteBuffer mapToByteBuffer(MapType mapType, InternalMap map) {
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        List<ByteBuffer> keyBufs = new ArrayList<>();
        List<ByteBuffer> valueBufs = new ArrayList<>();
        int dataSize = 0;
        for (int i = 0; i < map.size(); i++) {
            ByteBuffer keyBuf;
            if (keys.isNullAt(i)) {
                keyBuf = null;
            } else {
                keyBuf = toByteBuffer(keyType, keyGetter.getElementOrNull(keys, i));
                dataSize += keyBuf.limit();
            }
            keyBufs.add(keyBuf);
            ByteBuffer valueBuf;
            if (values.isNullAt(i)) {
                valueBuf = null;
            } else {
                valueBuf = toByteBuffer(valueType, valueGetter.getElementOrNull(values, i));
                dataSize += valueBuf.limit();
            }
            valueBufs.add(valueBuf);
        }
        ByteBuffer result =
                ByteBuffer.allocate(4 + map.size() * 8 + dataSize).order(ByteOrder.LITTLE_ENDIAN);
        result.putInt(map.size());
        for (int i = 0; i < map.size(); i++) {
            ByteBuffer keyBuf = keyBufs.get(i);
            if (keyBuf == null) {
                result.putInt(-1);
            } else {
                result.putInt(keyBuf.limit());
                result.put(keyBuf);
            }
            ByteBuffer valueBuf = valueBufs.get(i);
            if (valueBuf == null) {
                result.putInt(-1);
            } else {
                result.putInt(valueBuf.limit());
                result.put(valueBuf);
            }
        }
        result.flip();
        return result;
    }

    private static ByteBuffer rowToByteBuffer(RowType rowType, InternalRow row) {
        List<DataField> fields = rowType.getFields();
        List<ByteBuffer> fieldBufs = new ArrayList<>();
        int dataSize = 0;
        for (int i = 0; i < fields.size(); i++) {
            if (row.isNullAt(i)) {
                fieldBufs.add(null);
            } else {
                Object fieldValue = getFieldValue(row, fields.get(i).type(), i);
                ByteBuffer buf = toByteBuffer(fields.get(i).type(), fieldValue);
                fieldBufs.add(buf);
                dataSize += buf.limit();
            }
        }
        ByteBuffer result =
                ByteBuffer.allocate(fields.size() * 4 + dataSize).order(ByteOrder.LITTLE_ENDIAN);
        for (ByteBuffer buf : fieldBufs) {
            if (buf == null) {
                result.putInt(-1);
            } else {
                result.putInt(buf.limit());
                result.put(buf);
            }
        }
        result.flip();
        return result;
    }

    private static Object getFieldValue(InternalRow row, DataType fieldType, int pos) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos);
            case BINARY:
            case VARBINARY:
                return row.getBinary(pos);
            case DECIMAL:
                DecimalType dt = (DecimalType) fieldType;
                return row.getDecimal(pos, dt.getPrecision(), dt.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestamp(pos, ((TimestampType) fieldType).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestamp(pos, ((LocalZonedTimestampType) fieldType).getPrecision());
            case ARRAY:
                return row.getArray(pos);
            case MAP:
                return row.getMap(pos);
            case ROW:
                return row.getRow(pos, ((RowType) fieldType).getFieldCount());
            default:
                throw new UnsupportedOperationException(
                        "Cannot get field value for type: " + fieldType);
        }
    }

    public static Object toPaimonObject(DataType type, byte[] bytes) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return bytes[0] != 0;
            case INTEGER:
            case DATE:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            case TINYINT:
                return (byte) ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            case SMALLINT:
                return (short) ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            case BIGINT:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
            case FLOAT:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
            case DOUBLE:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            case CHAR:
            case VARCHAR:
                try {
                    return BinaryString.fromString(
                            DECODER.get().decode(ByteBuffer.wrap(bytes)).toString());
                } catch (CharacterCodingException e) {
                    throw new RuntimeException("Failed to decode bytes as UTF-8", e);
                }
            case BINARY:
            case VARBINARY:
                return bytes;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromUnscaledBytes(
                        bytes, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampFromBytes(bytes, ((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // LocalZonedTimestampType does not extend TimestampType, so it cannot
                // share a switch arm with TIMESTAMP_WITHOUT_TIME_ZONE.
                return timestampFromBytes(bytes, ((LocalZonedTimestampType) type).getPrecision());
            case TIME_WITHOUT_TIME_ZONE:
                int timePrecision = ((TimeType) type).getPrecision();
                Preconditions.checkArgument(
                        timePrecision >= 0 && timePrecision <= 3,
                        "Paimon Iceberg compatibility only support time type with precision from 0 to 3.");
                long timeMicros = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
                return (int) (timeMicros / 1000L);
            case ARRAY:
                return arrayFromBytes((ArrayType) type, bytes);
            case MAP:
                return mapFromBytes((MapType) type, bytes);
            case ROW:
                return rowFromBytes((RowType) type, bytes);
            default:
                throw new UnsupportedOperationException("Cannot deserialize type: " + type);
        }
    }

    private static Object arrayFromBytes(ArrayType arrayType, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        DataType elementType = arrayType.getElementType();
        Object[] elements = new Object[count];
        for (int i = 0; i < count; i++) {
            elements[i] = readElement(elementType, buf);
        }
        return new GenericArray(elements);
    }

    private static Object mapFromBytes(MapType mapType, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buf.getInt();
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < count; i++) {
            Object key = readElement(mapType.getKeyType(), buf);
            Object value = readElement(mapType.getValueType(), buf);
            map.put(key, value);
        }
        return new GenericMap(map);
    }

    private static Object rowFromBytes(RowType rowType, byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        List<DataField> fields = rowType.getFields();
        GenericRow row = new GenericRow(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            row.setField(i, readElement(fields.get(i).type(), buf));
        }
        return row;
    }

    private static Object readElement(DataType type, ByteBuffer buf) {
        int length = buf.getInt();
        if (length == -1) {
            return null;
        }
        byte[] elementBytes = new byte[length];
        buf.get(elementBytes);
        return toPaimonObject(type, elementBytes);
    }
}
