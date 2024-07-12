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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

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
                    return ENCODER.get().encode(buffer);
                } catch (CharacterCodingException e) {
                    throw new RuntimeException("Failed to encode value as UTF-8: " + value, e);
                }
            case BINARY:
            case VARBINARY:
                return ByteBuffer.wrap((byte[]) value);
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return ByteBuffer.wrap((decimal.toUnscaledBytes()));
            default:
                throw new UnsupportedOperationException("Cannot serialize type: " + type);
        }
    }

    public static Object toObject(DataType type, byte[] bytes) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return bytes[0] != 0;
            case INTEGER:
            case DATE:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
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
            default:
                throw new UnsupportedOperationException("Cannot deserialize type: " + type);
        }
    }
}
