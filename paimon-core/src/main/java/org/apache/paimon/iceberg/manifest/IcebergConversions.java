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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
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

    public static ByteBuffer toByteBuffer(DataType type, Object value) {
        int precision;
        Timestamp timestamp;
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
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                precision = timestampType.getPrecision();
                timestamp = (Timestamp) value;
                return convertTimestampWithPrecisionToBuffer(timestamp, precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localTimestampType = (LocalZonedTimestampType) type;
                precision = localTimestampType.getPrecision();
                timestamp = (Timestamp) value;
                return convertTimestampWithPrecisionToBuffer(timestamp, precision);
            case TIME_WITHOUT_TIME_ZONE:
                Long time = ((Integer) value).longValue();
                return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, time);
            default:
                throw new UnsupportedOperationException("Cannot serialize type: " + type);
        }
    }

    private static ByteBuffer convertTimestampWithPrecisionToBuffer(
            Timestamp timestamp, int precision) {
        long timestampValue;
        if (precision <= 6) {
            timestampValue = timestamp.getMillisecond();
        } else {
            timestampValue = timestamp.getNanoOfMillisecond();
        }
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(timestampValue);
    }
}
