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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/** Converts Mosaic's byte[] statistics to Paimon objects. */
public class MosaicObjects {

    @Nullable
    public static Object convertStatsValue(byte[] bytes, DataType dataType) {
        if (bytes == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.fromBytes(bytes);
            case BINARY:
            case VARBINARY:
                return bytes;
            default:
                break;
        }
        if (bytes.length == 0) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return bytes[0] != 0;
            case TINYINT:
                return bytes[0];
            case SMALLINT:
                return buf.getShort();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return buf.getInt();
            case BIGINT:
                return buf.getLong();
            case FLOAT:
                return buf.getFloat();
            case DOUBLE:
                return buf.getDouble();
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                BigInteger unscaled = new BigInteger(bytes);
                BigDecimal decimal = new BigDecimal(unscaled, decimalType.getScale());
                return Decimal.fromBigDecimal(
                        decimal, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return convertTimestamp(buf, ((TimestampType) dataType).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return convertTimestamp(buf, ((LocalZonedTimestampType) dataType).getPrecision());
            default:
                return null;
        }
    }

    private static Timestamp convertTimestamp(ByteBuffer buf, int precision) {
        if (precision <= 3) {
            return Timestamp.fromEpochMillis(buf.getLong());
        } else if (precision <= 6) {
            return Timestamp.fromMicros(buf.getLong());
        } else {
            // precision 7-9: 12 bytes = i64 millis (BE) + i32 nanos_of_milli (BE)
            long millis = buf.getLong();
            int nanosOfMilli = buf.getInt();
            return Timestamp.fromEpochMillis(millis, nanosOfMilli);
        }
    }

    private MosaicObjects() {}
}
