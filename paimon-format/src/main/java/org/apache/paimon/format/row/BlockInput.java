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

package org.apache.paimon.format.row;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;

/** A cursor over a byte array that reads primitives in little-endian order. */
class BlockInput {

    final byte[] data;
    int position;

    BlockInput(byte[] data) {
        this.data = data;
        this.position = 0;
    }

    boolean readBoolean() {
        return data[position++] != 0;
    }

    byte readByte() {
        return data[position++];
    }

    short readShort() {
        short v = (short) ((data[position] & 0xFF) | ((data[position + 1] & 0xFF) << 8));
        position += 2;
        return v;
    }

    int readInt() {
        int v =
                (data[position] & 0xFF)
                        | ((data[position + 1] & 0xFF) << 8)
                        | ((data[position + 2] & 0xFF) << 16)
                        | ((data[position + 3] & 0xFF) << 24);
        position += 4;
        return v;
    }

    long readLong() {
        long v =
                (data[position] & 0xFFL)
                        | ((data[position + 1] & 0xFFL) << 8)
                        | ((data[position + 2] & 0xFFL) << 16)
                        | ((data[position + 3] & 0xFFL) << 24)
                        | ((data[position + 4] & 0xFFL) << 32)
                        | ((data[position + 5] & 0xFFL) << 40)
                        | ((data[position + 6] & 0xFFL) << 48)
                        | ((data[position + 7] & 0xFFL) << 56);
        position += 8;
        return v;
    }

    float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    int readVarInt() {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = data[position++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

    BinaryString readString() {
        int length = readVarInt();
        BinaryString s = BinaryString.fromBytes(data, position, length);
        position += length;
        return s;
    }

    byte[] readBytes() {
        int length = readVarInt();
        byte[] result = new byte[length];
        System.arraycopy(data, position, result, 0, length);
        position += length;
        return result;
    }

    Decimal readDecimal(int precision, int scale) {
        return Decimal.isCompact(precision)
                ? Decimal.fromUnscaledLong(readLong(), precision, scale)
                : Decimal.fromUnscaledBytes(readBytes(), precision, scale);
    }

    Timestamp readTimestamp(int precision) {
        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(readLong());
        }
        long millis = readLong();
        int nanos = readVarInt();
        return Timestamp.fromEpochMillis(millis, nanos);
    }

    static int readIntLE(byte[] buf, int offset) {
        return (buf[offset] & 0xFF)
                | ((buf[offset + 1] & 0xFF) << 8)
                | ((buf[offset + 2] & 0xFF) << 16)
                | ((buf[offset + 3] & 0xFF) << 24);
    }
}
