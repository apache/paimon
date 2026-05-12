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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/** Shared encoding/decoding utilities for the Mosaic file format. */
public class MosaicUtils {

    // ==================== byte[] varint ====================

    public static int readVarint(byte[] buf, int pos) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = buf[pos++] & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    public static int readVarint(byte[] buf, int[] pos) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = buf[pos[0]++] & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    public static int writeVarint(byte[] buf, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }

    public static int varintSize(int value) {
        int size = 1;
        while ((value & ~0x7F) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    // ==================== byte[] int/long ====================

    public static int readInt(byte[] buf, int pos) {
        return ((buf[pos] & 0xFF) << 24)
                | ((buf[pos + 1] & 0xFF) << 16)
                | ((buf[pos + 2] & 0xFF) << 8)
                | (buf[pos + 3] & 0xFF);
    }

    public static long readLong(byte[] buf, int pos) {
        return ((long) (buf[pos] & 0xFF) << 56)
                | ((long) (buf[pos + 1] & 0xFF) << 48)
                | ((long) (buf[pos + 2] & 0xFF) << 40)
                | ((long) (buf[pos + 3] & 0xFF) << 32)
                | ((long) (buf[pos + 4] & 0xFF) << 24)
                | ((long) (buf[pos + 5] & 0xFF) << 16)
                | ((long) (buf[pos + 6] & 0xFF) << 8)
                | (buf[pos + 7] & 0xFF);
    }

    public static long readLong(byte[] buf, int[] pos) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[pos[0]++] & 0xFF);
        }
        return v;
    }

    public static int writeLong(byte[] buf, int pos, long v) {
        buf[pos] = (byte) (v >>> 56);
        buf[pos + 1] = (byte) (v >>> 48);
        buf[pos + 2] = (byte) (v >>> 40);
        buf[pos + 3] = (byte) (v >>> 32);
        buf[pos + 4] = (byte) (v >>> 24);
        buf[pos + 5] = (byte) (v >>> 16);
        buf[pos + 6] = (byte) (v >>> 8);
        buf[pos + 7] = (byte) v;
        return pos + 8;
    }

    // ==================== stream varint ====================

    public static void writeVarint(DataOutputStream out, int value) throws IOException {
        while ((value & ~0x7F) != 0) {
            out.writeByte((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    public static int readVarint(DataInputStream in) throws IOException {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = in.readByte() & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    // ==================== bit-packing ====================

    public static int bitWidth(int numEntries) {
        if (numEntries <= 1) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(numEntries - 1);
    }

    public static void writeBitPacked(
            byte[] buf, int byteBase, int bitOffset, int value, int bitWidth) {
        for (int b = 0; b < bitWidth; b++) {
            if ((value & (1 << b)) != 0) {
                int globalBit = bitOffset + b;
                buf[byteBase + globalBit / 8] |= (byte) (1 << (globalBit % 8));
            }
        }
    }

    public static int readBitPacked(byte[] buf, int byteBase, int bitOffset, int bitWidth) {
        int value = 0;
        for (int b = 0; b < bitWidth; b++) {
            int globalBit = bitOffset + b;
            if ((buf[byteBase + globalBit / 8] & (1 << (globalBit % 8))) != 0) {
                value |= (1 << b);
            }
        }
        return value;
    }

    // ==================== type width ====================

    public static int getFixedWidth(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            case DECIMAL:
                if (Decimal.isCompact(((DecimalType) type).getPrecision())) {
                    return 8;
                }
                return -1;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (Timestamp.isCompact(((TimestampType) type).getPrecision())) {
                    return 8;
                }
                return 12;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (Timestamp.isCompact(((LocalZonedTimestampType) type).getPrecision())) {
                    return 8;
                }
                return 12;
            default:
                return -1;
        }
    }

    // ==================== collection helpers ====================

    public static int[][] toIntArrays(List<List<Integer>> lists) {
        int[][] result = new int[lists.size()][];
        for (int i = 0; i < lists.size(); i++) {
            List<Integer> list = lists.get(i);
            result[i] = new int[list.size()];
            for (int j = 0; j < list.size(); j++) {
                result[i][j] = list.get(j);
            }
        }
        return result;
    }
}
