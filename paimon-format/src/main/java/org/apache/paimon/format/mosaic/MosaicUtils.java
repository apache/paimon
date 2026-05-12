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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Shared varint and long encoding/decoding utilities for the Mosaic file format. */
public class MosaicUtils {

    // ==================== byte[] based ====================

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

    public static long readLong(byte[] buf, int[] pos) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (buf[pos[0]++] & 0xFF);
        }
        return v;
    }

    public static int writeVarint(byte[] buf, int pos, int value) {
        while ((value & ~0x7F) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        return pos;
    }

    public static int writeLong(byte[] buf, int pos, long value) {
        buf[pos++] = (byte) (value >>> 56);
        buf[pos++] = (byte) (value >>> 48);
        buf[pos++] = (byte) (value >>> 40);
        buf[pos++] = (byte) (value >>> 32);
        buf[pos++] = (byte) (value >>> 24);
        buf[pos++] = (byte) (value >>> 16);
        buf[pos++] = (byte) (value >>> 8);
        buf[pos++] = (byte) value;
        return pos;
    }

    // ==================== stream based ====================

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
}
