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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;

/** A resizable byte buffer with little-endian primitive write operations. */
class BlockOutput {

    byte[] buffer;
    int position;

    BlockOutput(int initialCapacity) {
        this.buffer = new byte[initialCapacity];
        this.position = 0;
    }

    void writeBoolean(boolean value) {
        ensureCapacity(1);
        buffer[position++] = (byte) (value ? 1 : 0);
    }

    void writeByte(byte value) {
        ensureCapacity(1);
        buffer[position++] = value;
    }

    void writeShort(short value) {
        ensureCapacity(2);
        buffer[position++] = (byte) (value & 0xFF);
        buffer[position++] = (byte) ((value >>> 8) & 0xFF);
    }

    void writeInt(int value) {
        ensureCapacity(4);
        buffer[position++] = (byte) (value & 0xFF);
        buffer[position++] = (byte) ((value >>> 8) & 0xFF);
        buffer[position++] = (byte) ((value >>> 16) & 0xFF);
        buffer[position++] = (byte) ((value >>> 24) & 0xFF);
    }

    void writeLong(long value) {
        ensureCapacity(8);
        buffer[position++] = (byte) (value & 0xFF);
        buffer[position++] = (byte) ((value >>> 8) & 0xFF);
        buffer[position++] = (byte) ((value >>> 16) & 0xFF);
        buffer[position++] = (byte) ((value >>> 24) & 0xFF);
        buffer[position++] = (byte) ((value >>> 32) & 0xFF);
        buffer[position++] = (byte) ((value >>> 40) & 0xFF);
        buffer[position++] = (byte) ((value >>> 48) & 0xFF);
        buffer[position++] = (byte) ((value >>> 56) & 0xFF);
    }

    void writeFloat(float value) {
        writeInt(Float.floatToRawIntBits(value));
    }

    void writeDouble(double value) {
        writeLong(Double.doubleToRawLongBits(value));
    }

    void writeVarInt(int value) {
        ensureCapacity(5);
        while ((value & ~0x7F) != 0) {
            buffer[position++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buffer[position++] = (byte) value;
    }

    void writeBytes(byte[] value) {
        writeVarInt(value.length);
        ensureCapacity(value.length);
        System.arraycopy(value, 0, buffer, position, value.length);
        position += value.length;
    }

    void writeDecimal(Decimal value, int precision) {
        if (Decimal.isCompact(precision)) {
            writeLong(value.toUnscaledLong());
        } else {
            writeBytes(value.toUnscaledBytes());
        }
    }

    void writeTimestamp(Timestamp value, int precision) {
        if (Timestamp.isCompact(precision)) {
            writeLong(value.getMillisecond());
        } else {
            writeLong(value.getMillisecond());
            writeVarInt(value.getNanoOfMillisecond());
        }
    }

    void ensureCapacity(int additional) {
        int required = position + additional;
        if (required > buffer.length) {
            int newSize = Math.max(buffer.length * 2, required);
            byte[] newBuffer = new byte[newSize];
            System.arraycopy(buffer, 0, newBuffer, 0, position);
            buffer = newBuffer;
        }
    }
}
