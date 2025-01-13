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

package org.apache.paimon.memory;

import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

import static org.apache.paimon.data.BinarySection.HIGHEST_FIRST_BIT;
import static org.apache.paimon.data.BinarySection.HIGHEST_SECOND_TO_EIGHTH_BIT;
import static org.apache.paimon.memory.MemorySegment.LITTLE_ENDIAN;

/** Utils for byte[]. */
public class BytesUtils {

    public static int getInt(byte[] bytes, int offset) {
        return (bytes[offset + 3] << 24)
                | ((bytes[offset + 2] & 0xff) << 16)
                | ((bytes[offset + 1] & 0xff) << 8)
                | (bytes[offset] & 0xff);
    }

    public static short getShort(byte[] bytes, int offset) {
        return (short) ((bytes[offset + 1] << 8) | (bytes[offset] & 0xff));
    }

    public static long getLong(byte[] bytes, int offset) {
        return ((long) bytes[offset + 7] << 56)
                | (((long) bytes[offset + 6] & 0xff) << 48)
                | (((long) bytes[offset + 5] & 0xff) << 40)
                | (((long) bytes[offset + 4] & 0xff) << 32)
                | (((long) bytes[offset + 3] & 0xff) << 24)
                | (((long) bytes[offset + 2] & 0xff) << 16)
                | (((long) bytes[offset + 1] & 0xff) << 8)
                | ((long) bytes[offset] & 0xff);
    }

    public static byte[] readBinary(
            byte[] bytes, int baseOffset, int fieldOffset, long variablePartOffsetAndLen) {
        long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            final int subOffset = (int) (variablePartOffsetAndLen >> 32);
            final int len = (int) variablePartOffsetAndLen;
            byte[] ret = new byte[len];
            System.arraycopy(bytes, baseOffset + subOffset, ret, 0, len);
            return ret;
        } else {
            int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
            byte[] ret = new byte[len];
            if (LITTLE_ENDIAN) {
                System.arraycopy(bytes, fieldOffset, ret, 0, len);
            } else {
                System.arraycopy(bytes, fieldOffset + 1, ret, 0, len);
            }
            return ret;
        }
    }

    public static Variant readVariant(byte[] bytes, int baseOffset, long offsetAndLen) {
        int offset = baseOffset + (int) (offsetAndLen >> 32);
        int totalSize = (int) offsetAndLen;
        int valueSize = getInt(bytes, offset);
        int metadataSize = totalSize - 4 - valueSize;
        byte[] value = new byte[valueSize];
        byte[] metadata = new byte[metadataSize];
        System.arraycopy(bytes, offset + 4, value, 0, valueSize);
        System.arraycopy(bytes, offset + 4 + valueSize, metadata, 0, metadataSize);
        return new GenericVariant(value, metadata);
    }
}
