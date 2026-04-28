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

package dev.vortex.api.proto;

import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.math.BigInteger;

/** Utility class for endianness conversions in Vortex protocol buffers. */
public final class EndianUtils {
    public static byte[] reverse(ByteString src) {
        byte[] dst = new byte[src.size()];
        for (int i = 0; i < dst.length; i++) {
            dst[i] = src.byteAt(dst.length - 1 - i);
        }
        return dst;
    }

    public static byte[] littleEndianDecimal(BigDecimal decimal) {
        BigInteger unscaled = decimal.unscaledValue();
        byte[] bigEndianBytes = unscaled.toByteArray();

        // Determine target size (1, 2, 4, 8, 16, or 32 bytes)
        int targetSize;
        if (bigEndianBytes.length <= 1) {
            targetSize = 1;
        } else if (bigEndianBytes.length <= 2) {
            targetSize = 2;
        } else if (bigEndianBytes.length <= 4) {
            targetSize = 4;
        } else if (bigEndianBytes.length <= 8) {
            targetSize = 8;
        } else if (bigEndianBytes.length <= 16) {
            targetSize = 16;
        } else if (bigEndianBytes.length <= 32) {
            targetSize = 32;
        } else {
            throw new IllegalArgumentException(
                    "BigDecimal with " + bigEndianBytes.length + " bytes overflows maximum Vortex decimal size");
        }

        byte[] result = new byte[targetSize];

        // Copy bytes in reverse order (big endian to little endian)
        for (int i = 0; i < bigEndianBytes.length; i++) {
            result[i] = bigEndianBytes[bigEndianBytes.length - 1 - i];
        }

        // Sign extend if negative
        if (unscaled.signum() < 0) {
            for (int i = bigEndianBytes.length; i < targetSize; i++) {
                result[i] = (byte) 0xFF;
            }
        }

        return result;
    }
}
