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

package org.apache.paimon.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Combining Delta Encoding and Varints Encoding, suitable for integer sequences that are increasing
 * or not significantly different.
 */
public class DeltaVarintCompressor {

    // Compresses a long array using delta encoding, ZigZag transformation, and Varints encoding
    public static byte[] compress(long[] data) {
        if (data == null || data.length == 0) {
            return new byte[0];
        }

        LongArrayList deltas = new LongArrayList(data.length);
        // Store the first element
        deltas.add(data[0]);
        for (int i = 1; i < data.length; i++) {
            // Compute delta
            deltas.add(data[i] - data[i - 1]);
        }

        // Pre-allocate space
        ByteArrayOutputStream out = new ByteArrayOutputStream(data.length * 10);
        for (int i = 0; i < deltas.size(); i++) {
            // Apply ZigZag and Varints
            encodeVarint(deltas.get(i), out);
        }
        return out.toByteArray();
    }

    // Decompresses a byte array back to the original long array
    public static long[] decompress(byte[] compressed) {
        if (compressed == null || compressed.length == 0) {
            return new long[0];
        }

        ByteArrayInputStream in = new ByteArrayInputStream(compressed);
        // Pre-allocate space
        LongArrayList deltas = new LongArrayList(compressed.length);
        while (in.available() > 0) {
            // Decode Varints and reverse ZigZag
            deltas.add(decodeVarint(in));
        }

        long[] result = new long[deltas.size()];
        // Restore the first element
        result[0] = deltas.get(0);
        for (int i = 1; i < result.length; i++) {
            // Reconstruct using deltas
            result[i] = result[i - 1] + deltas.get(i);
        }
        return result;
    }

    // Encodes a long value using ZigZag and Varints
    private static void encodeVarint(long value, ByteArrayOutputStream out) {
        // ZigZag transformation for long
        long tmp = (value << 1) ^ (value >> 63);
        // Check if multiple bytes are needed
        while ((tmp & ~0x7FL) != 0) {
            // Set MSB to 1 (continuation)
            out.write(((int) tmp & 0x7F) | 0x80);
            // Unsigned right shift
            tmp >>>= 7;
        }
        // Final byte with MSB set to 0
        out.write((byte) tmp);
    }

    // Decodes a Varints-encoded value and reverses ZigZag transformation
    private static long decodeVarint(ByteArrayInputStream in) {
        long result = 0;
        int shift = 0;
        while (true) {
            long b = in.read();
            if (b == -1) {
                throw new RuntimeException("Unexpected end of input");
            }
            // Extract 7 bits
            result |= (b & 0x7F) << shift;
            // MSB is 0, end of encoding
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw new RuntimeException("Varint overflow");
            }
        }
        // Reverse ZigZag transformation
        long zigzag = result >>> 1;
        return (result & 1) == 0 ? zigzag : (~zigzag);
    }
}
