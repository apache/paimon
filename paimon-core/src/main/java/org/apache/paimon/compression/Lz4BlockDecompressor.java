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

package org.apache.paimon.compression;

import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.readIntLE;
import static org.apache.paimon.compression.CompressorUtils.validateLength;

/**
 * Decode data written with {@link Lz4BlockCompressor}. It reads from and writes to byte arrays
 * provided from the outside, thus reducing copy time.
 *
 * <p>This class is copied and modified from {@link net.jpountz.lz4.LZ4BlockInputStream}.
 */
public class Lz4BlockDecompressor implements BlockDecompressor {

    private final LZ4FastDecompressor decompressor;

    public Lz4BlockDecompressor() {
        this.decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        final int compressedLen = readIntLE(src, srcOff);
        final int originalLen = readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new BufferDecompressionException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new BufferDecompressionException(
                    "Source data is not integral for decompression.");
        }

        try {
            final int compressedLen2 =
                    decompressor.decompress(src, srcOff + HEADER_LENGTH, dst, dstOff, originalLen);
            if (compressedLen != compressedLen2) {
                throw new BufferDecompressionException("Input is corrupted");
            }
        } catch (LZ4Exception e) {
            throw new BufferDecompressionException("Input is corrupted", e);
        }

        return originalLen;
    }
}
