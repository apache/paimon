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

import io.airlift.compress.Decompressor;
import io.airlift.compress.MalformedInputException;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;
import static org.apache.paimon.compression.CompressorUtils.readIntLE;
import static org.apache.paimon.compression.CompressorUtils.validateLength;

/** Implementation of {@link BlockDecompressor} for airlift compressors. */
public class AirBlockDecompressor implements BlockDecompressor {

    private final Decompressor internalDecompressor;

    public AirBlockDecompressor(Decompressor internalDecompressor) {
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        int compressedLen = readIntLE(src, srcOff);
        int originalLen = readIntLE(src, srcOff + 4);
        validateLength(compressedLen, originalLen);

        if (dst.length - dstOff < originalLen) {
            throw new BufferDecompressionException("Buffer length too small");
        }

        if (src.length - srcOff - HEADER_LENGTH < compressedLen) {
            throw new BufferDecompressionException(
                    "Source data is not integral for decompression.");
        }

        try {
            final int decompressedLen =
                    internalDecompressor.decompress(
                            src, srcOff + HEADER_LENGTH, compressedLen, dst, dstOff, originalLen);
            if (originalLen != decompressedLen) {
                throw new BufferDecompressionException("Input is corrupted");
            }
        } catch (MalformedInputException e) {
            throw new BufferDecompressionException("Input is corrupted", e);
        }

        return originalLen;
    }
}
