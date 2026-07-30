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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;

/** A {@link BlockDecompressor} for zstd. */
public class ZstdBlockDecompressor implements BlockDecompressor {

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        long decompressedLen;
        try {
            decompressedLen =
                    Zstd.decompressByteArray(dst, dstOff, dst.length - dstOff, src, srcOff, srcLen);
        } catch (ZstdException e) {
            throw new BufferDecompressionException(e);
        }
        if (Zstd.isError(decompressedLen)) {
            throw new BufferDecompressionException(Zstd.getErrorName(decompressedLen));
        }
        if (decompressedLen > Integer.MAX_VALUE) {
            throw new BufferDecompressionException(
                    "Decompressed ZSTD block is too large: " + decompressedLen);
        }
        return (int) decompressedLen;
    }
}
