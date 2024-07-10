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

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/** A {@link BlockDecompressor} for zstd. */
public class ZstdBlockDecompressor implements BlockDecompressor {

    @Override
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(src, srcOff, srcLen);
        try (ZstdInputStream decompressorStream =
                new ZstdInputStream(inputStream, RecyclingBufferPool.INSTANCE)) {
            int decompressedLen = 0;
            while (true) {
                int offset = dstOff + decompressedLen;
                int count = decompressorStream.read(dst, offset, dst.length - offset);
                if (count <= 0) {
                    if (decompressorStream.available() != 0) {
                        throw new BufferDecompressionException(
                                "Dst is too small and the decompression was not completed.");
                    }
                    break;
                }
                decompressedLen += count;
            }
            return decompressedLen;
        } catch (IOException e) {
            throw new BufferDecompressionException(e);
        }
    }
}
