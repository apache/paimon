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
import com.github.luben.zstd.ZstdOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.paimon.compression.CompressorUtils.HEADER_LENGTH;

/** A {@link BlockCompressor} for zstd. */
public class ZstdBlockCompressor implements BlockCompressor {

    private static final int MAX_BLOCK_SIZE = 128 * 1024;

    private final int level;

    public ZstdBlockCompressor(int level) {
        this.level = level;
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return HEADER_LENGTH + zstdMaxCompressedLength(srcSize);
    }

    private int zstdMaxCompressedLength(int uncompressedSize) {
        // refer to io.airlift.compress.zstd.ZstdCompressor
        int result = uncompressedSize + (uncompressedSize >>> 8);
        if (uncompressedSize < MAX_BLOCK_SIZE) {
            result += (MAX_BLOCK_SIZE - uncompressedSize) >>> 11;
        }
        return result;
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferCompressionException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(dst, dstOff);
        try (ZstdOutputStream zstdStream =
                new ZstdOutputStream(stream, RecyclingBufferPool.INSTANCE, level)) {
            zstdStream.setWorkers(0);
            zstdStream.write(src, srcOff, srcLen);
        } catch (IOException e) {
            throw new BufferCompressionException(e);
        }
        return stream.position() - dstOff;
    }

    private static class ByteArrayOutputStream extends OutputStream {

        private final byte[] buf;
        private int position;

        public ByteArrayOutputStream(byte[] buf, int position) {
            this.buf = buf;
            this.position = position;
        }

        @Override
        public void write(int b) {
            buf[position] = (byte) b;
            position += 1;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (b == null || buf == null) {
                throw new NullPointerException("Input array or buffer is null");
            }
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException("Invalid offset or length");
            }
            if (b.length == 0){
                return;
            }
            try {
                System.arraycopy(b, off, buf, position, len);
            } catch (IndexOutOfBoundsException e) {
                throw new IOException(e);
            }
            position += len;
        }

        int position() {
            return position;
        }
    }
}
