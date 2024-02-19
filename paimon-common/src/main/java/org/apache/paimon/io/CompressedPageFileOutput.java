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

package org.apache.paimon.io;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/** A class to output bytes with compression. */
public class CompressedPageFileOutput implements PageFileOutput {

    private final FileOutputStream out;
    private final byte[] page;
    private final BlockCompressor compressor;
    private final byte[] compressedPage;
    private final List<Long> pages;

    private long uncompressBytes;
    private long position;
    private int count;

    public CompressedPageFileOutput(
            File file, int pageSize, BlockCompressionFactory compressionFactory)
            throws FileNotFoundException {
        this.out = new FileOutputStream(file);
        this.page = new byte[pageSize];
        this.compressor = compressionFactory.getCompressor();
        this.compressedPage = new byte[compressor.getMaxCompressedSize(pageSize)];
        this.pages = new ArrayList<>();

        this.uncompressBytes = 0;
        this.position = 0;
        this.count = 0;
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            pages.add(position);
            int len = compressor.compress(page, 0, count, compressedPage, 0);
            // write length
            out.write((len >>> 24) & 0xFF);
            out.write((len >>> 16) & 0xFF);
            out.write((len >>> 8) & 0xFF);
            out.write(len & 0xFF);
            // write page
            out.write(compressedPage, 0, len);
            count = 0;
            position += (len + 4);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        uncompressBytes += len;

        while (len > 0) {
            if (count >= page.length) {
                flushBuffer();
            }
            int toWrite = Math.min(len, page.length - count);
            System.arraycopy(b, off, page, count, toWrite);
            off += toWrite;
            len -= toWrite;
            count += toWrite;
        }
    }

    @Override
    public void close() throws IOException {
        try (OutputStream ignored = out) {
            flushBuffer();
        }
    }

    public long uncompressBytes() {
        return uncompressBytes;
    }

    public long[] pages() {
        long[] pages = new long[this.pages.size()];
        for (int i = 0; i < pages.length; i++) {
            pages[i] = this.pages.get(i);
        }
        return pages;
    }
}
