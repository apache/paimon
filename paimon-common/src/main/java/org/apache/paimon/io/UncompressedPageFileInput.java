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

import org.apache.paimon.utils.MathUtils;

import java.io.IOException;
import java.io.RandomAccessFile;

/** A class to wrap uncompressed {@link RandomAccessFile}. */
public class UncompressedPageFileInput implements PageFileInput {

    private final RandomAccessFile file;
    private final long fileLength;
    private final int pageSize;
    private final int pageSizeBits;

    public UncompressedPageFileInput(RandomAccessFile file, int pageSize) throws IOException {
        this.file = file;
        this.fileLength = file.length();
        this.pageSize = pageSize;
        this.pageSizeBits = MathUtils.log2strict(pageSize);
    }

    @Override
    public RandomAccessFile file() {
        return file;
    }

    @Override
    public long uncompressBytes() {
        return fileLength;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public byte[] readPage(int pageIndex) throws IOException {
        long position = (long) pageIndex << pageSizeBits;
        file.seek(position);

        int length = (int) Math.min(pageSize, fileLength - position);
        byte[] result = new byte[length];
        file.readFully(result);
        return result;
    }

    @Override
    public byte[] readPosition(long position, int length) throws IOException {
        file.seek(position);
        byte[] result = new byte[length];
        file.readFully(result);
        return result;
    }

    @Override
    public void close() throws IOException {
        this.file.close();
    }
}
