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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/** An interface to read pages from file. */
public interface PageFileInput extends Closeable {

    RandomAccessFile file();

    long uncompressBytes();

    int pageSize();

    byte[] readPage(int pageIndex) throws IOException;

    byte[] readPosition(long position, int length) throws IOException;

    static PageFileInput create(
            File file,
            int pageSize,
            @Nullable BlockCompressionFactory compressionFactory,
            long uncompressBytes,
            @Nullable long[] compressPagePositions)
            throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "r");
        if (compressionFactory == null) {
            return new UncompressedPageFileInput(accessFile, pageSize);
        } else {
            return new CompressedPageFileInput(
                    accessFile,
                    pageSize,
                    compressionFactory,
                    uncompressBytes,
                    compressPagePositions);
        }
    }
}
