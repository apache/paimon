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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/** A {@link LookupStoreWriter} backed by an {@link SstFileWriter}. */
public class SortLookupStoreWriter implements LookupStoreWriter {
    private final SstFileWriter sstFileWriter;
    private final FileIO fileIO;
    private final PositionOutputStream out;

    public SortLookupStoreWriter(
            File file,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            BlockCompressionFactory compressionFactory)
            throws IOException {
        final Path filePath = new Path(file.getAbsolutePath());
        this.fileIO = LocalFileIO.create();
        this.out = fileIO.newOutputStream(filePath, true);
        this.sstFileWriter = new SstFileWriter(out, blockSize, bloomFilter, compressionFactory);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        sstFileWriter.put(key, value);
    }

    @Override
    public void close() throws IOException {
        sstFileWriter.close();
        out.close();
        fileIO.close();
    }
}
