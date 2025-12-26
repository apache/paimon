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
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A {@link LookupStoreWriter} backed by an {@link SstFileWriter}. The SST File layout is as below:
 * (For layouts of each block type, please refer to corresponding classes)
 *
 * <pre>
 *     +-----------------------------------+------+
 *     |             Footer                |      |
 *     +-----------------------------------+      |
 *     |           Index Block             |      +--> Loaded on open
 *     +-----------------------------------+      |
 *     |        Bloom Filter Block         |      |
 *     +-----------------------------------+------+
 *     |            Data Block             |      |
 *     +-----------------------------------+      |
 *     |              ......               |      +--> Loaded on requested
 *     +-----------------------------------+      |
 *     |            Data Block             |      |
 *     +-----------------------------------+------+
 * </pre>
 */
public class SortLookupStoreWriter implements LookupStoreWriter {

    private final SstFileWriter writer;
    private final PositionOutputStream out;

    public SortLookupStoreWriter(
            PositionOutputStream out,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            BlockCompressionFactory compressionFactory) {
        this.out = out;
        this.writer = new SstFileWriter(out, blockSize, bloomFilter, compressionFactory);
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        writer.put(key, value);
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();
        BlockHandle indexBlockHandle = writer.writeIndexBlock();
        SortLookupStoreFooter footer =
                new SortLookupStoreFooter(bloomFilterHandle, indexBlockHandle);
        MemorySlice footerEncoding = SortLookupStoreFooter.writeFooter(footer);
        writer.writeSlice(footerEncoding);
        out.close();
    }
}
