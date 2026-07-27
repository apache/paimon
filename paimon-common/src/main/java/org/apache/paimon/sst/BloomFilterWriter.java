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

package org.apache.paimon.sst;

import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.BloomFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** Writer for collecting key hashes and writing an SST Bloom filter. */
interface BloomFilterWriter {

    Logger LOG = LoggerFactory.getLogger(BloomFilterWriter.class);

    void addHash(int hash);

    @Nullable
    BloomFilter.Builder finish();

    @Nullable
    default BloomFilterHandle write(PositionOutputStream out) throws IOException {
        BloomFilter.Builder bloomFilter = finish();
        if (bloomFilter == null) {
            return null;
        }

        MemorySegment buffer = bloomFilter.getBuffer();
        BloomFilterHandle handle =
                new BloomFilterHandle(out.getPos(), buffer.size(), bloomFilter.expectedEntries());
        MemorySlice slice = MemorySlice.wrap(buffer);
        out.write(slice.getHeapMemory(), slice.offset(), slice.length());
        LOG.info("Bloom filter size: {} bytes", buffer.size());
        return handle;
    }
}
