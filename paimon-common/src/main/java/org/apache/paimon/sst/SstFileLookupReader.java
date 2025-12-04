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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.MurmurHashUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/** An SST File Reader to serve lookup queries. */
public class SstFileLookupReader extends AbstractSstFileReader {
    public SstFileLookupReader(
            SeekableInputStream input,
            Comparator<MemorySlice> comparator,
            long fileSize,
            Path filePath,
            @Nullable BlockCache blockCache)
            throws IOException {
        super(input, comparator, fileSize, filePath, blockCache);
    }

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, null if not found.
     */
    public byte[] lookup(byte[] key) throws IOException {
        if (bloomFilter != null && !bloomFilter.testHash(MurmurHashUtils.hashBytes(key))) {
            return null;
        }

        MemorySlice keySlice = MemorySlice.wrap(key);
        if (firstKey == null || comparator.compare(firstKey, keySlice) > 0) {
            return null;
        }
        // seek the index to the block containing the key
        indexBlockIterator.seekTo(keySlice);

        // if indexIterator does not have a next, it means the key does not exist in this iterator
        if (indexBlockIterator.hasNext()) {
            // seek the current iterator to the key
            BlockIterator current = getNextBlock(indexBlockIterator);
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }
}
