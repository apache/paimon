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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Sequential reader for btree_file_meta SST files (fileName → ManifestEntry bytes).
 *
 * <p>btree_file_meta files are written by {@link BinaryFileMetaWriter} using the same SST format as
 * btree key index files, but without a BTreeIndexMeta (null meta) and without a null bitmap. Keys
 * are UTF-8 encoded file names; values are serialized {@link
 * org.apache.paimon.manifest.ManifestEntry} bytes.
 */
public class BinaryFileMetaIndexReader implements Closeable {

    private final SeekableInputStream input;
    private final SstFileReader sstReader;

    public BinaryFileMetaIndexReader(
            GlobalIndexFileReader fileReader, GlobalIndexIOMeta meta, Options options)
            throws IOException {
        this.input = fileReader.getInputStream(meta);
        long fileSize = meta.fileSize();

        CacheManager cacheManager =
                new CacheManager(
                        options.get(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE),
                        options.get(BTreeIndexOptions.BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO));

        BlockCache blockCache = new BlockCache(meta.filePath(), input, cacheManager);
        BTreeFileFooter footer = readFooter(blockCache, fileSize);

        Comparator<MemorySlice> sliceComparator = lexicographicComparator();
        this.sstReader =
                new SstFileReader(sliceComparator, blockCache, footer.getIndexBlockHandle(), null);
    }

    /** Returns an iterator over all (key, value) pairs in insertion order. */
    public EntryIterator iterator() {
        return new EntryIterator();
    }

    @Override
    public void close() throws IOException {
        sstReader.close();
        input.close();
    }

    private BTreeFileFooter readFooter(BlockCache blockCache, long fileSize) {
        MemorySegment footerBytes =
                blockCache.getBlock(
                        fileSize - BTreeFileFooter.ENCODED_LENGTH,
                        BTreeFileFooter.ENCODED_LENGTH,
                        b -> b,
                        true);
        return BTreeFileFooter.readFooter(MemorySlice.wrap(footerBytes).toInput());
    }

    private static Comparator<MemorySlice> lexicographicComparator() {
        return (a, b) -> {
            int lenA = a.length();
            int lenB = b.length();
            int min = Math.min(lenA, lenB);
            for (int i = 0; i < min; i++) {
                int diff = (a.readByte(i) & 0xFF) - (b.readByte(i) & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }
            return lenA - lenB;
        };
    }

    /**
     * Iterator over btree_file_meta SST entries, yielding (fileName, ManifestEntry bytes) pairs.
     */
    public class EntryIterator {

        private final SstFileReader.SstFileIterator fileIter;
        private BlockIterator dataIter;
        private String nextKey;
        private byte[] nextValue;

        private EntryIterator() {
            this.fileIter = sstReader.createIterator();
        }

        public boolean hasNext() throws IOException {
            if (nextValue != null) {
                return true;
            }

            while (true) {
                if (dataIter != null && dataIter.hasNext()) {
                    Map.Entry<MemorySlice, MemorySlice> entry = dataIter.next();
                    nextKey = new String(entry.getKey().copyBytes(), StandardCharsets.UTF_8);
                    nextValue = entry.getValue().copyBytes();
                    return true;
                }

                dataIter = fileIter.readBatch();
                if (dataIter == null) {
                    return false;
                }
            }
        }

        /**
         * Returns the fileName (key) of the current entry. Must be called after {@link #hasNext()}
         * returns {@code true} and before {@link #nextValue()}.
         */
        public String nextKey() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in btree_file_meta file.");
            }
            return nextKey;
        }

        /** Returns the raw ManifestEntry bytes (value) and advances the iterator. */
        public byte[] nextValue() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in btree_file_meta file.");
            }
            byte[] result = nextValue;
            nextKey = null;
            nextValue = null;
            return result;
        }
    }
}
