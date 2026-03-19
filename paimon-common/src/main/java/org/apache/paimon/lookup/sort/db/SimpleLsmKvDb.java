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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.lookup.sort.SortLookupStoreReader;
import org.apache.paimon.lookup.sort.SortLookupStoreWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A simple LSM-Tree based KV database built on top of {@link SortLookupStoreFactory}.
 *
 * <p>Architecture (Universal Compaction, inspired by RocksDB):
 *
 * <pre>
 *     ┌──────────────────────────────────────────────┐
 *     │            MemTable (SkipList)                │  ← Active writes
 *     ├──────────────────────────────────────────────┤
 *     │  Sorted Runs (newest → oldest):              │
 *     │    [Run-0] [Run-1] [Run-2] ... [Run-N]       │  ← Each run is a sorted SST file set
 *     └──────────────────────────────────────────────┘
 * </pre>
 *
 * <p>Compaction is triggered when the number of sorted runs exceeds a threshold. Runs are selected
 * for merging based on size ratios between adjacent runs, following RocksDB's Universal Compaction
 * strategy.
 *
 * <p>Note: No WAL is implemented. Data in the MemTable will be lost on crash.
 *
 * <p>This class is <b>not</b> thread-safe. External synchronization is required if accessed from
 * multiple threads.
 */
public class SimpleLsmKvDb implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleLsmKvDb.class);

    /** Tombstone marker for deleted keys. */
    private static final byte[] TOMBSTONE = new byte[0];

    /** Maximum number of levels in the LSM tree. */
    static final int MAX_LEVELS = 4;

    /**
     * Estimated per-entry memory overhead in the MemTable's TreeMap, beyond the raw key/value
     * bytes. This accounts for:
     *
     * <ul>
     *   <li>TreeMap.Entry node: ~64 bytes (object header + left/right/parent/key/value refs +
     *       color)
     *   <li>MemorySlice wrapper: ~32 bytes (object header + segment ref + offset + length)
     *   <li>MemorySegment backing the key: ~48 bytes (object header + heapMemory/offHeapBuffer refs
     *       + address + size)
     *   <li>byte[] value array header: ~16 bytes (object header + length)
     * </ul>
     */
    static final long PER_ENTRY_OVERHEAD = 160;

    private final File dataDirectory;
    private final SortLookupStoreFactory storeFactory;
    private final Comparator<MemorySlice> keyComparator;
    private final long memTableFlushThreshold;
    private final LsmCompactor compactor;

    /** Active MemTable: key -> value bytes (empty byte[] = tombstone). */
    private TreeMap<MemorySlice, byte[]> memTable;

    /** Estimated size of the current MemTable in bytes. */
    private long memTableSize;

    /**
     * Multi-level SST file storage. Each level contains a list of {@link SstFileMetadata} ordered
     * by key range. Level 0 files are ordered newest-first (key ranges may overlap). Level 1+ files
     * are ordered by minKey (key ranges do NOT overlap).
     */
    private final List<List<SstFileMetadata>> levels;

    /** Cached readers for SST files, keyed by file path. Lazily populated on first lookup. */
    private final Map<File, SortLookupStoreReader> readerCache;

    private long fileSequence;
    private boolean closed;

    private SimpleLsmKvDb(
            File dataDirectory,
            SortLookupStoreFactory storeFactory,
            Comparator<MemorySlice> keyComparator,
            long memTableFlushThreshold,
            long maxSstFileSize,
            int level0FileNumCompactTrigger,
            int sizeRatio) {
        this.dataDirectory = dataDirectory;
        this.storeFactory = storeFactory;
        this.keyComparator = keyComparator;
        this.memTableFlushThreshold = memTableFlushThreshold;
        this.memTable = new TreeMap<>(keyComparator);
        this.memTableSize = 0;
        this.levels = new ArrayList<>();
        for (int i = 0; i < MAX_LEVELS; i++) {
            this.levels.add(new ArrayList<>());
        }
        this.readerCache = new HashMap<>();
        this.fileSequence = 0;
        this.closed = false;
        this.compactor =
                new LsmCompactor(
                        keyComparator,
                        storeFactory,
                        maxSstFileSize,
                        level0FileNumCompactTrigger,
                        sizeRatio,
                        this::closeAndDeleteSstFile);
    }

    /**
     * Close the cached reader for the given SST file (if any) and delete the file from disk. This
     * is invoked by {@link LsmCompactor} via the {@link LsmCompactor.FileDeleter} callback during
     * compaction.
     */
    private void closeAndDeleteSstFile(File file) {
        SortLookupStoreReader reader = readerCache.remove(file);
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close reader for SST file: {}", file.getName(), e);
            }
        }
        if (file.exists()) {
            boolean deleted = file.delete();
            if (!deleted) {
                LOG.warn("Failed to delete SST file: {}", file.getName());
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Builder
    // -------------------------------------------------------------------------

    /** Create a builder for {@link SimpleLsmKvDb}. */
    public static Builder builder(File dataDirectory) {
        return new Builder(dataDirectory);
    }

    // -------------------------------------------------------------------------
    //  Write Operations
    // -------------------------------------------------------------------------

    /**
     * Put a key-value pair into the database.
     *
     * @param key the key bytes, must not be null
     * @param value the value bytes, must not be null
     */
    public void put(byte[] key, byte[] value) throws IOException {
        ensureOpen();
        if (value.length == 0) {
            throw new IllegalArgumentException(
                    "Value must not be an empty byte array, which is reserved as TOMBSTONE marker. "
                            + "Use delete() to remove a key.");
        }
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        byte[] oldValue = memTable.put(wrappedKey, value);
        long delta = key.length + value.length;
        if (oldValue != null) {
            delta -= (key.length + oldValue.length);
        } else {
            delta += PER_ENTRY_OVERHEAD;
        }
        memTableSize += delta;
        maybeFlushMemTable();
    }

    /**
     * Delete a key from the database by writing a tombstone.
     *
     * @param key the key bytes to delete
     */
    public void delete(byte[] key) throws IOException {
        ensureOpen();
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        byte[] oldValue = memTable.put(wrappedKey, TOMBSTONE);
        long delta = key.length;
        if (oldValue != null) {
            delta -= (key.length + oldValue.length);
        } else {
            delta += PER_ENTRY_OVERHEAD;
        }
        memTableSize += delta;
        maybeFlushMemTable();
    }

    // -------------------------------------------------------------------------
    //  Read Operations
    // -------------------------------------------------------------------------

    /**
     * Get the value associated with the given key.
     *
     * <p>Search order: MemTable → Level 0 (newest to oldest) → Level 1 → Level 2 → ...
     *
     * @param key the key bytes
     * @return the value bytes, or null if the key does not exist or has been deleted
     */
    @Nullable
    public byte[] get(byte[] key) throws IOException {
        ensureOpen();

        // 1. Search MemTable first (newest data)
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        byte[] memValue = memTable.get(wrappedKey);
        if (memValue != null) {
            return isTombstone(memValue) ? null : memValue;
        }

        // 2. Search each level from L0 to Lmax
        for (int level = 0; level < MAX_LEVELS; level++) {
            List<SstFileMetadata> levelFiles = levels.get(level);
            if (levelFiles.isEmpty()) {
                continue;
            }

            if (level == 0) {
                // L0: files may have overlapping keys, search newest-first
                for (SstFileMetadata meta : levelFiles) {
                    if (!meta.mightContainKey(wrappedKey, keyComparator)) {
                        continue;
                    }
                    byte[] value = lookupInFile(meta.getFile(), key);
                    if (value != null) {
                        return isTombstone(value) ? null : value;
                    }
                }
            } else {
                // L1+: files have non-overlapping key ranges, binary search
                SstFileMetadata target = findFileForKey(levelFiles, wrappedKey);
                if (target != null) {
                    byte[] value = lookupInFile(target.getFile(), key);
                    if (value != null) {
                        return isTombstone(value) ? null : value;
                    }
                }
            }
        }

        return null;
    }

    // -------------------------------------------------------------------------
    //  Flush & Compaction
    // -------------------------------------------------------------------------

    /** Force flush the current MemTable to a Level 0 SST file. */
    public void flush() throws IOException {
        ensureOpen();
        if (memTable.isEmpty()) {
            return;
        }

        TreeMap<MemorySlice, byte[]> snapshot = memTable;
        memTable = new TreeMap<>(keyComparator);
        memTableSize = 0;

        SstFileMetadata metadata = writeMemTableToSst(snapshot);

        levels.get(0).add(0, metadata);

        LOG.info(
                "Flushed MemTable to L0 SST file: {}, entries: {}",
                metadata.getFile().getName(),
                snapshot.size());

        compactor.maybeCompact(levels, MAX_LEVELS, this::newSstFile);
    }

    /**
     * Force a full compaction of all levels into the deepest level. This merges all data and cleans
     * up tombstones (which are only removed at the max level), reducing the total number of SST
     * files to the minimum.
     */
    public void compact() throws IOException {
        ensureOpen();
        compactor.fullCompact(levels, MAX_LEVELS, this::newSstFile);
    }

    // -------------------------------------------------------------------------
    //  Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;

        // Flush remaining MemTable data to L0
        if (!memTable.isEmpty()) {
            TreeMap<MemorySlice, byte[]> snapshot = memTable;
            memTable = new TreeMap<>(keyComparator);
            memTableSize = 0;

            SstFileMetadata metadata = writeMemTableToSst(snapshot);
            levels.get(0).add(0, metadata);
        }

        // Close all cached readers
        for (SortLookupStoreReader reader : readerCache.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                LOG.warn("Failed to close cached reader during shutdown", e);
            }
        }
        readerCache.clear();

        LOG.info("SimpleLsmKvDb closed. Level stats: {}", getLevelStats());
    }

    /** Return the total number of SST files across all levels. */
    @VisibleForTesting
    int getSstFileCount() {
        int count = 0;
        for (List<SstFileMetadata> levelFiles : levels) {
            count += levelFiles.size();
        }
        return count;
    }

    /** Return the number of SST files at a specific level. */
    public int getLevelFileCount(int level) {
        if (level < 0 || level >= MAX_LEVELS) {
            return 0;
        }
        return levels.get(level).size();
    }

    /** Return the estimated MemTable size in bytes. */
    public long getMemTableSize() {
        return memTableSize;
    }

    /** Return a human-readable summary of file counts per level. */
    public String getLevelStats() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MAX_LEVELS; i++) {
            int count = levels.get(i).size();
            if (count > 0) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append("L").append(i).append("=").append(count);
            }
        }
        return sb.length() == 0 ? "empty" : sb.toString();
    }

    // -------------------------------------------------------------------------
    //  Internal Helpers
    // -------------------------------------------------------------------------

    private void maybeFlushMemTable() throws IOException {
        if (memTableSize >= memTableFlushThreshold) {
            flush();
        }
    }

    @Nullable
    private byte[] lookupInFile(File file, byte[] key) throws IOException {
        SortLookupStoreReader reader = readerCache.get(file);
        if (reader == null) {
            reader = storeFactory.createReader(file);
            readerCache.put(file, reader);
        }
        return reader.lookup(key);
    }

    @Nullable
    private SstFileMetadata findFileForKey(List<SstFileMetadata> sortedFiles, MemorySlice key) {
        int low = 0;
        int high = sortedFiles.size() - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            SstFileMetadata midFile = sortedFiles.get(mid);
            if (keyComparator.compare(key, midFile.getMinKey()) < 0) {
                high = mid - 1;
            } else if (keyComparator.compare(key, midFile.getMaxKey()) > 0) {
                low = mid + 1;
            } else {
                return midFile;
            }
        }
        return null;
    }

    private SstFileMetadata writeMemTableToSst(TreeMap<MemorySlice, byte[]> data)
            throws IOException {
        File sstFile = newSstFile();
        SortLookupStoreWriter writer = storeFactory.createWriter(sstFile, null);
        MemorySlice minKey = null;
        MemorySlice maxKey = null;
        long tombstoneCount = 0;
        try {
            for (Map.Entry<MemorySlice, byte[]> entry : data.entrySet()) {
                writer.put(entry.getKey().copyBytes(), entry.getValue());
                if (minKey == null) {
                    minKey = entry.getKey();
                }
                maxKey = entry.getKey();
                if (isTombstone(entry.getValue())) {
                    tombstoneCount++;
                }
            }
        } finally {
            writer.close();
        }
        return new SstFileMetadata(sstFile, minKey, maxKey, tombstoneCount, 0);
    }

    private File newSstFile() {
        long sequence = fileSequence++;
        return new File(dataDirectory, String.format("sst-%06d.db", sequence));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SimpleLsmKvDb is already closed");
        }
    }

    // -------------------------------------------------------------------------
    //  Builder
    // -------------------------------------------------------------------------

    /** Builder for {@link SimpleLsmKvDb}. */
    public static class Builder {

        private final File dataDirectory;
        private long memTableFlushThreshold = 64 * 1024 * 1024; // 64 MB
        private long maxSstFileSize = 8 * 1024 * 1024; // 8 MB
        private int blockSize = 32 * 1024; // 32 KB
        private int level0FileNumCompactTrigger = 4;
        private int sizeRatio = 10;
        private CacheManager cacheManager;
        private CompressOptions compressOptions = CompressOptions.defaultOptions();
        private Comparator<MemorySlice> keyComparator = MemorySlice::compareTo;

        Builder(File dataDirectory) {
            this.dataDirectory = dataDirectory;
        }

        /** Set the MemTable flush threshold in bytes. Default is 64 MB. */
        public Builder memTableFlushThreshold(long thresholdBytes) {
            this.memTableFlushThreshold = thresholdBytes;
            return this;
        }

        /** Set the maximum SST file size produced by compaction in bytes. Default is 8 MB. */
        public Builder maxSstFileSize(long maxSstFileSize) {
            this.maxSstFileSize = maxSstFileSize;
            return this;
        }

        /** Set the SST block size in bytes. Default is 32 KB. */
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /** Set the cache manager. */
        public Builder cacheManager(CacheManager cacheManager) {
            this.cacheManager = cacheManager;
            return this;
        }

        /** Set the level 0 file number that triggers compaction. Default is 4. */
        public Builder level0FileNumCompactTrigger(int fileNum) {
            this.level0FileNumCompactTrigger = fileNum;
            return this;
        }

        /**
         * Set the size ratio percentage for Universal Compaction. When the accumulated size of
         * newer runs divided by the next run's size is less than this percentage, the runs are
         * merged together. Default is 10 (meaning 10%).
         */
        public Builder sizeRatio(int sizeRatio) {
            this.sizeRatio = sizeRatio;
            return this;
        }

        /** Set compression options. Default is zstd level 1. */
        public Builder compressOptions(CompressOptions compressOptions) {
            this.compressOptions = compressOptions;
            return this;
        }

        /**
         * Set a custom key comparator. Default is unsigned lexicographic byte comparison.
         *
         * <p>The comparator must be consistent with the {@link SortLookupStoreFactory}'s comparator
         * so that SST file lookups return correct results.
         */
        public Builder keyComparator(Comparator<MemorySlice> keyComparator) {
            this.keyComparator = keyComparator;
            return this;
        }

        /** Build the {@link SimpleLsmKvDb} instance. */
        public SimpleLsmKvDb build() {
            if (!dataDirectory.exists()) {
                boolean created = dataDirectory.mkdirs();
                if (!created) {
                    throw new IllegalStateException(
                            "Failed to create data directory: " + dataDirectory);
                }
            }

            if (cacheManager == null) {
                cacheManager = new CacheManager(MemorySize.ofMebiBytes(8));
            }
            SortLookupStoreFactory factory =
                    new SortLookupStoreFactory(
                            keyComparator, cacheManager, blockSize, compressOptions);

            return new SimpleLsmKvDb(
                    dataDirectory,
                    factory,
                    keyComparator,
                    memTableFlushThreshold,
                    maxSstFileSize,
                    level0FileNumCompactTrigger,
                    sizeRatio);
        }
    }

    static boolean isTombstone(byte[] value) {
        return value.length == 0;
    }
}
