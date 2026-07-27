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
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongFunction;

import static org.apache.paimon.utils.Preconditions.checkArgument;

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
 * <p>Compaction is scheduled on a background thread when the number of sorted runs exceeds a
 * threshold. Runs are selected for merging based on size ratios between adjacent runs, following
 * RocksDB's Universal Compaction strategy. MemTable flushes remain synchronous; writes wait for
 * background compaction only when too many Level-0 files accumulate.
 *
 * <p>Note: No WAL is implemented. Data in the MemTable will be lost on crash.
 *
 * <p>This class is <b>not</b> thread-safe. External synchronization is required if accessed from
 * multiple threads.
 */
public class LocalKvDb implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LocalKvDb.class);

    /** Tombstone marker for deleted keys. */
    private static final byte[] TOMBSTONE = new byte[0];

    /** Maximum number of levels in the LSM tree. */
    static final int MAX_LEVELS = 4;

    /** Marker used when an SST's entry count cannot be estimated before writing. */
    static final long UNKNOWN_NUM_ENTRIES = -1;

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
    private final String uuid;
    private final SortLookupStoreFactory storeFactory;
    private final LongFunction<BloomFilter.Builder> bloomFilterBuilderFactory;
    private final Comparator<MemorySlice> keyComparator;
    private final long memTableFlushThreshold;
    private final long maxSstFileSize;
    private final int level0FileNumCompactTrigger;
    private final LsmCompactor compactor;
    private final ExecutorService compactionExecutor;
    private final ReentrantReadWriteLock levelsLock;
    private final ThreadLocal<List<File>> deferredCompactionDeletes;

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

    private final AtomicLong fileSequence;
    @Nullable private Future<?> compactionFuture;
    private boolean closed;

    private LocalKvDb(
            File dataDirectory,
            SortLookupStoreFactory storeFactory,
            LongFunction<BloomFilter.Builder> bloomFilterBuilderFactory,
            Comparator<MemorySlice> keyComparator,
            long memTableFlushThreshold,
            long maxSstFileSize,
            int level0FileNumCompactTrigger,
            int sizeRatio,
            ExecutorService compactionExecutor) {
        this.dataDirectory = dataDirectory;
        this.uuid = UUID.randomUUID().toString();
        this.storeFactory = storeFactory;
        this.bloomFilterBuilderFactory = bloomFilterBuilderFactory;
        this.keyComparator = keyComparator;
        this.memTableFlushThreshold = memTableFlushThreshold;
        this.maxSstFileSize = maxSstFileSize;
        this.level0FileNumCompactTrigger = level0FileNumCompactTrigger;
        this.compactionExecutor = compactionExecutor;
        this.levelsLock = new ReentrantReadWriteLock();
        this.deferredCompactionDeletes = new ThreadLocal<>();
        this.memTable = new TreeMap<>(keyComparator);
        this.memTableSize = 0;
        this.levels = new ArrayList<>();
        for (int i = 0; i < MAX_LEVELS; i++) {
            this.levels.add(new ArrayList<>());
        }
        this.readerCache = new HashMap<>();
        this.fileSequence = new AtomicLong();
        this.compactionFuture = null;
        this.closed = false;
        this.compactor =
                new LsmCompactor(
                        keyComparator,
                        storeFactory,
                        bloomFilterBuilderFactory,
                        maxSstFileSize,
                        level0FileNumCompactTrigger,
                        sizeRatio,
                        this::deferOrDeleteCompactedFile);
    }

    /**
     * Defer deletion while a compaction is building its private level snapshot. Files are only
     * deleted after the compacted snapshot is atomically published.
     */
    private void deferOrDeleteCompactedFile(File file) {
        List<File> deferredDeletes = deferredCompactionDeletes.get();
        if (deferredDeletes != null) {
            deferredDeletes.add(file);
            return;
        }
        closeAndDeleteSstFile(file);
    }

    /** Close the cached reader for the given SST file (if any) and delete the file from disk. */
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

    /** Create a builder for {@link LocalKvDb}. */
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
        checkCompactionFailure();
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
        checkCompactionFailure();
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

    /**
     * Bulk-load globally sorted entries directly into SST files at the deepest level, bypassing
     * MemTable, flush, and compaction entirely. The database must be empty when this is called.
     *
     * @param sortedEntries an iterator of key-value pairs in sorted order (by the DB's key
     *     comparator)
     * @param numEntries number of entries in the iterator
     */
    public void bulkLoad(Iterator<Map.Entry<byte[], byte[]>> sortedEntries, long numEntries)
            throws IOException {
        ensureOpen();
        checkCompactionFailure();
        checkArgument(numEntries >= 0, "numEntries must be non-negative.");
        if (!memTable.isEmpty() || getSstFileCount() > 0) {
            throw new IllegalStateException(
                    "bulkLoad requires an empty database (no memTable entries and no SST files)");
        }

        int targetLevel = MAX_LEVELS - 1;
        List<SstFileMetadata> bulkLoadFiles = new ArrayList<>();

        SortLookupStoreWriter currentWriter = null;
        File currentSstFile = null;
        MemorySlice currentFileMinKey = null;
        MemorySlice currentFileMaxKey = null;
        MemorySlice previousFileMaxKey = null;
        long currentBatchSize = 0;
        long loadedEntries = 0;
        long loadedBytes = 0;

        try {
            while (sortedEntries.hasNext()) {
                checkArgument(
                        loadedEntries < numEntries,
                        "The iterator contains more entries than numEntries (%s).",
                        numEntries);
                Map.Entry<byte[], byte[]> entry = sortedEntries.next();
                byte[] key = entry.getKey();
                byte[] value = entry.getValue();
                MemorySlice currentKey = MemorySlice.wrap(key);
                long entrySize = (long) key.length + value.length;

                if (currentWriter == null) {
                    currentSstFile = newSstFile();
                    long expectedEntries =
                            estimateBulkLoadSstEntries(
                                    numEntries - loadedEntries,
                                    loadedEntries,
                                    loadedBytes,
                                    entrySize);
                    currentWriter =
                            storeFactory.createWriter(
                                    currentSstFile,
                                    bloomFilterBuilderFactory.apply(expectedEntries));
                    currentFileMinKey = currentKey;
                    currentBatchSize = 0;
                }

                currentWriter.put(key, value);
                currentFileMaxKey = currentKey;
                currentBatchSize += entrySize;
                loadedEntries++;
                loadedBytes += entrySize;

                if (currentBatchSize >= maxSstFileSize) {
                    currentWriter.close();
                    currentWriter = null;
                    previousFileMaxKey =
                            addBulkLoadSstFile(
                                    bulkLoadFiles,
                                    currentSstFile,
                                    currentFileMinKey,
                                    currentFileMaxKey,
                                    previousFileMaxKey,
                                    targetLevel);
                    currentSstFile = null;
                    currentFileMinKey = null;
                    currentFileMaxKey = null;
                }
            }

            if (currentWriter != null) {
                currentWriter.close();
                currentWriter = null;
                addBulkLoadSstFile(
                        bulkLoadFiles,
                        currentSstFile,
                        currentFileMinKey,
                        currentFileMaxKey,
                        previousFileMaxKey,
                        targetLevel);
            }

            checkArgument(
                    loadedEntries == numEntries,
                    "The iterator contains %s entries, but numEntries is %s.",
                    loadedEntries,
                    numEntries);
        } catch (IOException | RuntimeException e) {
            if (currentWriter != null) {
                try {
                    currentWriter.close();
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            if (currentSstFile != null) {
                deleteFileQuietly(currentSstFile);
            }
            for (SstFileMetadata metadata : bulkLoadFiles) {
                deleteFileQuietly(metadata.getFile());
            }
            throw e;
        }

        levelsLock.writeLock().lock();
        try {
            levels.get(targetLevel).addAll(bulkLoadFiles);
        } finally {
            levelsLock.writeLock().unlock();
        }

        LOG.info(
                "Bulk-loaded {} SST files directly to level {}", bulkLoadFiles.size(), targetLevel);
    }

    private long estimateBulkLoadSstEntries(
            long remainingEntries, long loadedEntries, long loadedBytes, long firstEntrySize) {
        long averageEntrySize =
                loadedEntries == 0 ? firstEntrySize : divideRoundUp(loadedBytes, loadedEntries);
        long estimatedEntries =
                Math.max(1, divideRoundUp(maxSstFileSize, Math.max(1, averageEntrySize)));
        // Leave headroom for entries smaller than the observed average. Underestimating is safe,
        // but increases the actual Bloom filter false-positive probability.
        long headroom = divideRoundUp(estimatedEntries, 4);
        estimatedEntries =
                Long.MAX_VALUE - estimatedEntries < headroom
                        ? Long.MAX_VALUE
                        : estimatedEntries + headroom;
        return Math.min(remainingEntries, estimatedEntries);
    }

    private static long divideRoundUp(long dividend, long divisor) {
        return dividend / divisor + (dividend % divisor == 0 ? 0 : 1);
    }

    private MemorySlice addBulkLoadSstFile(
            List<SstFileMetadata> targetLevelFiles,
            File currentSstFile,
            MemorySlice currentFileMinKey,
            MemorySlice currentFileMaxKey,
            @Nullable MemorySlice previousFileMaxKey,
            int targetLevel) {
        if (keyComparator.compare(currentFileMinKey, currentFileMaxKey) > 0) {
            throw new IllegalArgumentException(
                    "bulkLoad requires entries sorted by the configured key comparator; "
                            + "generated SST min key is greater than max key.");
        }
        if (previousFileMaxKey != null
                && keyComparator.compare(previousFileMaxKey, currentFileMinKey) > 0) {
            throw new IllegalArgumentException(
                    "bulkLoad requires entries sorted by the configured key comparator; "
                            + "generated SST key ranges are not ordered.");
        }
        targetLevelFiles.add(
                new SstFileMetadata(
                        currentSstFile, currentFileMinKey, currentFileMaxKey, 0, targetLevel));
        return currentFileMaxKey;
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
        checkCompactionFailure();

        // 1. Search MemTable first (newest data)
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        byte[] memValue = memTable.get(wrappedKey);
        if (memValue != null) {
            return isTombstone(memValue) ? null : memValue;
        }

        // 2. Search each level from L0 to Lmax. Hold the read lock until lookups finish so
        // compaction cannot publish a new snapshot and delete an SST being read.
        levelsLock.readLock().lock();
        try {
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
        } finally {
            levelsLock.readLock().unlock();
        }

        return null;
    }

    // -------------------------------------------------------------------------
    //  Flush & Compaction
    // -------------------------------------------------------------------------

    /**
     * Force flush the current MemTable to a Level-0 SST file and schedule compaction when needed.
     *
     * <p>The MemTable write is synchronous. Compaction runs in the background unless the number of
     * Level-0 files reaches the backpressure threshold.
     */
    public void flush() throws IOException {
        ensureOpen();
        checkCompactionFailure();
        if (memTable.isEmpty()) {
            return;
        }

        flushMemTable();
        scheduleCompactionIfNeeded();
        applyCompactionBackpressure();
    }

    private void flushMemTable() throws IOException {
        TreeMap<MemorySlice, byte[]> snapshot = memTable;
        memTable = new TreeMap<>(keyComparator);
        memTableSize = 0;

        SstFileMetadata metadata = writeMemTableToSst(snapshot);

        levelsLock.writeLock().lock();
        try {
            levels.get(0).add(0, metadata);
        } finally {
            levelsLock.writeLock().unlock();
        }

        LOG.info(
                "Flushed MemTable to L0 SST file: {}, entries: {}",
                metadata.getFile().getName(),
                snapshot.size());
    }

    /**
     * Force a full compaction of all levels into the deepest level. This merges all data and cleans
     * up tombstones (which are only removed at the max level), reducing the total number of SST
     * files to the minimum.
     */
    public void compact() throws IOException {
        ensureOpen();
        checkCompactionFailure();
        awaitScheduledCompaction();
        compactLevelSnapshot(true);
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

        IOException failure = null;
        try {
            checkCompactionFailure();
        } catch (IOException e) {
            failure = e;
        }
        try {
            if (!memTable.isEmpty()) {
                flushMemTable();
            }
            if (failure == null) {
                scheduleCompactionIfNeeded();
                awaitScheduledCompaction();
            }
        } catch (IOException e) {
            failure = addOrSuppress(failure, e);
        } finally {
            compactionExecutor.shutdown();
            try {
                if (!compactionExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    compactionExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                compactionExecutor.shutdownNow();
                failure = addOrSuppress(failure, new IOException("Interrupted while closing.", e));
            }

            levelsLock.writeLock().lock();
            try {
                for (SortLookupStoreReader reader : readerCache.values()) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        LOG.warn("Failed to close cached reader during shutdown", e);
                    }
                }
                readerCache.clear();
            } finally {
                levelsLock.writeLock().unlock();
            }
        }

        LOG.info("LocalKvDb closed. Level stats: {}", getLevelStats());
        if (failure != null) {
            throw failure;
        }
    }

    /** Return the total number of SST files across all levels. */
    @VisibleForTesting
    int getSstFileCount() {
        levelsLock.readLock().lock();
        try {
            int count = 0;
            for (List<SstFileMetadata> levelFiles : levels) {
                count += levelFiles.size();
            }
            return count;
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    /** Return the number of SST files at a specific level. */
    public int getLevelFileCount(int level) {
        if (level < 0 || level >= MAX_LEVELS) {
            return 0;
        }
        levelsLock.readLock().lock();
        try {
            return levels.get(level).size();
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    /** Return the estimated MemTable size in bytes. */
    public long getMemTableSize() {
        return memTableSize;
    }

    /** Return a human-readable summary of file counts per level. */
    public String getLevelStats() {
        levelsLock.readLock().lock();
        try {
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
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    // -------------------------------------------------------------------------
    //  Internal Helpers
    // -------------------------------------------------------------------------

    private void scheduleCompactionIfNeeded() throws IOException {
        checkCompactionFailure();
        if (!needsCompaction() || compactionFuture != null) {
            return;
        }

        try {
            compactionFuture =
                    compactionExecutor.submit(
                            () -> {
                                while (needsCompaction()) {
                                    compactLevelSnapshot(false);
                                }
                                return null;
                            });
        } catch (RuntimeException e) {
            throw new IOException("Failed to schedule background compaction.", e);
        }
    }

    private void applyCompactionBackpressure() throws IOException {
        if ((long) getLevelFileCount(0) >= (long) level0FileNumCompactTrigger * 2) {
            awaitScheduledCompaction();
        }
    }

    private boolean needsCompaction() {
        levelsLock.readLock().lock();
        try {
            return levels.get(0).size() >= level0FileNumCompactTrigger;
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    private void compactLevelSnapshot(boolean fullCompaction) throws IOException {
        List<List<SstFileMetadata>> originalLevels = copyLevels();
        if (!fullCompaction && originalLevels.get(0).size() < level0FileNumCompactTrigger) {
            return;
        }

        List<List<SstFileMetadata>> compactedLevels = copyLevels(originalLevels);
        List<File> generatedFiles = new ArrayList<>();
        List<File> compactedFiles = new ArrayList<>();
        boolean published = false;
        deferredCompactionDeletes.set(compactedFiles);
        try {
            LsmCompactor.FileSupplier fileSupplier =
                    () -> {
                        File file = newSstFile();
                        generatedFiles.add(file);
                        return file;
                    };
            if (fullCompaction) {
                compactor.fullCompact(compactedLevels, MAX_LEVELS, fileSupplier);
            } else {
                compactor.maybeCompact(compactedLevels, MAX_LEVELS, fileSupplier);
            }
            publishCompactedLevels(originalLevels, compactedLevels, compactedFiles);
            published = true;
        } finally {
            deferredCompactionDeletes.remove();
            if (!published) {
                for (File generatedFile : generatedFiles) {
                    deleteFileQuietly(generatedFile);
                }
            }
        }
    }

    private List<List<SstFileMetadata>> copyLevels() {
        levelsLock.readLock().lock();
        try {
            return copyLevels(levels);
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    private static List<List<SstFileMetadata>> copyLevels(
            List<List<SstFileMetadata>> levelsToCopy) {
        List<List<SstFileMetadata>> copy = new ArrayList<>(levelsToCopy.size());
        for (List<SstFileMetadata> level : levelsToCopy) {
            copy.add(new ArrayList<>(level));
        }
        return copy;
    }

    private void publishCompactedLevels(
            List<List<SstFileMetadata>> originalLevels,
            List<List<SstFileMetadata>> compactedLevels,
            List<File> compactedFiles)
            throws IOException {
        Set<File> originalFiles = filesInLevels(originalLevels);

        levelsLock.writeLock().lock();
        try {
            List<SstFileMetadata> newLevelZeroFiles = new ArrayList<>();
            for (SstFileMetadata metadata : levels.get(0)) {
                if (!originalFiles.contains(metadata.getFile())) {
                    newLevelZeroFiles.add(metadata);
                }
            }

            for (int level = 1; level < MAX_LEVELS; level++) {
                for (SstFileMetadata metadata : levels.get(level)) {
                    if (!originalFiles.contains(metadata.getFile())) {
                        throw new IOException(
                                "Unexpected concurrent update to level " + level + ".");
                    }
                }
            }

            for (int level = 0; level < MAX_LEVELS; level++) {
                levels.get(level).clear();
                if (level == 0) {
                    levels.get(level).addAll(newLevelZeroFiles);
                }
                levels.get(level).addAll(compactedLevels.get(level));
            }

            for (File compactedFile : compactedFiles) {
                closeAndDeleteSstFile(compactedFile);
            }
        } finally {
            levelsLock.writeLock().unlock();
        }
    }

    private static Set<File> filesInLevels(List<List<SstFileMetadata>> levels) {
        Set<File> files = new HashSet<>();
        for (List<SstFileMetadata> level : levels) {
            for (SstFileMetadata metadata : level) {
                files.add(metadata.getFile());
            }
        }
        return files;
    }

    private void checkCompactionFailure() throws IOException {
        Future<?> future = compactionFuture;
        if (future != null && future.isDone()) {
            awaitCompactionFuture(future);
            if (compactionFuture == future) {
                compactionFuture = null;
            }
        }
    }

    private void awaitScheduledCompaction() throws IOException {
        Future<?> future = compactionFuture;
        if (future == null) {
            return;
        }
        awaitCompactionFuture(future);
        if (compactionFuture == future) {
            compactionFuture = null;
        }
    }

    private static void awaitCompactionFuture(Future<?> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for background compaction.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Background compaction failed.", cause);
        } catch (CancellationException e) {
            throw new IOException("Background compaction was cancelled.", e);
        }
    }

    @VisibleForTesting
    void awaitCompaction() throws IOException {
        ensureOpen();
        awaitScheduledCompaction();
    }

    private static IOException addOrSuppress(
            @Nullable IOException current, IOException additional) {
        if (current == null) {
            return additional;
        }
        current.addSuppressed(additional);
        return current;
    }

    private void deleteFileQuietly(File file) {
        if (file.exists() && !file.delete()) {
            LOG.warn("Failed to delete SST file: {}", file.getName());
        }
    }

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
        SortLookupStoreWriter writer =
                storeFactory.createWriter(sstFile, bloomFilterBuilderFactory.apply(data.size()));
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
        long sequence = fileSequence.getAndIncrement();
        return new File(dataDirectory, String.format("sst-%s-%06d.db", uuid, sequence));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LocalKvDb is already closed");
        }
    }

    // -------------------------------------------------------------------------
    //  Builder
    // -------------------------------------------------------------------------

    /** Builder for {@link LocalKvDb}. */
    public static class Builder {

        private final File dataDirectory;
        private long memTableFlushThreshold = 64 * 1024 * 1024; // 64 MB
        private long maxSstFileSize = 8 * 1024 * 1024; // 8 MB
        private int blockSize = 4 * 1024; // 4 KB
        private int level0FileNumCompactTrigger = 4;
        private int sizeRatio = 10;
        private CacheManager cacheManager;
        private CompressOptions compressOptions = CompressOptions.defaultOptions();
        private Comparator<MemorySlice> keyComparator = MemorySlice::compareTo;
        private boolean bloomFilterEnabled = true;
        private double bloomFilterFpp = 0.1;
        @Nullable private ExecutorService compactionExecutor;

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

        /** Set the SST block size in bytes. Default is 4 KB. */
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

        /** Enable or disable per-SST Bloom filters. Enabled by default. */
        public Builder bloomFilterEnabled(boolean bloomFilterEnabled) {
            this.bloomFilterEnabled = bloomFilterEnabled;
            return this;
        }

        /** Set the Bloom filter false positive probability. Default is 0.1. */
        public Builder bloomFilterFpp(double bloomFilterFpp) {
            checkArgument(
                    bloomFilterFpp > 0 && bloomFilterFpp < 1,
                    "Bloom filter false positive probability must be between 0 and 1.");
            this.bloomFilterFpp = bloomFilterFpp;
            return this;
        }

        @VisibleForTesting
        Builder compactionExecutor(ExecutorService compactionExecutor) {
            this.compactionExecutor = compactionExecutor;
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

        /** Build the {@link LocalKvDb} instance. */
        public LocalKvDb build() {
            if (!dataDirectory.exists()) {
                boolean created = dataDirectory.mkdirs();
                if (!created) {
                    throw new IllegalStateException(
                            "Failed to create data directory: " + dataDirectory);
                }
            }

            if (cacheManager == null) {
                cacheManager = new CacheManager(MemorySize.ofMebiBytes(8), 0);
            }
            SortLookupStoreFactory factory =
                    new SortLookupStoreFactory(
                            keyComparator, cacheManager, blockSize, compressOptions);
            LongFunction<BloomFilter.Builder> bloomFilterBuilderFactory =
                    bloomFilterEnabled
                            ? expectedEntries ->
                                    expectedEntries > 0
                                            ? BloomFilter.fixedBuilder(
                                                    expectedEntries, bloomFilterFpp)
                                            : BloomFilter.dynamicBuilder(bloomFilterFpp)
                            : expectedEntries -> null;
            ExecutorService executor =
                    compactionExecutor == null
                            ? Executors.newSingleThreadExecutor(
                                    new ExecutorThreadFactory("local-kv-db-compaction"))
                            : compactionExecutor;

            return new LocalKvDb(
                    dataDirectory,
                    factory,
                    bloomFilterBuilderFactory,
                    keyComparator,
                    memTableFlushThreshold,
                    maxSstFileSize,
                    level0FileNumCompactTrigger,
                    sizeRatio,
                    executor);
        }
    }

    static boolean isTombstone(byte[] value) {
        return value.length == 0;
    }
}
