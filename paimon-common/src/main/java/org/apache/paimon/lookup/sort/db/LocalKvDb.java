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
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.KeyValueIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.function.Predicate;

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
 * <p>By default, compaction runs synchronously when the number of sorted runs exceeds a threshold.
 * When a compaction executor is configured, compaction is scheduled on that executor. Runs are
 * selected for merging based on size ratios between adjacent runs, following RocksDB's Universal
 * Compaction strategy. MemTable flushes remain synchronous; asynchronous writes wait for background
 * compaction only when too many Level-0 files accumulate.
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

    /** Bound open SST inputs while retaining one hot reader per LSM level. */
    static final int MAX_CACHED_READERS = MAX_LEVELS;

    /** Initial number of values retained by a lazy MemTable merge. */
    private static final int MERGE_VALUES_INITIAL_CAPACITY = 32;

    /** Approximate retained memory for a lazy merge function and its initial reference array. */
    private static final long MEMTABLE_MERGE_FUNCTION_OVERHEAD =
            64L + MERGE_VALUES_INITIAL_CAPACITY * Long.BYTES;

    /** Approximate array header and reference overhead for each retained merge value. */
    private static final long MEMTABLE_MERGED_VALUE_OVERHEAD = 24;

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
    private final LsmLevels levels;
    private final LsmCompactor compaction;
    @Nullable private final MergeOperator mergeOperator;

    /** Active MemTable: key -> value bytes, tombstone, or lazy merge function. */
    private TreeMap<MemorySlice, Object> memTable;

    /** Estimated size of the current MemTable in bytes. */
    private long memTableSize;

    /** Cached readers for SST files, keyed by file path. Lazily populated on first lookup. */
    private final Map<File, SortLookupStoreReader> readerCache;

    private final AtomicLong fileSequence;
    @Nullable private BulkLoadWriter activeBulkLoadWriter;
    private int openRangeIterators;
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
            @Nullable Predicate<byte[]> expiredValuePredicate,
            @Nullable MergeOperator mergeOperator,
            @Nullable ExecutorService compactionExecutor) {
        this.dataDirectory = dataDirectory;
        this.uuid = UUID.randomUUID().toString();
        this.storeFactory = storeFactory;
        this.bloomFilterBuilderFactory = bloomFilterBuilderFactory;
        this.keyComparator = keyComparator;
        this.memTableFlushThreshold = memTableFlushThreshold;
        this.maxSstFileSize = maxSstFileSize;
        this.memTable = new TreeMap<>(keyComparator);
        this.memTableSize = 0;
        this.levels = new LsmLevels(MAX_LEVELS);
        this.readerCache = new LinkedHashMap<>(16, 0.75f, true);
        this.fileSequence = new AtomicLong();
        this.activeBulkLoadWriter = null;
        this.openRangeIterators = 0;
        this.closed = false;
        this.mergeOperator = mergeOperator;
        LsmCompactor.CompactorFactory compactorFactory =
                fileDeleter ->
                        new UniversalCompactor(
                                keyComparator,
                                storeFactory,
                                bloomFilterBuilderFactory,
                                maxSstFileSize,
                                level0FileNumCompactTrigger,
                                sizeRatio,
                                expiredValuePredicate,
                                mergeOperator,
                                fileDeleter);
        this.compaction =
                compactionExecutor == null
                        ? new SyncLsmCompactor(
                                levels,
                                compactorFactory,
                                level0FileNumCompactTrigger,
                                this::newSstFile,
                                this::closeAndDeleteSstFile)
                        : new AsyncLsmCompactor(
                                levels,
                                compactorFactory,
                                level0FileNumCompactTrigger,
                                this::newSstFile,
                                this::closeAndDeleteSstFile,
                                compactionExecutor);
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
        ensureNoBulkLoad();
        ensureNoRangeIterator();
        checkCompactionFailure();
        if (value.length == 0) {
            throw new IllegalArgumentException(
                    "Value must not be an empty byte array, which is reserved as TOMBSTONE marker. "
                            + "Use delete() to remove a key.");
        }
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        if (mergeOperator != null) {
            Map.Entry<MemorySlice, Object> previous = memTable.lowerEntry(wrappedKey);
            if (previous != null
                    && !isMemTableTombstone(previous.getValue())
                    && mergeOperator.canMerge(previous.getKey(), wrappedKey)) {
                Object previousValue = previous.getValue();
                long previousSize = estimatedMemTableValueSize(previousValue);
                MemTableMergeFunction mergeFunction;
                if (previousValue instanceof MemTableMergeFunction) {
                    mergeFunction = (MemTableMergeFunction) previousValue;
                } else {
                    mergeFunction = new MemTableMergeFunction(mergeOperator);
                    mergeFunction.reset((byte[]) previousValue);
                }
                mergeFunction.add(value);
                memTableSize += mergeFunction.estimatedSize() - previousSize;
                memTable.put(previous.getKey(), mergeFunction);
                maybeFlushMemTable();
                return;
            }
        }
        Object oldValue = memTable.put(wrappedKey, value);
        long delta = key.length + value.length;
        if (oldValue != null) {
            delta -= (key.length + estimatedMemTableValueSize(oldValue));
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
        ensureNoBulkLoad();
        ensureNoRangeIterator();
        checkCompactionFailure();
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        Object oldValue = memTable.put(wrappedKey, TOMBSTONE);
        long delta = key.length;
        if (oldValue != null) {
            delta -= (key.length + estimatedMemTableValueSize(oldValue));
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
        checkArgument(numEntries >= 0, "numEntries must be non-negative.");
        try (BulkLoadWriter writer = createBulkLoadWriter(numEntries)) {
            long loadedEntries = 0;
            while (sortedEntries.hasNext()) {
                checkArgument(
                        loadedEntries < numEntries,
                        "The iterator contains more entries than numEntries (%s).",
                        numEntries);
                Map.Entry<byte[], byte[]> entry = sortedEntries.next();
                writer.put(entry.getKey(), entry.getValue());
                loadedEntries++;
            }
            checkArgument(
                    loadedEntries == numEntries,
                    "The iterator contains %s entries, but numEntries is %s.",
                    loadedEntries,
                    numEntries);
            writer.finish();
        }
    }

    /**
     * Create a streaming bulk-load writer. Keys must be written in strictly increasing order
     * according to the configured key comparator. The database must be empty.
     */
    public BulkLoadWriter createBulkLoadWriter() throws IOException {
        return createBulkLoadWriter(UNKNOWN_NUM_ENTRIES);
    }

    private BulkLoadWriter createBulkLoadWriter(long expectedEntries) throws IOException {
        ensureOpen();
        ensureNoRangeIterator();
        checkCompactionFailure();
        if (activeBulkLoadWriter != null) {
            throw new IllegalStateException("Another bulk load is already in progress.");
        }
        if (!memTable.isEmpty() || getSstFileCount() > 0) {
            throw new IllegalStateException(
                    "bulkLoad requires an empty database (no memTable entries and no SST files)");
        }
        BulkLoadWriter writer = new BulkLoadWriter(expectedEntries);
        activeBulkLoadWriter = writer;
        return writer;
    }

    /** A streaming writer which atomically publishes bulk-loaded SST files on {@link #finish()}. */
    public final class BulkLoadWriter implements Closeable {

        private final long expectedEntries;
        private final int targetLevel;
        private final List<SstFileMetadata> bulkLoadFiles;

        @Nullable private SortLookupStoreWriter currentWriter;
        @Nullable private File currentSstFile;
        @Nullable private MemorySlice currentFileMinKey;
        @Nullable private MemorySlice currentFileMaxKey;
        @Nullable private MemorySlice previousFileMaxKey;
        @Nullable private MemorySlice previousKey;
        private long currentBatchSize;
        private long loadedEntries;
        private long loadedBytes;
        private boolean active;

        private BulkLoadWriter(long expectedEntries) {
            this.expectedEntries = expectedEntries;
            this.targetLevel = MAX_LEVELS - 1;
            this.bulkLoadFiles = new ArrayList<>();
            this.active = true;
        }

        /** Write one key-value pair. */
        public void put(byte[] key, byte[] value) throws IOException {
            ensureActive();
            try {
                checkArgument(
                        expectedEntries < 0 || loadedEntries < expectedEntries,
                        "The bulk load contains more entries than expected (%s).",
                        expectedEntries);
                checkArgument(
                        value.length > 0,
                        "Value must not be empty, which is reserved as the tombstone marker.");
                MemorySlice currentKey = MemorySlice.wrap(key);
                checkArgument(
                        previousKey == null || keyComparator.compare(previousKey, currentKey) < 0,
                        "bulkLoad requires entries sorted in strictly increasing order according "
                                + "to the configured key comparator; generated SST key ranges "
                                + "must be ordered.");
                long entrySize = (long) key.length + value.length;

                if (currentWriter == null) {
                    currentSstFile = newSstFile();
                    long expectedEntries =
                            this.expectedEntries < 0
                                    ? UNKNOWN_NUM_ENTRIES
                                    : estimateBulkLoadSstEntries(
                                            this.expectedEntries - loadedEntries,
                                            loadedEntries,
                                            loadedBytes,
                                            entrySize);
                    currentWriter =
                            storeFactory.createWriter(
                                    currentSstFile,
                                    bloomFilterBuilderFactory.apply(expectedEntries));
                    currentFileMinKey = copyKey(key);
                    currentBatchSize = 0;
                }

                currentWriter.put(key, value);
                currentFileMaxKey = copyKey(key);
                previousKey = currentFileMaxKey;
                currentBatchSize += entrySize;
                loadedEntries++;
                loadedBytes += entrySize;

                if (currentBatchSize >= maxSstFileSize) {
                    closeCurrentFile();
                }
            } catch (IOException | RuntimeException e) {
                abort(e);
                throw e;
            }
        }

        /** Finish writing and publish all generated SST files. */
        public void finish() throws IOException {
            ensureActive();
            try {
                if (currentWriter != null) {
                    closeCurrentFile();
                }
                checkArgument(
                        expectedEntries < 0 || loadedEntries == expectedEntries,
                        "The bulk load contains %s entries, but expected %s.",
                        loadedEntries,
                        expectedEntries);
                levels.addFiles(targetLevel, bulkLoadFiles);
                active = false;
                release();
            } catch (IOException | RuntimeException e) {
                abort(e);
                throw e;
            }

            LOG.info(
                    "Bulk-loaded {} SST files directly to level {}",
                    bulkLoadFiles.size(),
                    targetLevel);
        }

        private void closeCurrentFile() throws IOException {
            if (currentWriter != null) {
                currentWriter.close();
                currentWriter = null;
            }
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

        private void abort(Throwable cause) {
            if (!active) {
                return;
            }
            if (currentWriter != null) {
                try {
                    currentWriter.close();
                } catch (IOException e) {
                    cause.addSuppressed(e);
                }
                currentWriter = null;
            }
            if (currentSstFile != null) {
                deleteFileQuietly(currentSstFile);
                currentSstFile = null;
            }
            for (SstFileMetadata metadata : bulkLoadFiles) {
                deleteFileQuietly(metadata.getFile());
            }
            bulkLoadFiles.clear();
            active = false;
            release();
        }

        private void release() {
            if (activeBulkLoadWriter == this) {
                activeBulkLoadWriter = null;
            }
        }

        private void ensureActive() {
            if (!active) {
                throw new IllegalStateException("Bulk-load writer is already closed.");
            }
        }

        @Override
        public void close() {
            abort(new IOException("Bulk load was closed before finish."));
        }
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
        ensureNoBulkLoad();
        checkCompactionFailure();

        // 1. Search MemTable first (newest data)
        MemorySlice wrappedKey = MemorySlice.wrap(key);
        Object memTableValue = memTable.get(wrappedKey);
        byte[] memValue = memTableValue == null ? null : materializeMemTableValue(memTableValue);
        if (memValue != null) {
            return isTombstone(memValue) ? null : memValue;
        }

        // 2. Search each level from L0 to Lmax.
        byte[] value = levels.lookup(key, wrappedKey, keyComparator, this::lookupInFile);
        return value == null || isTombstone(value) ? null : value;
    }

    /**
     * Scan live entries in the half-open range [{@code fromInclusive}, {@code toExclusive}).
     *
     * <p>The result is sorted by the configured key comparator. Newer values and tombstones shadow
     * older versions in the same way as {@link #get(byte[])}. A null upper bound scans to the end
     * of the database.
     */
    public List<Map.Entry<byte[], byte[]>> rangeScan(
            byte[] fromInclusive, @Nullable byte[] toExclusive) throws IOException {
        List<Map.Entry<byte[], byte[]>> result = new ArrayList<>();
        forEachInRange(
                fromInclusive,
                toExclusive,
                (key, value) ->
                        result.add(
                                new AbstractMap.SimpleImmutableEntry<>(
                                        key.copyBytes(), value.copyBytes())));
        return result;
    }

    /**
     * Visit live entries in the half-open range [{@code fromInclusive}, {@code toExclusive}).
     *
     * <p>Entries are visited in key order with the same shadowing rules as {@link #get(byte[])}.
     * The supplied slices are only valid for the duration of the callback and must be copied if
     * retained.
     */
    public void forEachInRange(
            byte[] fromInclusive, @Nullable byte[] toExclusive, RangeEntryConsumer consumer)
            throws IOException {
        try (RangeIterator iterator = rangeIterator(fromInclusive, toExclusive)) {
            while (iterator.advanceNext()) {
                consumer.accept(iterator.getKey(), iterator.getValue());
            }
        }
    }

    /**
     * Create a lazy iterator over live entries in the half-open range [{@code fromInclusive},
     * {@code toExclusive}).
     *
     * <p>The iterator merges the MemTable and all overlapping SST files in key order. For duplicate
     * keys, the newest source wins and tombstones suppress older values. The iterator must be
     * closed before modifying or closing the database. Closing releases the levels read lock so a
     * concurrent compaction can publish its result.
     */
    public RangeIterator rangeIterator(byte[] fromInclusive, @Nullable byte[] toExclusive)
            throws IOException {
        ensureOpen();
        ensureNoBulkLoad();
        checkCompactionFailure();

        MemorySlice from = MemorySlice.wrap(fromInclusive);
        MemorySlice to = toExclusive == null ? null : MemorySlice.wrap(toExclusive);
        checkArgument(
                to == null || keyComparator.compare(from, to) <= 0,
                "Range start must not be greater than range end.");

        Map<MemorySlice, Object> memoryEntries =
                to == null ? memTable.tailMap(from, true) : memTable.subMap(from, true, to, false);
        LsmLevels.RangeSnapshot snapshot = levels.openRangeSnapshot(from, to, keyComparator);
        try {
            RangeIterator iterator = new RangeIterator(snapshot, memoryEntries, fromInclusive, to);
            openRangeIterators++;
            return iterator;
        } catch (IOException | RuntimeException e) {
            snapshot.close();
            throw e;
        }
    }

    // -------------------------------------------------------------------------
    //  Flush & Compaction
    // -------------------------------------------------------------------------

    /**
     * Force flush the current MemTable to a Level-0 SST file and schedule compaction when needed.
     *
     * <p>The MemTable write is synchronous. Compaction runs according to the configured execution
     * mode; asynchronous compaction waits only when the number of Level-0 files reaches the
     * backpressure threshold.
     */
    public void flush() throws IOException {
        ensureOpen();
        ensureNoBulkLoad();
        ensureNoRangeIterator();
        checkCompactionFailure();
        if (memTable.isEmpty()) {
            return;
        }

        flushMemTable();
        compaction.scheduleIfNeeded();
        compaction.applyBackpressure();
    }

    private void flushMemTable() throws IOException {
        TreeMap<MemorySlice, Object> snapshot = memTable;
        SstFileMetadata metadata = writeMemTableToSst(snapshot);
        levels.addLevelZeroFile(metadata);
        memTable = new TreeMap<>(keyComparator);
        memTableSize = 0;

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
        ensureNoBulkLoad();
        ensureNoRangeIterator();
        compaction.fullCompact();
    }

    // -------------------------------------------------------------------------
    //  Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        ensureNoRangeIterator();
        closed = true;

        IOException failure = null;
        if (activeBulkLoadWriter != null) {
            activeBulkLoadWriter.close();
        }
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
                compaction.scheduleIfNeeded();
                compaction.await();
            }
        } catch (IOException e) {
            failure = addOrSuppress(failure, e);
        } finally {
            levels.runWithWriteLock(
                    () -> {
                        for (SortLookupStoreReader reader : readerCache.values()) {
                            try {
                                reader.close();
                            } catch (IOException e) {
                                LOG.warn("Failed to close cached reader during shutdown", e);
                            }
                        }
                        readerCache.clear();
                    });
        }

        LOG.info("LocalKvDb closed. Level stats: {}", getLevelStats());
        if (failure != null) {
            throw failure;
        }
    }

    /** Return the total number of SST files across all levels. */
    @VisibleForTesting
    int getSstFileCount() {
        return levels.fileCount();
    }

    /** Return the number of readers retained for point lookups. */
    @VisibleForTesting
    int getCachedReaderCount() {
        return readerCache.size();
    }

    /** Return the number of SST files at a specific level. */
    public int getLevelFileCount(int level) {
        return levels.fileCount(level);
    }

    /** Return the estimated MemTable size in bytes. */
    public long getMemTableSize() {
        return memTableSize;
    }

    /** Return a human-readable summary of file counts per level. */
    public String getLevelStats() {
        return levels.stats();
    }

    // -------------------------------------------------------------------------
    //  Internal Helpers
    // -------------------------------------------------------------------------

    private void checkCompactionFailure() throws IOException {
        compaction.checkFailure();
    }

    @VisibleForTesting
    void awaitCompaction() throws IOException {
        ensureOpen();
        compaction.await();
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

    private boolean isMemTableTombstone(Object value) {
        return value instanceof byte[] && isTombstone((byte[]) value);
    }

    private long estimatedMemTableValueSize(Object value) {
        return value instanceof byte[]
                ? ((byte[]) value).length
                : ((MemTableMergeFunction) value).estimatedSize();
    }

    private byte[] materializeMemTableValue(Object value) throws IOException {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        MemTableMergeFunction mergeFunction = (MemTableMergeFunction) value;
        long previousSize = mergeFunction.estimatedSize();
        byte[] result = mergeFunction.getResult();
        memTableSize += mergeFunction.estimatedSize() - previousSize;
        return result;
    }

    @Nullable
    private byte[] lookupInFile(File file, byte[] key) throws IOException {
        return getOrCreateReader(file).lookup(key);
    }

    private SortLookupStoreReader getOrCreateReader(File file) throws IOException {
        SortLookupStoreReader reader = readerCache.get(file);
        if (reader == null) {
            reader = storeFactory.createReader(file);
            if (readerCache.size() >= MAX_CACHED_READERS) {
                Iterator<Map.Entry<File, SortLookupStoreReader>> iterator =
                        readerCache.entrySet().iterator();
                Map.Entry<File, SortLookupStoreReader> eldest = iterator.next();
                iterator.remove();
                try {
                    eldest.getValue().closeInput();
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to close evicted reader for SST file: {}",
                            eldest.getKey().getName(),
                            e);
                }
            }
            readerCache.put(file, reader);
        }
        return reader;
    }

    private SstFileMetadata writeMemTableToSst(TreeMap<MemorySlice, Object> data)
            throws IOException {
        File sstFile = newSstFile();
        SortLookupStoreWriter writer = null;
        try {
            writer =
                    storeFactory.createWriter(
                            sstFile,
                            bloomFilterBuilderFactory.apply(
                                    mergeOperator == null ? data.size() : UNKNOWN_NUM_ENTRIES));
            SstMetadataWriter output = new SstMetadataWriter(writer);
            RecordCombiningWriter combiningWriter =
                    new RecordCombiningWriter(mergeOperator, output);
            for (Map.Entry<MemorySlice, Object> entry : data.entrySet()) {
                combiningWriter.put(entry.getKey(), materializeMemTableValue(entry.getValue()));
            }
            combiningWriter.finish();
            writer.close();
            writer = null;
            return new SstFileMetadata(
                    sstFile, output.minKey, output.maxKey, output.tombstoneCount, 0);
        } catch (IOException | RuntimeException e) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            deleteFileQuietly(sstFile);
            throw e;
        }
    }

    private static final class SstMetadataWriter implements RecordCombiningWriter.RecordConsumer {

        private final SortLookupStoreWriter writer;

        @Nullable private MemorySlice minKey;
        @Nullable private MemorySlice maxKey;
        private long tombstoneCount;

        private SstMetadataWriter(SortLookupStoreWriter writer) {
            this.writer = writer;
        }

        @Override
        public void accept(MemorySlice key, byte[] value) throws IOException {
            writer.put(key.copyBytes(), value);
            if (minKey == null) {
                minKey = key;
            }
            maxKey = key;
            if (isTombstone(value)) {
                tombstoneCount++;
            }
        }
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

    private void ensureNoBulkLoad() {
        if (activeBulkLoadWriter != null) {
            throw new IllegalStateException("A bulk load is in progress.");
        }
    }

    private static MemorySlice copyKey(byte[] key) {
        return MemorySlice.wrap(Arrays.copyOf(key, key.length));
    }

    private void ensureNoRangeIterator() {
        if (openRangeIterators > 0) {
            throw new IllegalStateException(
                    "The database cannot be modified or closed while a range iterator is open.");
        }
    }

    /** Lazy range iterator with newest-version-wins semantics. */
    public final class RangeIterator
            implements KeyValueIterator<MemorySlice, MemorySlice>, AutoCloseable {

        private final LsmLevels.RangeSnapshot snapshot;
        private final PriorityQueue<RangeSource> sources;

        @Nullable private MemorySlice currentKey;
        @Nullable private MemorySlice currentValue;
        private boolean closed;

        private RangeIterator(
                LsmLevels.RangeSnapshot snapshot,
                Map<MemorySlice, Object> memoryEntries,
                byte[] fromInclusive,
                @Nullable MemorySlice toExclusive)
                throws IOException {
            this.snapshot = snapshot;
            this.sources =
                    new PriorityQueue<>(
                            (left, right) -> {
                                int compare = keyComparator.compare(left.key(), right.key());
                                return compare != 0
                                        ? compare
                                        : Integer.compare(left.priority(), right.priority());
                            });

            int priority = 0;
            advanceAndAdd(new MemoryRangeSource(priority++, memoryEntries.entrySet().iterator()));
            for (File file : snapshot.files()) {
                advanceAndAdd(new SstRangeSource(priority++, file, fromInclusive, toExclusive));
            }
        }

        @Override
        public boolean advanceNext() throws IOException {
            if (closed) {
                return false;
            }

            currentKey = null;
            currentValue = null;
            try {
                while (!sources.isEmpty()) {
                    RangeSource newest = sources.poll();
                    MemorySlice key = newest.key();
                    MemorySlice value = newest.value();
                    advanceAndAdd(newest);

                    while (!sources.isEmpty()
                            && keyComparator.compare(sources.peek().key(), key) == 0) {
                        advanceAndAdd(sources.poll());
                    }

                    if (!isTombstoneSlice(value)) {
                        currentKey = key;
                        currentValue = value;
                        return true;
                    }
                }
                close();
                return false;
            } catch (IOException | RuntimeException e) {
                close();
                throw e;
            }
        }

        @Override
        public MemorySlice getKey() {
            if (currentKey == null) {
                throw new IllegalStateException("Range iterator is not positioned on an entry.");
            }
            return currentKey;
        }

        @Override
        public MemorySlice getValue() {
            if (currentValue == null) {
                throw new IllegalStateException("Range iterator is not positioned on an entry.");
            }
            return currentValue;
        }

        private void advanceAndAdd(RangeSource source) throws IOException {
            if (source.advance()) {
                sources.add(source);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                sources.clear();
                currentKey = null;
                currentValue = null;
                snapshot.close();
                openRangeIterators--;
            }
        }
    }

    private interface RangeSource {

        int priority();

        boolean advance() throws IOException;

        MemorySlice key();

        MemorySlice value();
    }

    private final class MemoryRangeSource implements RangeSource {

        private final int priority;
        private final Iterator<Map.Entry<MemorySlice, Object>> iterator;

        @Nullable private Map.Entry<MemorySlice, Object> current;
        @Nullable private byte[] currentValue;

        private MemoryRangeSource(int priority, Iterator<Map.Entry<MemorySlice, Object>> iterator) {
            this.priority = priority;
            this.iterator = iterator;
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public boolean advance() throws IOException {
            current = iterator.hasNext() ? iterator.next() : null;
            currentValue = current == null ? null : materializeMemTableValue(current.getValue());
            return current != null;
        }

        @Override
        public MemorySlice key() {
            return current.getKey();
        }

        @Override
        public MemorySlice value() {
            return MemorySlice.wrap(currentValue);
        }
    }

    private final class SstRangeSource implements RangeSource {

        private final int priority;
        private final File file;
        private final byte[] fromInclusive;
        @Nullable private final MemorySlice toExclusive;

        @Nullable private BlockIterator block;
        @Nullable private MemorySlice resumeAfterKey;
        @Nullable private Map.Entry<MemorySlice, MemorySlice> current;
        private boolean finished;

        private SstRangeSource(
                int priority, File file, byte[] fromInclusive, @Nullable MemorySlice toExclusive) {
            this.priority = priority;
            this.file = file;
            this.fromInclusive = Arrays.copyOf(fromInclusive, fromInclusive.length);
            this.toExclusive =
                    toExclusive == null ? null : MemorySlice.wrap(toExclusive.copyBytes());
        }

        @Override
        public int priority() {
            return priority;
        }

        @Override
        public boolean advance() throws IOException {
            while (block == null || !block.hasNext()) {
                block = loadNextBlock();
                if (block == null) {
                    current = null;
                    return false;
                }
            }

            current = block.next();
            if (toExclusive != null && keyComparator.compare(current.getKey(), toExclusive) >= 0) {
                current = null;
                finished = true;
                return false;
            }
            if (!block.hasNext()) {
                resumeAfterKey = MemorySlice.wrap(current.getKey().copyBytes());
            }
            return true;
        }

        @Nullable
        private BlockIterator loadNextBlock() throws IOException {
            if (finished) {
                return null;
            }

            SortLookupStoreReader reader = getOrCreateReader(file);
            SstFileReader.SstFileIterator iterator = reader.createIterator();
            byte[] seekKey = resumeAfterKey == null ? fromInclusive : resumeAfterKey.copyBytes();
            iterator.seekTo(seekKey);
            boolean skipResumeKey = resumeAfterKey != null;
            while (true) {
                BlockIterator nextBlock = iterator.readBatch();
                if (nextBlock == null) {
                    finished = true;
                    return null;
                }

                if (skipResumeKey) {
                    if (nextBlock.seekTo(resumeAfterKey)) {
                        nextBlock.next();
                    }
                    skipResumeKey = false;
                }
                if (nextBlock.hasNext()) {
                    return nextBlock;
                }
            }
        }

        @Override
        public MemorySlice key() {
            return current.getKey();
        }

        @Override
        public MemorySlice value() {
            return current.getValue();
        }
    }

    private static boolean isTombstoneSlice(MemorySlice value) {
        return value.length() == 0;
    }

    /**
     * Retains raw merge operands until a read or flush requires their serialized result.
     *
     * <p>After materialization, the operands are reset to the result so repeated reads do not merge
     * again and later appends only retain the previous result plus new operands.
     */
    private static final class MemTableMergeFunction {

        private final MergeOperator mergeOperator;
        private List<byte[]> values;

        @Nullable private byte[] result;
        private long retainedValueBytes;

        private MemTableMergeFunction(MergeOperator mergeOperator) {
            this.mergeOperator = mergeOperator;
            this.values = new ArrayList<>(MERGE_VALUES_INITIAL_CAPACITY);
        }

        private void reset(byte[] value) {
            if (values.size() > MERGE_VALUES_INITIAL_CAPACITY) {
                values = new ArrayList<>(MERGE_VALUES_INITIAL_CAPACITY);
            } else {
                values.clear();
            }
            values.add(value);
            result = value;
            retainedValueBytes = value.length;
        }

        private void add(byte[] value) {
            values.add(value);
            result = null;
            retainedValueBytes += value.length;
        }

        private byte[] getResult() throws IOException {
            if (result == null) {
                byte[] merged = mergeOperator.merge(values);
                if (isTombstone(merged)) {
                    throw new IllegalStateException("MergeOperator returned the tombstone marker.");
                }
                reset(merged);
            }
            return result;
        }

        private long estimatedSize() {
            return MEMTABLE_MERGE_FUNCTION_OVERHEAD
                    + retainedValueBytes
                    + values.size() * MEMTABLE_MERGED_VALUE_OVERHEAD;
        }
    }

    /** Callback for visiting an entry during a range scan. */
    @FunctionalInterface
    public interface RangeEntryConsumer {

        void accept(MemorySlice key, MemorySlice value) throws IOException;
    }

    /**
     * Operator for combining adjacent logical records while flushing and compacting SST files.
     *
     * <p>The first record's key is retained for the combined value.
     *
     * <p>Mergeable physical keys must be append-only and must not be reused after being consumed
     * into the first record.
     */
    public interface MergeOperator {

        boolean canMerge(MemorySlice firstKey, MemorySlice nextKey);

        byte[] merge(List<byte[]> values) throws IOException;
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
        @Nullable private Predicate<byte[]> expiredValuePredicate;
        @Nullable private MergeOperator mergeOperator;
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
            checkArgument(fileNum > 0, "level0FileNumCompactTrigger must be positive.");
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

        /**
         * Set a predicate which identifies expired stored values during compaction. Partial
         * compaction converts matching values into tombstones to avoid resurrecting older values;
         * full compaction drops them.
         */
        public Builder expiredValuePredicate(@Nullable Predicate<byte[]> expiredValuePredicate) {
            this.expiredValuePredicate = expiredValuePredicate;
            return this;
        }

        /** Set an operator for combining adjacent logical records in generated SST files. */
        public Builder mergeOperator(@Nullable MergeOperator mergeOperator) {
            this.mergeOperator = mergeOperator;
            return this;
        }

        /**
         * Set the executor for asynchronous compaction. Compaction runs synchronously when no
         * executor is configured. The executor remains owned by the caller and is not shut down
         * when the database is closed.
         */
        public Builder compactionExecutor(@Nullable ExecutorService compactionExecutor) {
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
            return new LocalKvDb(
                    dataDirectory,
                    factory,
                    bloomFilterBuilderFactory,
                    keyComparator,
                    memTableFlushThreshold,
                    maxSstFileSize,
                    level0FileNumCompactTrigger,
                    sizeRatio,
                    expiredValuePredicate,
                    mergeOperator,
                    compactionExecutor);
        }
    }

    static boolean isTombstone(byte[] value) {
        return value.length == 0;
    }
}
