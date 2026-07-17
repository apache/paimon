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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedFileMetaSelector;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.zip.CRC32;

/**
 * Synchronous index reader for a single BTree index file. Parallelism across multiple files is
 * handled by {@link LazyFilteredBTreeReader}.
 */
public class BTreeIndexReader implements Closeable {

    private final SeekableInputStream input;
    private final SstFileReader reader;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final LazyField<RoaringNavigableMap64> nullBitmap;
    @Nullable private final BlockHandle nullBitmapHandle;
    private final Object minKey;
    private final Object maxKey;

    /** A key and its local row ids stored in one btree entry. */
    public static class KeyRowIds {
        private final Object key;
        private final MemorySlice serializedRowIds;
        private final int rowIdCount;

        private KeyRowIds(Object key, MemorySlice serializedRowIds) {
            this.key = key;
            this.serializedRowIds = serializedRowIds;
            this.rowIdCount = readRowIdCount(serializedRowIds.toInput());
        }

        public Object key() {
            return key;
        }

        public int rowIdCount() {
            return rowIdCount;
        }

        public RowIdIterator rowIdIterator() {
            return new RowIdIterator(serializedRowIds);
        }

        public long[] rowIds() {
            long[] rowIds = new long[rowIdCount];
            RowIdIterator iterator = rowIdIterator();
            for (int i = 0; i < rowIds.length; i++) {
                rowIds[i] = iterator.nextLong();
            }
            return rowIds;
        }
    }

    /** Streaming iterator over the encoded row ids of one key. */
    public static class RowIdIterator {
        private final MemorySliceInput input;
        private int remaining;

        private RowIdIterator(MemorySlice serializedRowIds) {
            this.input = serializedRowIds.toInput();
            this.remaining = readRowIdCount(input);
        }

        public boolean hasNext() {
            return remaining > 0;
        }

        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more row ids in btree entry.");
            }
            remaining--;
            return input.readVarLenLong();
        }
    }

    private static int readRowIdCount(MemorySliceInput input) {
        int count = input.readVarLenInt();
        Preconditions.checkState(count > 0, "Invalid row id length: 0");
        return count;
    }

    /**
     * Sequential iterator over all non-null key entries.
     *
     * <p>Each returned element contains one key and all local row ids belonging to this key.
     */
    public class EntryIterator {
        private final SstFileReader.SstFileIterator fileIter;
        private final long maxReadBlockSize;
        private BlockIterator dataIter;
        private KeyRowIds next;

        private EntryIterator(long maxReadBlockSize) {
            this.fileIter = reader.createIterator();
            this.maxReadBlockSize = maxReadBlockSize;
            this.dataIter = null;
            this.next = null;
        }

        public boolean hasNext() throws IOException {
            if (next != null) {
                return true;
            }

            while (true) {
                if (dataIter != null && dataIter.hasNext()) {
                    Map.Entry<MemorySlice, MemorySlice> entry = dataIter.next();
                    Object key = keySerializer.deserialize(entry.getKey());
                    next = new KeyRowIds(key, entry.getValue());
                    return true;
                }

                dataIter = fileIter.readBatch(maxReadBlockSize);
                if (dataIter == null) {
                    return false;
                }
            }
        }

        public KeyRowIds next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in btree index file.");
            }
            KeyRowIds current = next;
            next = null;
            return current;
        }
    }

    /** Reverse iterator over all non-null key entries. */
    public class ReverseEntryIterator {
        private final SstFileReader.SstFileIterator fileIter;
        private final long maxReadBlockSize;
        private BlockIterator dataIter;

        private ReverseEntryIterator(long maxReadBlockSize) {
            this.fileIter = reader.createReverseIterator();
            this.maxReadBlockSize = maxReadBlockSize;
        }

        public boolean hasNext() throws IOException {
            while (dataIter == null || !dataIter.hasPrevious()) {
                dataIter = fileIter.readBatchReverse(maxReadBlockSize);
                if (dataIter == null) {
                    return false;
                }
            }
            return true;
        }

        public KeyRowIds next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in btree index file.");
            }
            Map.Entry<MemorySlice, MemorySlice> entry = dataIter.previous();
            return new KeyRowIds(keySerializer.deserialize(entry.getKey()), entry.getValue());
        }
    }

    public BTreeIndexReader(
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexIOMeta,
            CacheManager cacheManager)
            throws IOException {
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        SortedIndexFileMeta indexMeta =
                SortedIndexFileMeta.deserialize(globalIndexIOMeta.metadata());
        if (indexMeta.getFirstKey() != null) {
            this.minKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getFirstKey()));
            this.maxKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getLastKey()));
        } else {
            // this is possible if this btree index file only stores nulls.
            this.minKey = null;
            this.maxKey = null;
        }
        this.input = fileReader.getInputStream(globalIndexIOMeta);

        // prepare file footer
        long fileSize = globalIndexIOMeta.fileSize();
        Path filePath = globalIndexIOMeta.filePath();
        BlockCache blockCache = new BlockCache(filePath, input, cacheManager);
        BTreeFileFooter footer = readFooter(blockCache, fileSize);

        // prepare nullBitmap and SstFileReader
        this.nullBitmap =
                new LazyField<>(() -> readNullBitmap(blockCache, footer.getNullBitmapHandle()));
        this.nullBitmapHandle = footer.getNullBitmapHandle();
        FileBasedBloomFilter bloomFilter =
                FileBasedBloomFilter.create(
                        input, filePath, cacheManager, footer.getBloomFilterHandle());
        this.reader =
                new SstFileReader(
                        createSliceComparator(keySerializer),
                        blockCache,
                        footer.getIndexBlockHandle(),
                        bloomFilter);
    }

    private BTreeFileFooter readFooter(BlockCache blockCache, long fileSize) {
        MemorySegment footerEncodings =
                blockCache.getBlock(
                        fileSize - BTreeFileFooter.ENCODED_LENGTH,
                        BTreeFileFooter.ENCODED_LENGTH,
                        b -> b,
                        true);
        return BTreeFileFooter.readFooter(MemorySlice.wrap(footerEncodings).toInput());
    }

    private RoaringNavigableMap64 readNullBitmap(
            BlockCache cache, @Nullable BlockHandle blockHandle) {
        RoaringNavigableMap64 nullBitmap = new RoaringNavigableMap64();
        if (blockHandle == null) {
            return nullBitmap;
        }

        CRC32 crc32c = new CRC32();
        // read bytes and crc value
        MemorySliceInput sliceInput =
                MemorySlice.wrap(
                                cache.getBlock(
                                        blockHandle.offset(),
                                        blockHandle.size() + 4,
                                        b -> b,
                                        false))
                        .toInput();
        byte[] nullBitmapEncodings = sliceInput.readSlice(blockHandle.size()).copyBytes();

        // check crc value
        crc32c.update(nullBitmapEncodings, 0, nullBitmapEncodings.length);
        int expectedCrcValue = sliceInput.readInt();
        Preconditions.checkState(
                (int) crc32c.getValue() == expectedCrcValue,
                "Crc check failure during decoding null bitmap.");

        try {
            nullBitmap.deserialize(nullBitmapEncodings);
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Fail to deserialize null bitmap but crc check passed,"
                            + " this means the ser/de algorithms not match.",
                    ioe);
        }

        return nullBitmap;
    }

    private Comparator<MemorySlice> createSliceComparator(KeySerializer keySerializer) {
        return (slice1, slice2) ->
                comparator.compare(
                        keySerializer.deserialize(slice1), keySerializer.deserialize(slice2));
    }

    @Override
    public void close() throws IOException {
        reader.close();
        input.close();
    }

    /** Returns a sequential iterator over all non-null key entries in this index file. */
    public EntryIterator entryIterator() {
        return entryIterator(Long.MAX_VALUE);
    }

    EntryIterator entryIterator(long maxReadBlockSize) {
        return new EntryIterator(maxReadBlockSize);
    }

    public ReverseEntryIterator reverseEntryIterator() {
        return reverseEntryIterator(Long.MAX_VALUE);
    }

    ReverseEntryIterator reverseEntryIterator(long maxReadBlockSize) {
        return new ReverseEntryIterator(maxReadBlockSize);
    }

    /** Visits all local row ids belonging to null keys. */
    public void scanNullRowIds(LongConsumer consumer) {
        for (long rowId : nullBitmap.get()) {
            consumer.accept(rowId);
        }
    }

    RoaringNavigableMap64 nullRowIds() {
        return nullBitmap.get();
    }

    Optional<RoaringNavigableMap64> nullRowIds(long maxReadBlockSize) {
        if (nullBitmapHandle != null && nullBitmapHandle.size() + 4L > maxReadBlockSize) {
            return Optional.empty();
        }
        return Optional.of(nullBitmap.get());
    }

    public Optional<GlobalIndexResult> visitIsNotNull() {
        return createResult(this::allNonNullRows);
    }

    public Optional<GlobalIndexResult> visitIsNull() {
        return createResult(nullBitmap::get);
    }

    public Optional<GlobalIndexResult> visitStartsWith(Object literal) {
        return createResult(
                () -> {
                    if (minKey == null) {
                        return new RoaringNavigableMap64();
                    }
                    byte[] upperBound =
                            SortedFileMetaSelector.prefixUpperBound(
                                    keySerializer.serialize(literal));
                    if (upperBound == null) {
                        return rangeQuery(literal, maxKey, true, true);
                    }
                    return rangeQuery(
                            literal,
                            keySerializer.deserialize(MemorySlice.wrap(upperBound)),
                            true,
                            false);
                });
    }

    public Optional<GlobalIndexResult> visitEndsWith(Object literal) {
        return createResult(this::allNonNullRows, false);
    }

    public Optional<GlobalIndexResult> visitContains(Object literal) {
        return createResult(this::allNonNullRows, false);
    }

    public Optional<GlobalIndexResult> visitLike(Object literal) {
        return createResult(this::allNonNullRows, false);
    }

    public Optional<GlobalIndexResult> visitLessThan(Object literal) {
        return createResult(() -> rangeQuery(minKey, literal, true, false));
    }

    public Optional<GlobalIndexResult> visitGreaterOrEqual(Object literal) {
        return createResult(() -> rangeQuery(literal, maxKey, true, true));
    }

    public Optional<GlobalIndexResult> visitNotEqual(Object literal) {
        return createResult(
                () -> {
                    RoaringNavigableMap64 result = allNonNullRows();
                    result.andNot(rangeQuery(literal, literal, true, true));
                    return result;
                });
    }

    public Optional<GlobalIndexResult> visitLessOrEqual(Object literal) {
        return createResult(() -> rangeQuery(minKey, literal, true, true));
    }

    public Optional<GlobalIndexResult> visitEqual(Object literal) {
        return createResult(() -> rangeQuery(literal, literal, true, true));
    }

    public Optional<GlobalIndexResult> visitGreaterThan(Object literal) {
        return createResult(() -> rangeQuery(literal, maxKey, false, true));
    }

    public Optional<GlobalIndexResult> visitIn(List<Object> literals) {
        return createResult(
                () -> {
                    RoaringNavigableMap64 result = new RoaringNavigableMap64();
                    for (Object literal : literals) {
                        result.or(rangeQuery(literal, literal, true, true));
                    }
                    return result;
                });
    }

    public Optional<GlobalIndexResult> visitNotIn(List<Object> literals) {
        return createResult(
                () -> {
                    RoaringNavigableMap64 result = allNonNullRows();
                    RoaringNavigableMap64 inResult = new RoaringNavigableMap64();
                    for (Object literal : literals) {
                        inResult.or(rangeQuery(literal, literal, true, true));
                    }
                    result.andNot(inResult);
                    return result;
                });
    }

    public Optional<GlobalIndexResult> visitBetween(Object from, Object to) {
        return createResult(() -> rangeQuery(from, to, true, true));
    }

    static class ScalarSearchResultAccumulator {
        private final RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
        private final long maxResultSize;
        private final long maxScannedRowIds;
        private long cardinality;
        private long scannedRowIds;
        private boolean exceededBudget;

        ScalarSearchResultAccumulator(long maxResultSize, long maxScannedRowIds) {
            this.maxResultSize = maxResultSize;
            this.maxScannedRowIds = maxScannedRowIds;
        }

        boolean add(long rowId) {
            if (rowIds.contains(rowId)) {
                return true;
            }
            if (cardinality >= maxResultSize) {
                exceededBudget = true;
                return false;
            }
            rowIds.add(rowId);
            cardinality++;
            return true;
        }

        boolean reserveScannedRowIds(long count) {
            if (count > maxScannedRowIds - scannedRowIds) {
                exceededBudget = true;
                return false;
            }
            scannedRowIds += count;
            return true;
        }

        long cardinality() {
            return cardinality;
        }

        boolean exceededBudget() {
            return exceededBudget;
        }

        RoaringNavigableMap64 rowIds() {
            return rowIds;
        }
    }

    private Optional<GlobalIndexResult> createResult(IOSupplier<RoaringNavigableMap64> supplier) {
        return createResult(supplier, true);
    }

    private Optional<GlobalIndexResult> createResult(
            IOSupplier<RoaringNavigableMap64> supplier, boolean exact) {
        try {
            return Optional.of(GlobalIndexResult.create(supplier.get(), exact));
        } catch (IOException e) {
            throw new RuntimeException("fail to read btree index file.", e);
        }
    }

    @FunctionalInterface
    private interface IOSupplier<T> {
        T get() throws IOException;
    }

    private RoaringNavigableMap64 allNonNullRows() throws IOException {
        // Traverse all data to avoid returning null values, which is very advantageous in
        // situations where there are many null values
        // TODO do not traverse all data if less null values
        if (minKey == null) {
            return new RoaringNavigableMap64();
        }
        return rangeQuery(minKey, maxKey, true, true);
    }

    /**
     * Range query on underlying SST File.
     *
     * @param from lower bound
     * @param to upper bound
     * @param fromInclusive whether include lower bound
     * @param toInclusive whether include upper bound
     * @return a bitmap containing all qualified row ids
     */
    private RoaringNavigableMap64 rangeQuery(
            Object from, Object to, boolean fromInclusive, boolean toInclusive) throws IOException {
        SstFileReader.SstFileIterator fileIter = reader.createIterator();
        fileIter.seekTo(keySerializer.serialize(from));

        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        BlockIterator dataIter;
        Map.Entry<MemorySlice, MemorySlice> entry;
        while ((dataIter = fileIter.readBatch()) != null) {
            while (dataIter.hasNext()) {
                entry = dataIter.next();
                Object key = keySerializer.deserialize(entry.getKey());

                if (!fromInclusive && comparator.compare(key, from) == 0) {
                    continue;
                }

                int difference = comparator.compare(key, to);
                if (difference > 0 || !toInclusive && difference == 0) {
                    return result;
                }

                for (long rowId : deserializeRowIds(entry.getValue())) {
                    result.add(rowId);
                }
            }
        }
        return result;
    }

    private long[] deserializeRowIds(MemorySlice slice) {
        MemorySliceInput sliceInput = slice.toInput();
        int length = sliceInput.readVarLenInt();
        Preconditions.checkState(length > 0, "Invalid row id length: 0");
        long[] ids = new long[length];
        for (int i = 0; i < length; i++) {
            ids[i] = sliceInput.readVarLenLong();
        }
        return ids;
    }
}
