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
import org.apache.paimon.globalindex.TopNGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.ReverseBlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
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
    private final Object minKey;
    private final Object maxKey;

    /** A key and its local row ids stored in one btree entry. */
    public static class KeyRowIds {
        private final Object key;
        private final long[] rowIds;

        public KeyRowIds(Object key, long[] rowIds) {
            this.key = key;
            this.rowIds = rowIds;
        }

        public Object key() {
            return key;
        }

        public long[] rowIds() {
            return rowIds;
        }
    }

    /**
     * Sequential iterator over all non-null key entries.
     *
     * <p>Each returned element contains one key and all local row ids belonging to this key.
     */
    public class EntryIterator {
        private final SstFileReader.SstFileIterator fileIter;
        private BlockIterator dataIter;
        private KeyRowIds next;

        private EntryIterator() {
            this.fileIter = reader.createIterator();
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
                    next = new KeyRowIds(key, deserializeRowIds(entry.getValue()));
                    return true;
                }

                dataIter = fileIter.readBatch();
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
        try {
            // prepare file footer
            long fileSize = globalIndexIOMeta.fileSize();
            Path filePath = globalIndexIOMeta.filePath();
            BlockCache blockCache = new BlockCache(filePath, input, cacheManager);
            BTreeFileFooter footer = readFooter(blockCache, fileSize);

            // prepare nullBitmap and SstFileReader
            this.nullBitmap =
                    new LazyField<>(() -> readNullBitmap(blockCache, footer.getNullBitmapHandle()));
            FileBasedBloomFilter bloomFilter =
                    FileBasedBloomFilter.create(
                            input, filePath, cacheManager, footer.getBloomFilterHandle());
            this.reader =
                    new SstFileReader(
                            createSliceComparator(keySerializer),
                            blockCache,
                            footer.getIndexBlockHandle(),
                            bloomFilter);
        } catch (RuntimeException e) {
            // nothing else holds a reference to input yet, so this is the only chance to release it
            IOUtils.closeQuietly(input);
            throw e;
        }
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
        // input is this reader's own handle, so it has to be released even when the reader
        // itself fails to close. Rethrow the original failure rather than a wrapper.
        try {
            IOUtils.closeAll(reader, input);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /** Returns a sequential iterator over all non-null key entries in this index file. */
    public EntryIterator entryIterator() {
        return new EntryIterator();
    }

    /** Visits all local row ids belonging to null keys. */
    public void scanNullRowIds(LongConsumer consumer) {
        for (long rowId : nullBitmap.get()) {
            consumer.accept(rowId);
        }
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
        return createResult(this::allNonNullRows);
    }

    public Optional<GlobalIndexResult> visitContains(Object literal) {
        return createResult(this::allNonNullRows);
    }

    public Optional<GlobalIndexResult> visitLike(Object literal) {
        return createResult(this::allNonNullRows);
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
                        // SQL IN treats NULL as never matching; skip it instead of
                        // failing to serialize a null key.
                        if (literal == null) {
                            continue;
                        }
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

    public Optional<GlobalIndexResult> visitTopN(TopN topN) {
        List<SortValue> orders = topN.orders();
        if (orders.size() != 1) {
            return Optional.empty();
        }
        Preconditions.checkArgument(topN.limit() >= 0, "TopN limit must not be negative.");
        SortValue order = orders.get(0);
        try {
            return Optional.of(topN(topN.limit(), order.direction(), order.nullOrdering()));
        } catch (IOException e) {
            throw new RuntimeException("fail to read btree index file.", e);
        }
    }

    private Optional<GlobalIndexResult> createResult(IOSupplier<RoaringNavigableMap64> supplier) {
        try {
            return Optional.of(GlobalIndexResult.create(supplier.get()));
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

    private TopNGlobalIndexResult topN(
            int limit, SortValue.SortDirection direction, SortValue.NullOrdering nullOrdering)
            throws IOException {
        List<KeyRowIds> result = new ArrayList<>();
        if (limit == 0) {
            return TopNGlobalIndexResult.create(result, comparator, direction, nullOrdering, limit);
        }

        int remaining = limit;
        if (nullOrdering == SortValue.NullOrdering.NULLS_FIRST) {
            remaining = addNullRows(result, remaining);
        }
        if (remaining > 0) {
            remaining =
                    direction == SortValue.SortDirection.ASCENDING
                            ? addAscendingNonNullRows(result, remaining)
                            : addDescendingNonNullRows(result, remaining);
        }
        if (remaining > 0 && nullOrdering == SortValue.NullOrdering.NULLS_LAST) {
            addNullRows(result, remaining);
        }
        return TopNGlobalIndexResult.create(result, comparator, direction, nullOrdering, limit);
    }

    private int addNullRows(List<KeyRowIds> result, int remaining) {
        int count = (int) Math.min(nullBitmap.get().getLongCardinality(), remaining);
        long[] rowIds = new long[count];
        int position = 0;
        for (long rowId : nullBitmap.get()) {
            rowIds[position++] = rowId;
            if (position == count) {
                break;
            }
        }
        if (count > 0) {
            result.add(new KeyRowIds(null, rowIds));
        }
        return remaining - count;
    }

    private int addDescendingNonNullRows(List<KeyRowIds> result, int remaining) throws IOException {
        if (maxKey == null) {
            return remaining;
        }

        SstFileReader.SstFileReverseIterator fileIterator = reader.createReverseIterator();
        ReverseBlockIterator dataIterator;
        while (remaining > 0 && (dataIterator = fileIterator.readBatch()) != null) {
            while (remaining > 0 && dataIterator.hasNext()) {
                Map.Entry<MemorySlice, MemorySlice> entry = dataIterator.next();
                Object key = keySerializer.deserialize(entry.getKey());
                long[] rowIds = deserializeRowIds(entry.getValue(), remaining);
                result.add(new KeyRowIds(key, rowIds));
                remaining -= rowIds.length;
            }
        }
        return remaining;
    }

    private int addAscendingNonNullRows(List<KeyRowIds> result, int remaining) throws IOException {
        if (minKey == null) {
            return remaining;
        }

        SstFileReader.SstFileIterator fileIterator = reader.createIterator();
        BlockIterator dataIterator;
        while (remaining > 0 && (dataIterator = fileIterator.readBatch()) != null) {
            while (remaining > 0 && dataIterator.hasNext()) {
                Map.Entry<MemorySlice, MemorySlice> entry = dataIterator.next();
                Object key = keySerializer.deserialize(entry.getKey());
                long[] rowIds = deserializeRowIds(entry.getValue(), remaining);
                result.add(new KeyRowIds(key, rowIds));
                remaining -= rowIds.length;
            }
        }
        return remaining;
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
        return deserializeRowIds(slice, Integer.MAX_VALUE);
    }

    static long[] deserializeRowIds(MemorySlice slice, int maxRowIds) {
        Preconditions.checkArgument(maxRowIds >= 0, "Max row id count must not be negative.");
        MemorySliceInput sliceInput = slice.toInput();
        int length = sliceInput.readVarLenInt();
        Preconditions.checkState(length > 0, "Invalid row id length: 0");
        int resultLength = Math.min(length, maxRowIds);
        long[] ids = new long[resultLength];
        for (int i = 0; i < resultLength; i++) {
            ids[i] = sliceInput.readVarLenLong();
        }
        return ids;
    }
}
