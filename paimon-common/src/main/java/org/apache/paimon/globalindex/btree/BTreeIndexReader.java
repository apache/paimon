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
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.sst.BlockCache;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32;

/** The {@link GlobalIndexReader} implementation for btree index. */
public class BTreeIndexReader implements GlobalIndexReader {

    private final SeekableInputStream input;
    private final SstFileReader reader;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final LazyField<RoaringNavigableMap64> nullBitmap;
    private final Object minKey;
    private final Object maxKey;

    public BTreeIndexReader(
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexIOMeta,
            CacheManager cacheManager)
            throws IOException {
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        BTreeIndexMeta indexMeta = BTreeIndexMeta.deserialize(globalIndexIOMeta.metadata());
        if (indexMeta.getFirstKey() != null) {
            this.minKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getFirstKey()));
            this.maxKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getLastKey()));
        } else {
            // this is possible if this btree index file only stores nulls.
            this.minKey = null;
            this.maxKey = null;
        }
        this.input = fileReader.getInputStream(globalIndexIOMeta.fileName());

        // prepare file footer
        long fileSize = globalIndexIOMeta.fileSize();
        Path filePath = fileReader.filePath(globalIndexIOMeta.fileName());
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

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        // nulls are stored separately in null bitmap.
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return allNonNullRows();
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        // nulls are stored separately in null bitmap.
        return Optional.of(GlobalIndexResult.create(nullBitmap::get));
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        // todo: `startsWith` can also be covered by btree index.
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return allNonNullRows();
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return allNonNullRows();
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return allNonNullRows();
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return allNonNullRows();
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return rangeQuery(minKey, literal, true, false);
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return rangeQuery(literal, maxKey, true, true);
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                RoaringNavigableMap64 result = allNonNullRows();
                                result.andNot(rangeQuery(literal, literal, true, true));
                                return result;
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return rangeQuery(minKey, literal, true, true);
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return rangeQuery(literal, literal, true, true);
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                return rangeQuery(literal, maxKey, false, true);
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                RoaringNavigableMap64 result = new RoaringNavigableMap64();
                                for (Object literal : literals) {
                                    result.or(rangeQuery(literal, literal, true, true));
                                }
                                return result;
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(
                GlobalIndexResult.create(
                        () -> {
                            try {
                                RoaringNavigableMap64 result = allNonNullRows();
                                result.andNot(this.visitIn(fieldRef, literals).get().results());
                                return result;
                            } catch (IOException ioe) {
                                throw new RuntimeException("fail to read btree index file.", ioe);
                            }
                        }));
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
