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
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.sst.BlockIterator;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/** The {@link GlobalIndexReader} implementation for btree index. */
public class BTreeIndexReader implements GlobalIndexReader {
    private final SeekableInputStream input;
    private final SstFileReader reader;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final RoaringNavigableMap64 nullBitmap;
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
        this.minKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getFirstKey()));
        this.maxKey = keySerializer.deserialize(MemorySlice.wrap(indexMeta.getLastKey()));

        this.input = fileReader.getInputStream(globalIndexIOMeta.fileName());
        long fileSize = globalIndexIOMeta.fileSize();
        Trailer trailer = readTrailer(fileSize - Trailer.TRAILER_LENGTH);
        this.nullBitmap = readNullBitmap(trailer);

        this.reader =
                new SstFileReader(
                        createSliceComparator(keySerializer),
                        fileSize - Trailer.TRAILER_LENGTH,
                        fileReader.filePath(globalIndexIOMeta.fileName()),
                        input,
                        cacheManager);
    }

    private Trailer readTrailer(long offset) throws IOException {
        byte[] trailerEncodings = new byte[Trailer.TRAILER_LENGTH];
        input.seek(offset);
        IOUtils.readFully(input, trailerEncodings);
        return Trailer.readTrailer(MemorySlice.wrap(trailerEncodings));
    }

    private RoaringNavigableMap64 readNullBitmap(Trailer trailer) throws IOException {
        CRC32 crc32c = new CRC32();
        input.seek(trailer.getNullBitmapOffset());
        int length = input.read();
        Preconditions.checkState(length >= 0);
        crc32c.update(length);

        RoaringNavigableMap64 nullBitmap = new RoaringNavigableMap64();
        if (length != 0) {
            byte[] nullBitmapEncodings = new byte[length];
            IOUtils.readFully(input, nullBitmapEncodings);

            crc32c.update(nullBitmapEncodings, 0, length);
            Preconditions.checkState(
                    (int) crc32c.getValue() == trailer.getCrc32c(),
                    "Crc check failure during decoding null bitmap.");

            nullBitmap.deserialize(nullBitmapEncodings);
        } else {
            Preconditions.checkState(
                    (int) crc32c.getValue() == trailer.getCrc32c(),
                    "Crc check failure during decoding null bitmap.");
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
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return fullData();
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return GlobalIndexResult.create(() -> nullBitmap);
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return fullData();
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return fullData();
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return fullData();
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return fullData();
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return rangeQuery(minKey, literal, true, false);
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return rangeQuery(literal, maxKey, true, true);
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        RoaringNavigableMap64 result = fullData();
                        result.andNot(rangeQuery(literal, literal, true, true));
                        return result;
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return rangeQuery(minKey, literal, true, true);
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return rangeQuery(literal, literal, true, true);
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        return rangeQuery(literal, maxKey, false, true);
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return GlobalIndexResult.create(
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
                });
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return GlobalIndexResult.create(
                () -> {
                    try {
                        RoaringNavigableMap64 result = fullData();
                        result.andNot(this.visitIn(fieldRef, literals).results());
                        return result;
                    } catch (IOException ioe) {
                        throw new RuntimeException("fail to read btree index file.", ioe);
                    }
                });
    }

    private RoaringNavigableMap64 fullData() throws IOException {
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

        boolean skipped = false;
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        BlockIterator dataIter;
        Map.Entry<MemorySlice, MemorySlice> entry;
        while ((dataIter = fileIter.readBatch()) != null) {
            while ((entry = dataIter.next()) != null) {
                if (!fromInclusive && !skipped) {
                    // this is correct only if the underlying file do not have duplicated keys.
                    skipped = true;
                    continue;
                }
                int difference = comparator.compare(keySerializer.deserialize(entry.getKey()), to);
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
