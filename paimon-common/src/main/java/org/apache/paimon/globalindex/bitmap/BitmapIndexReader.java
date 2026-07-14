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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/** Reader for one bitmap global index file. */
class BitmapIndexReader implements BitmapGlobalIndexFormat.SeekableReader, Closeable {

    private final SeekableInputStream input;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final boolean logicalKeyOrder;
    private final LazyField<RoaringNavigableMap64> nullRows;
    private final LazyField<RoaringNavigableMap64> nonNullRows;
    private final LazyField<List<BitmapGlobalIndexFormat.DictionaryBlockMeta>> dictionaryBlocks;
    private final Map<
                    BitmapGlobalIndexFormat.DictionaryBlockMeta,
                    BitmapGlobalIndexFormat.DictionaryBlock>
            dictionaryBlockCache;

    BitmapIndexReader(
            KeySerializer keySerializer, GlobalIndexFileReader fileReader, GlobalIndexIOMeta meta)
            throws IOException {
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        this.dictionaryBlockCache = new ConcurrentHashMap<>();
        this.input = fileReader.getInputStream(meta);
        try {
            BitmapGlobalIndexFormat.Footer footer =
                    BitmapGlobalIndexFormat.readFooter(this, meta.fileSize());
            this.logicalKeyOrder = footer.logicalKeyOrder();
            this.nullRows =
                    new LazyField<>(
                            () ->
                                    BitmapGlobalIndexFormat.readBitmapUnchecked(
                                            this, footer.nullRowsBlock));
            this.nonNullRows =
                    new LazyField<>(
                            () ->
                                    BitmapGlobalIndexFormat.readBitmapUnchecked(
                                            this, footer.nonNullRowsBlock));
            this.dictionaryBlocks =
                    new LazyField<>(
                            () ->
                                    BitmapGlobalIndexFormat.readIndexBlockUnchecked(
                                            this, footer.indexBlock, footer.version));
        } catch (IOException | RuntimeException e) {
            IOUtils.closeQuietly(input);
            throw e;
        }
    }

    Optional<GlobalIndexResult> visitIsNotNull() {
        return createResult(isNotNull());
    }

    Optional<GlobalIndexResult> visitIsNull() {
        return createResult(isNull());
    }

    Optional<GlobalIndexResult> visitStartsWith(Object literal) {
        return createResult(startsWith(literal));
    }

    Optional<GlobalIndexResult> visitEndsWith(Object literal) {
        return createResult(endsWith(literal));
    }

    Optional<GlobalIndexResult> visitContains(Object literal) {
        return createResult(contains(literal));
    }

    Optional<GlobalIndexResult> visitLessThan(Object literal) {
        return createResult(lessThan(literal));
    }

    Optional<GlobalIndexResult> visitGreaterOrEqual(Object literal) {
        return createResult(greaterOrEqual(literal));
    }

    Optional<GlobalIndexResult> visitNotEqual(Object literal) {
        return createResult(notEqual(literal));
    }

    Optional<GlobalIndexResult> visitLessOrEqual(Object literal) {
        return createResult(lessOrEqual(literal));
    }

    Optional<GlobalIndexResult> visitEqual(Object literal) {
        return createResult(equal(literal));
    }

    Optional<GlobalIndexResult> visitGreaterThan(Object literal) {
        return createResult(greaterThan(literal));
    }

    Optional<GlobalIndexResult> visitIn(List<Object> literals) {
        return createResult(in(literals));
    }

    Optional<GlobalIndexResult> visitNotIn(List<Object> literals) {
        return createResult(notIn(literals));
    }

    Optional<GlobalIndexResult> visitBetween(Object from, Object to) {
        return createResult(between(from, to));
    }

    RoaringNavigableMap64 like(Predicate<Object> keyPredicate) {
        return scanDictionary(keyPredicate);
    }

    RoaringNavigableMap64 lessThan(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }
        return scanDictionary(key -> comparator.compare(key, literal) < 0);
    }

    RoaringNavigableMap64 greaterThan(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }
        return scanDictionary(key -> comparator.compare(key, literal) > 0);
    }

    @Override
    public synchronized byte[] read(long offset, int length) throws IOException {
        input.seek(offset);
        byte[] bytes = new byte[length];
        IOUtils.readFully(input, bytes);
        return bytes;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    private static Optional<GlobalIndexResult> createResult(RoaringNavigableMap64 bitmap) {
        return Optional.of(GlobalIndexResult.create(bitmap));
    }

    private RoaringNavigableMap64 isNull() {
        return copy(nullRows.get());
    }

    private RoaringNavigableMap64 isNotNull() {
        return copy(nonNullRows.get());
    }

    private RoaringNavigableMap64 equal(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }

        BitmapGlobalIndexFormat.BlockInfo bitmapBlock = findBitmapBlock(literal);
        if (bitmapBlock == null) {
            return new RoaringNavigableMap64();
        }

        return readBitmap(bitmapBlock);
    }

    private RoaringNavigableMap64 in(List<Object> literals) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        Set<BitmapGlobalIndexFormat.SerializedKey> keys = new HashSet<>();
        for (Object literal : literals) {
            if (literal == null) {
                continue;
            }

            BitmapGlobalIndexFormat.SerializedKey key =
                    BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, literal);
            if (!keys.add(key)) {
                continue;
            }

            BitmapGlobalIndexFormat.BlockInfo bitmapBlock = findBitmapBlock(literal);
            if (bitmapBlock != null) {
                result.or(readBitmap(bitmapBlock));
            }
        }
        return result;
    }

    private RoaringNavigableMap64 startsWith(Object literal) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        BitmapGlobalIndexFormat.SerializedKey prefix =
                BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, literal);
        if (prefix.bytes().length == 0) {
            return isNotNull();
        }
        BitmapGlobalIndexFormat.SerializedKey upperBound = prefixUpperBound(prefix);

        List<BitmapGlobalIndexFormat.DictionaryBlockMeta> blocks = dictionaryBlocks.get();
        int index = firstPossibleDictionaryBlock(blocks, prefix);
        while (index < blocks.size()) {
            if (upperBound != null && blocks.get(index).firstKey.compareTo(upperBound) >= 0) {
                return result;
            }

            BitmapGlobalIndexFormat.DictionaryBlock dictionaryBlock =
                    dictionaryBlock(blocks.get(index));
            for (BitmapGlobalIndexFormat.DictionaryEntry entry : dictionaryBlock.entries) {
                int compare = entry.key.compareTo(prefix);
                if (compare < 0) {
                    continue;
                }
                if (!startsWith(entry.key, prefix)) {
                    return result;
                }
                result.or(readBitmap(entry.bitmapBlock));
            }
            index++;
        }
        return result;
    }

    private RoaringNavigableMap64 endsWith(Object literal) {
        BitmapGlobalIndexFormat.SerializedKey suffix =
                BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, literal);
        if (suffix.bytes().length == 0) {
            return isNotNull();
        }
        return scanSerializedDictionary(key -> endsWith(key, suffix));
    }

    private RoaringNavigableMap64 contains(Object literal) {
        BitmapGlobalIndexFormat.SerializedKey infix =
                BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, literal);
        if (infix.bytes().length == 0) {
            return isNotNull();
        }
        return scanSerializedDictionary(key -> contains(key, infix));
    }

    private RoaringNavigableMap64 greaterOrEqual(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }
        return scanDictionary(key -> comparator.compare(key, literal) >= 0);
    }

    private RoaringNavigableMap64 lessOrEqual(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }
        return scanDictionary(key -> comparator.compare(key, literal) <= 0);
    }

    private RoaringNavigableMap64 between(Object from, Object to) {
        if (from == null || to == null || comparator.compare(from, to) > 0) {
            return new RoaringNavigableMap64();
        }
        return scanDictionary(
                key -> comparator.compare(key, from) >= 0 && comparator.compare(key, to) <= 0);
    }

    private RoaringNavigableMap64 notEqual(Object literal) {
        if (literal == null) {
            return new RoaringNavigableMap64();
        }

        RoaringNavigableMap64 result = isNotNull();
        result.andNot(equal(literal));
        return result;
    }

    private RoaringNavigableMap64 notIn(List<Object> literals) {
        for (Object literal : literals) {
            if (literal == null) {
                return new RoaringNavigableMap64();
            }
        }

        RoaringNavigableMap64 result = isNotNull();
        result.andNot(in(literals));
        return result;
    }

    private RoaringNavigableMap64 scanDictionary(Predicate<Object> keyPredicate) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (BitmapGlobalIndexFormat.DictionaryBlockMeta blockMeta : dictionaryBlocks.get()) {
            BitmapGlobalIndexFormat.DictionaryBlock dictionaryBlock = dictionaryBlock(blockMeta);
            for (BitmapGlobalIndexFormat.DictionaryEntry entry : dictionaryBlock.entries) {
                Object key = keySerializer.deserialize(MemorySlice.wrap(entry.key.bytes()));
                if (keyPredicate.test(key)) {
                    result.or(readBitmap(entry.bitmapBlock));
                }
            }
        }
        return result;
    }

    private RoaringNavigableMap64 scanSerializedDictionary(
            Predicate<BitmapGlobalIndexFormat.SerializedKey> keyPredicate) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (BitmapGlobalIndexFormat.DictionaryBlockMeta blockMeta : dictionaryBlocks.get()) {
            BitmapGlobalIndexFormat.DictionaryBlock dictionaryBlock = dictionaryBlock(blockMeta);
            for (BitmapGlobalIndexFormat.DictionaryEntry entry : dictionaryBlock.entries) {
                if (keyPredicate.test(entry.key)) {
                    result.or(readBitmap(entry.bitmapBlock));
                }
            }
        }
        return result;
    }

    private int firstPossibleDictionaryBlock(
            List<BitmapGlobalIndexFormat.DictionaryBlockMeta> blocks,
            BitmapGlobalIndexFormat.SerializedKey key) {
        int index = findSerializedDictionaryBlockIndex(blocks, key);
        return Math.max(index, 0);
    }

    private BitmapGlobalIndexFormat.BlockInfo findBitmapBlock(Object key) {
        List<BitmapGlobalIndexFormat.DictionaryBlockMeta> blocks = dictionaryBlocks.get();
        if (blocks.isEmpty()) {
            return null;
        }

        BitmapGlobalIndexFormat.SerializedKey serializedKey =
                BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, key);
        int index =
                logicalKeyOrder
                        ? findLogicalDictionaryBlockIndex(blocks, key)
                        : findSerializedDictionaryBlockIndex(blocks, serializedKey);
        if (index < 0) {
            return null;
        }

        BitmapGlobalIndexFormat.DictionaryBlock dictionaryBlock =
                dictionaryBlock(blocks.get(index));
        for (BitmapGlobalIndexFormat.DictionaryEntry entry : dictionaryBlock.entries) {
            int compare =
                    logicalKeyOrder
                            ? comparator.compare(
                                    keySerializer.deserialize(MemorySlice.wrap(entry.key.bytes())),
                                    key)
                            : entry.key.compareTo(serializedKey);
            if (compare == 0) {
                return entry.bitmapBlock;
            } else if (compare > 0) {
                return null;
            }
        }
        return null;
    }

    private RoaringNavigableMap64 readBitmap(BitmapGlobalIndexFormat.BlockInfo bitmapBlock) {
        try {
            return BitmapGlobalIndexFormat.readBitmap(this, bitmapBlock);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read bitmap global index block.", e);
        }
    }

    private int findSerializedDictionaryBlockIndex(
            List<BitmapGlobalIndexFormat.DictionaryBlockMeta> blocks,
            BitmapGlobalIndexFormat.SerializedKey key) {
        int low = 0;
        int high = blocks.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int compare = blocks.get(mid).firstKey.compareTo(key);
            if (compare <= 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return high;
    }

    private int findLogicalDictionaryBlockIndex(
            List<BitmapGlobalIndexFormat.DictionaryBlockMeta> blocks, Object key) {
        int low = 0;
        int high = blocks.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            Object firstKey =
                    keySerializer.deserialize(MemorySlice.wrap(blocks.get(mid).firstKey.bytes()));
            int compare = comparator.compare(firstKey, key);
            if (compare <= 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return high;
    }

    private BitmapGlobalIndexFormat.DictionaryBlock dictionaryBlock(
            BitmapGlobalIndexFormat.DictionaryBlockMeta blockMeta) {
        return dictionaryBlockCache.computeIfAbsent(blockMeta, this::readDictionaryBlock);
    }

    private BitmapGlobalIndexFormat.DictionaryBlock readDictionaryBlock(
            BitmapGlobalIndexFormat.DictionaryBlockMeta blockMeta) {
        try {
            return BitmapGlobalIndexFormat.readDictionaryBlock(this, blockMeta);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read bitmap dictionary block.", e);
        }
    }

    private static boolean startsWith(
            BitmapGlobalIndexFormat.SerializedKey key,
            BitmapGlobalIndexFormat.SerializedKey prefix) {
        byte[] keyBytes = key.bytes();
        byte[] prefixBytes = prefix.bytes();
        if (keyBytes.length < prefixBytes.length) {
            return false;
        }
        for (int i = 0; i < prefixBytes.length; i++) {
            if (keyBytes[i] != prefixBytes[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean endsWith(
            BitmapGlobalIndexFormat.SerializedKey key,
            BitmapGlobalIndexFormat.SerializedKey suffix) {
        byte[] keyBytes = key.bytes();
        byte[] suffixBytes = suffix.bytes();
        if (keyBytes.length < suffixBytes.length) {
            return false;
        }
        int keyOffset = keyBytes.length - suffixBytes.length;
        for (int i = 0; i < suffixBytes.length; i++) {
            if (keyBytes[keyOffset + i] != suffixBytes[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean contains(
            BitmapGlobalIndexFormat.SerializedKey key,
            BitmapGlobalIndexFormat.SerializedKey infix) {
        byte[] keyBytes = key.bytes();
        byte[] infixBytes = infix.bytes();
        if (keyBytes.length < infixBytes.length) {
            return false;
        }
        for (int i = 0; i <= keyBytes.length - infixBytes.length; i++) {
            boolean found = true;
            for (int j = 0; j < infixBytes.length; j++) {
                if (keyBytes[i + j] != infixBytes[j]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return true;
            }
        }
        return false;
    }

    private static BitmapGlobalIndexFormat.SerializedKey prefixUpperBound(
            BitmapGlobalIndexFormat.SerializedKey prefix) {
        byte[] prefixBytes = prefix.bytes();
        for (int i = prefixBytes.length - 1; i >= 0; i--) {
            int unsignedByte = prefixBytes[i] & 0xFF;
            if (unsignedByte != 0xFF) {
                byte[] upperBound = new byte[i + 1];
                System.arraycopy(prefixBytes, 0, upperBound, 0, i + 1);
                upperBound[i] = (byte) (unsignedByte + 1);
                return new BitmapGlobalIndexFormat.SerializedKey(upperBound);
            }
        }
        return null;
    }

    private static RoaringNavigableMap64 copy(RoaringNavigableMap64 bitmap) {
        return RoaringNavigableMap64.or(new RoaringNavigableMap64(), bitmap);
    }
}
