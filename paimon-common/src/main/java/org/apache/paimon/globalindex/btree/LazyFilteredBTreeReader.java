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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexQueryContext;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedFileGlobalIndexReader;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * An Index Reader for BTree which dynamically filters file list by input predicate, then visits
 * each selected file in parallel via an executor. Each index file is synchronized independently to
 * allow maximum concurrency.
 */
public class LazyFilteredBTreeReader extends SortedFileGlobalIndexReader<BTreeIndexReader> {

    private final KeySerializer keySerializer;
    private final CacheManager cacheManager;
    private final GlobalIndexFileReader fileReader;
    private final Comparator<Object> comparator;
    private final long totalRowCount;
    private final GlobalIndexQueryContext queryContext;
    @Nullable private final Pair<Object, Object> fullRangeBounds;

    public LazyFilteredBTreeReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            CacheManager cacheManager,
            long fallbackScanMaxSize,
            long totalRowCount,
            ExecutorService executor) {
        this(
                files,
                keySerializer,
                fileReader,
                cacheManager,
                fallbackScanMaxSize,
                totalRowCount,
                executor,
                GlobalIndexQueryContext.unlimited());
    }

    public LazyFilteredBTreeReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            CacheManager cacheManager,
            long fallbackScanMaxSize,
            long totalRowCount,
            ExecutorService executor,
            GlobalIndexQueryContext queryContext) {
        super(files, keySerializer, fallbackScanMaxSize, totalRowCount, executor);
        this.cacheManager = cacheManager;
        this.fileReader = fileReader;
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        this.totalRowCount = totalRowCount;
        this.queryContext = queryContext;
        this.fullRangeBounds = fullRangeBounds(files);
    }

    @Nullable
    private Pair<Object, Object> fullRangeBounds(List<GlobalIndexIOMeta> files) {
        if (totalRowCount == 0 || files.isEmpty()) {
            return null;
        }
        long remaining = totalRowCount;
        Object min = null;
        Object max = null;
        for (GlobalIndexIOMeta file : files) {
            // A file's min/max does not identify its row IDs. Only skip the entire reader
            // when its scalar entries cover the complete local row-ID domain without gaps.
            if (file.rowCount() < 0 || file.rowCount() > remaining) {
                return null;
            }
            remaining -= file.rowCount();
            SortedIndexFileMeta meta = SortedIndexFileMeta.deserialize(file.metadata());
            if (meta.hasNulls() || meta.firstKey() == null || meta.lastKey() == null) {
                return null;
            }
            Object first = keySerializer.deserialize(MemorySlice.wrap(meta.firstKey()));
            Object last = keySerializer.deserialize(MemorySlice.wrap(meta.lastKey()));
            if (min == null || comparator.compare(first, min) < 0) {
                min = first;
            }
            if (max == null || comparator.compare(last, max) > 0) {
                max = last;
            }
        }
        return remaining == 0 ? Pair.of(min, max) : null;
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return visitWithAllMatch(
                key -> literal != null && comparator.compare(key, literal) == 0,
                () -> super.visitEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return visitWithAllMatch(
                key -> literal != null && comparator.compare(key, literal) < 0,
                () -> super.visitLessThan(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return visitWithAllMatch(
                key -> literal != null && comparator.compare(key, literal) <= 0,
                () -> super.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return visitWithAllMatch(
                key -> literal != null && comparator.compare(key, literal) > 0,
                () -> super.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return visitWithAllMatch(
                key -> literal != null && comparator.compare(key, literal) >= 0,
                () -> super.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return visitWithAllMatch(
                key ->
                        from != null
                                && to != null
                                && comparator.compare(key, from) >= 0
                                && comparator.compare(key, to) <= 0,
                () -> super.visitBetween(fieldRef, from, to));
    }

    // Only use for predicates whose matching keys form one contiguous interval.
    private CompletableFuture<Optional<GlobalIndexResult>> visitWithAllMatch(
            Predicate<Object> predicate,
            Supplier<CompletableFuture<Optional<GlobalIndexResult>>> fallback) {
        if (fullRangeBounds != null
                && predicate.test(fullRangeBounds.getLeft())
                && predicate.test(fullRangeBounds.getRight())) {
            return CompletableFuture.completedFuture(
                    Optional.of(GlobalIndexResult.fromRange(new Range(0, totalRowCount - 1))));
        }
        return fallback.get();
    }

    @Override
    protected Optional<GlobalIndexResult> visitIsNull(BTreeIndexReader reader) {
        return reader.visitIsNull();
    }

    @Override
    protected Optional<GlobalIndexResult> visitStartsWith(BTreeIndexReader reader, Object literal) {
        return reader.visitStartsWith(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitEndsWith(BTreeIndexReader reader, Object literal) {
        return reader.visitEndsWith(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitContains(BTreeIndexReader reader, Object literal) {
        return reader.visitContains(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitLike(
            BTreeIndexReader reader, FieldRef fieldRef, Object literal) {
        return reader.visitLike(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitLessThan(BTreeIndexReader reader, Object literal) {
        return reader.visitLessThan(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitGreaterOrEqual(
            BTreeIndexReader reader, Object literal) {
        return reader.visitGreaterOrEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitLessOrEqual(
            BTreeIndexReader reader, Object literal) {
        return reader.visitLessOrEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitEqual(BTreeIndexReader reader, Object literal) {
        return reader.visitEqual(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitGreaterThan(
            BTreeIndexReader reader, Object literal) {
        return reader.visitGreaterThan(literal);
    }

    @Override
    protected Optional<GlobalIndexResult> visitIn(BTreeIndexReader reader, List<Object> literals) {
        return reader.visitIn(literals);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitRange(
            FieldRef fieldRef, Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        return visitRange(
                fieldRef,
                from,
                to,
                reader -> reader.visitRange(from, to, fromInclusive, toInclusive));
    }

    @Override
    protected Optional<GlobalIndexResult> visitBetween(
            BTreeIndexReader reader, Object from, Object to) {
        return reader.visitBetween(from, to);
    }

    @Override
    protected RoaringNavigableMap64 lessThan(BTreeIndexReader reader, Object literal) {
        return bitmap(reader.visitLessThan(literal));
    }

    @Override
    protected RoaringNavigableMap64 greaterThan(BTreeIndexReader reader, Object literal) {
        return bitmap(reader.visitGreaterThan(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitTopN(TopN topN) {
        return visitAllFiles(reader -> reader.visitTopN(topN));
    }

    @Override
    protected BTreeIndexReader openReader(GlobalIndexIOMeta meta) {
        try {
            return new BTreeIndexReader(
                    keySerializer, fileReader, meta, cacheManager, queryContext);
        } catch (IOException e) {
            throw new RuntimeException("Can't create BTree index reader for " + meta.filePath(), e);
        }
    }

    private RoaringNavigableMap64 bitmap(Optional<GlobalIndexResult> result) {
        return result.get().results();
    }
}
