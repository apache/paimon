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
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedFileGlobalIndexReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.ScalarSearch;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.sst.SstFileReader;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * An Index Reader for BTree which dynamically filters file list by input predicate, then visits
 * each selected file in parallel via an executor. Each index file is synchronized independently to
 * allow maximum concurrency.
 */
public class LazyFilteredBTreeReader extends SortedFileGlobalIndexReader<BTreeIndexReader> {

    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final CacheManager cacheManager;
    private final GlobalIndexFileReader fileReader;

    public LazyFilteredBTreeReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            CacheManager cacheManager,
            long fallbackScanMaxSize,
            ExecutorService executor) {
        super(files, keySerializer, fallbackScanMaxSize, executor);
        this.cacheManager = cacheManager;
        this.fileReader = fileReader;
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
    }

    @Override
    protected Optional<GlobalIndexResult> visitIsNotNull(BTreeIndexReader reader) {
        return reader.visitIsNotNull();
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
    protected Optional<GlobalIndexResult> visitNotEqual(BTreeIndexReader reader, Object literal) {
        return reader.visitNotEqual(literal);
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
    protected Optional<GlobalIndexResult> visitNotIn(
            BTreeIndexReader reader, List<Object> literals) {
        return reader.visitNotIn(literals);
    }

    @Override
    protected Optional<GlobalIndexResult> visitBetween(
            BTreeIndexReader reader, Object from, Object to) {
        return reader.visitBetween(from, to);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitScalarSearch(
            ScalarSearch scalarSearch) {
        if (scalarSearch.topN().orders().size() != 1) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (scalarSearch.includeRowIds() != null && scalarSearch.includeRowIds().isEmpty()) {
            return CompletableFuture.completedFuture(Optional.of(GlobalIndexResult.createEmpty()));
        }
        return visitAllReaders(
                scalarSearch.maxIndexFiles(),
                scalarSearch.maxIndexBytes(),
                readers -> scalarSearch(readers, scalarSearch));
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
    protected BTreeIndexReader openReader(GlobalIndexIOMeta meta) {
        try {
            return new BTreeIndexReader(keySerializer, fileReader, meta, cacheManager);
        } catch (IOException e) {
            throw new RuntimeException("Can't create BTree index reader for " + meta.filePath(), e);
        }
    }

    private RoaringNavigableMap64 bitmap(Optional<GlobalIndexResult> result) {
        return result.get().results();
    }

    private Optional<GlobalIndexResult> scalarSearch(
            List<BTreeIndexReader> readers, ScalarSearch scalarSearch) {
        int limit = scalarSearch.topN().limit();
        BTreeIndexReader.ScalarSearchResultAccumulator result =
                new BTreeIndexReader.ScalarSearchResultAccumulator(
                        scalarSearch.maxResultSize(), scalarSearch.maxScannedRowIds());
        if (limit == 0) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }

        SortValue order = scalarSearch.topN().orders().get(0);
        if (order.nullOrdering() == SortValue.NullOrdering.NULLS_FIRST) {
            if (!addNullRows(readers, scalarSearch, result)) {
                return Optional.empty();
            }
            if (result.cardinality() >= limit) {
                return Optional.of(GlobalIndexResult.create(result.rowIds()));
            }
        }

        if (!addNonNullRows(readers, scalarSearch, order, result)) {
            return Optional.empty();
        }
        if (order.nullOrdering() == SortValue.NullOrdering.NULLS_LAST
                && result.cardinality() < limit
                && !addNullRows(readers, scalarSearch, result)) {
            return Optional.empty();
        }
        return Optional.of(GlobalIndexResult.create(result.rowIds()));
    }

    private boolean addNullRows(
            List<BTreeIndexReader> readers,
            ScalarSearch scalarSearch,
            BTreeIndexReader.ScalarSearchResultAccumulator result) {
        RoaringNavigableMap64 includeRowIds = scalarSearch.includeRowIds();
        for (BTreeIndexReader reader : readers) {
            Optional<RoaringNavigableMap64> optionalNullRowIds =
                    reader.nullRowIds(scalarSearch.maxReadBlockSize());
            if (!optionalNullRowIds.isPresent()) {
                return false;
            }
            RoaringNavigableMap64 nullRowIds = optionalNullRowIds.get();
            if (!result.reserveScannedRowIds(nullRowIds.getLongCardinality())) {
                return false;
            }
            for (long rowId : nullRowIds) {
                if (includeRowIds == null || includeRowIds.contains(rowId)) {
                    if (!result.add(rowId)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private boolean addNonNullRows(
            List<BTreeIndexReader> readers,
            ScalarSearch scalarSearch,
            SortValue order,
            BTreeIndexReader.ScalarSearchResultAccumulator result) {
        Comparator<EntryCursor> cursorComparator =
                (left, right) -> comparator.compare(left.current().key(), right.current().key());
        if (order.direction() == SortValue.SortDirection.DESCENDING) {
            cursorComparator = cursorComparator.reversed();
        }

        PriorityQueue<EntryCursor> cursors = new PriorityQueue<>(cursorComparator);
        try {
            for (BTreeIndexReader reader : readers) {
                EntryCursor cursor =
                        new EntryCursor(reader, order.direction(), scalarSearch.maxReadBlockSize());
                if (cursor.advance()) {
                    cursors.offer(cursor);
                }
            }

            while (!cursors.isEmpty()) {
                Object currentKey = cursors.peek().current().key();
                boolean matched = false;
                while (!cursors.isEmpty()
                        && comparator.compare(cursors.peek().current().key(), currentKey) == 0) {
                    EntryCursor cursor = cursors.poll();
                    matched |= addRows(cursor.current(), scalarSearch, result);
                    if (result.exceededBudget()) {
                        return false;
                    }
                    if (cursor.advance()) {
                        cursors.offer(cursor);
                    }
                }
                if (matched && result.cardinality() >= scalarSearch.topN().limit()) {
                    break;
                }
            }
            return true;
        } catch (SstFileReader.BlockTooLargeException ignored) {
            return false;
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan BTree index files for scalar TopN.", e);
        }
    }

    private boolean addRows(
            BTreeIndexReader.KeyRowIds keyRowIds,
            ScalarSearch scalarSearch,
            BTreeIndexReader.ScalarSearchResultAccumulator result) {
        if (!result.reserveScannedRowIds(keyRowIds.rowIdCount())) {
            return false;
        }
        boolean matched = false;
        RoaringNavigableMap64 includeRowIds = scalarSearch.includeRowIds();
        BTreeIndexReader.RowIdIterator iterator = keyRowIds.rowIdIterator();
        while (iterator.hasNext()) {
            long rowId = iterator.nextLong();
            if (includeRowIds == null || includeRowIds.contains(rowId)) {
                matched = true;
                if (!result.add(rowId)) {
                    return true;
                }
            }
        }
        return matched;
    }

    private static class EntryCursor {
        private final SortValue.SortDirection direction;
        private final BTreeIndexReader.EntryIterator forwardIterator;
        private final BTreeIndexReader.ReverseEntryIterator reverseIterator;
        private BTreeIndexReader.KeyRowIds current;

        private EntryCursor(
                BTreeIndexReader reader, SortValue.SortDirection direction, long maxReadBlockSize) {
            this.direction = direction;
            this.forwardIterator =
                    direction == SortValue.SortDirection.ASCENDING
                            ? reader.entryIterator(maxReadBlockSize)
                            : null;
            this.reverseIterator =
                    direction == SortValue.SortDirection.DESCENDING
                            ? reader.reverseEntryIterator(maxReadBlockSize)
                            : null;
        }

        private boolean advance() throws IOException {
            if (direction == SortValue.SortDirection.ASCENDING) {
                if (!forwardIterator.hasNext()) {
                    current = null;
                    return false;
                }
                current = forwardIterator.next();
                return true;
            }
            if (!reverseIterator.hasNext()) {
                current = null;
                return false;
            }
            current = reverseIterator.next();
            return true;
        }

        private BTreeIndexReader.KeyRowIds current() {
            return current;
        }
    }
}
