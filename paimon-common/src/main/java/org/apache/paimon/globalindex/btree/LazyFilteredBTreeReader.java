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
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
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
    private final long rangeWidth;

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
        this.rangeWidth = rangeWidth(files);
    }

    /**
     * The dense local id universe [0, rangeWidth) shared by all files of this range, i.e. the sum
     * of the per-file row counts, or -1 when any count is unknown (older metadata) so the negations
     * fall back to a full-file scan.
     */
    private static long rangeWidth(List<GlobalIndexIOMeta> files) {
        long total = 0;
        for (GlobalIndexIOMeta file : files) {
            long count = file.rowCount();
            if (count < 0) {
                return -1;
            }
            total += count;
        }
        return total;
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        if (rangeWidth <= 0) {
            return super.visitIsNotNull(fieldRef);
        }
        return nonNullRowsInRange();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        if (rangeWidth <= 0) {
            return super.visitNotEqual(fieldRef, literal);
        }
        return nonNullRowsInRange()
                .thenCombine(
                        super.visitEqual(fieldRef, literal), LazyFilteredBTreeReader::subtract);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        if (rangeWidth <= 0) {
            return super.visitNotIn(fieldRef, literals);
        }
        return nonNullRowsInRange()
                .thenCombine(super.visitIn(fieldRef, literals), LazyFilteredBTreeReader::subtract);
    }

    @Override
    protected CompletableFuture<Optional<GlobalIndexResult>> visitConservativeString(
            FieldRef fieldRef,
            Object literal,
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector,
            Function<BTreeIndexReader, Optional<GlobalIndexResult>> visitor) {
        if (rangeWidth > 0
                && literal != null
                && fieldRef.type().is(DataTypeFamily.CHARACTER_STRING)) {
            return nonNullRowsInRange();
        }
        return super.visitConservativeString(fieldRef, literal, selector, visitor);
    }

    /**
     * The non-null rows of this range: the dense id universe [0, rangeWidth) minus the union of
     * every file's null bitmap. All files of a range share this id space, so unioning their null
     * bitmaps yields the range's null ids exactly. Every bitmap built here is fresh, so this is
     * safe under concurrent visits on one shared reader.
     */
    private CompletableFuture<Optional<GlobalIndexResult>> nonNullRowsInRange() {
        return visitAllFiles(reader -> Optional.of(GlobalIndexResult.create(reader.nullRows())))
                .thenApply(
                        nullUnion -> {
                            RoaringNavigableMap64 result = new RoaringNavigableMap64();
                            result.addRange(new Range(0, rangeWidth - 1));
                            nullUnion.ifPresent(u -> result.andNot(u.results()));
                            return Optional.of(GlobalIndexResult.create(result));
                        });
    }

    private static Optional<GlobalIndexResult> subtract(
            Optional<GlobalIndexResult> nonNull, Optional<GlobalIndexResult> matched) {
        RoaringNavigableMap64 result = nonNull.get().results();
        matched.ifPresent(m -> result.andNot(m.results()));
        return Optional.of(GlobalIndexResult.create(result));
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
}
