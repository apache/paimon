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
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * An Index Reader for BTree which dynamically filters file list by input predicate, then visits
 * each selected file in parallel via an executor. Each index file is synchronized independently to
 * allow maximum concurrency.
 */
public class LazyFilteredBTreeReader extends SortedFileGlobalIndexReader<BTreeIndexReader> {

    private final KeySerializer keySerializer;
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
    public java.util.concurrent.CompletableFuture<Optional<GlobalIndexResult>> visitScalarSearch(
            ScalarSearch scalarSearch) {
        return visitAllFiles(reader -> reader.visitScalarSearch(scalarSearch));
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
