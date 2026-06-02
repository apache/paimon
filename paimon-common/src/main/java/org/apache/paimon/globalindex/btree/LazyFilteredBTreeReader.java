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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.predicate.FieldRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An Index Reader for BTree which dynamically filters file list by input predicate, then visits
 * each selected file in parallel via an executor. Each index file is synchronized independently to
 * allow maximum concurrency.
 */
public class LazyFilteredBTreeReader implements GlobalIndexReader {

    private final BTreeFileMetaSelector fileSelector;
    private final Map<Path, BTreeIndexReader> readerCache;
    private final KeySerializer keySerializer;
    private final CacheManager cacheManager;
    private final GlobalIndexFileReader fileReader;
    private final ExecutorService executor;

    public LazyFilteredBTreeReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            GlobalIndexFileReader fileReader,
            CacheManager cacheManager,
            ExecutorService executor) {
        this.fileSelector = new BTreeFileMetaSelector(files, keySerializer);
        this.readerCache = new ConcurrentHashMap<>();
        this.cacheManager = cacheManager;
        this.fileReader = fileReader;
        this.keySerializer = keySerializer;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return visitParallel(
                () -> fileSelector.visitIsNotNull(fieldRef), BTreeIndexReader::visitIsNotNull);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return visitParallel(
                () -> fileSelector.visitIsNull(fieldRef), BTreeIndexReader::visitIsNull);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitStartsWith(fieldRef, literal),
                reader -> reader.visitStartsWith(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitEndsWith(fieldRef, literal),
                reader -> reader.visitEndsWith(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitContains(fieldRef, literal),
                reader -> reader.visitContains(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitLike(fieldRef, literal),
                reader -> reader.visitLike(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitLessThan(fieldRef, literal),
                reader -> reader.visitLessThan(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitGreaterOrEqual(fieldRef, literal),
                reader -> reader.visitGreaterOrEqual(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitNotEqual(fieldRef, literal),
                reader -> reader.visitNotEqual(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitLessOrEqual(fieldRef, literal),
                reader -> reader.visitLessOrEqual(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitEqual(fieldRef, literal),
                reader -> reader.visitEqual(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitGreaterThan(fieldRef, literal),
                reader -> reader.visitGreaterThan(literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return visitParallel(
                () -> fileSelector.visitIn(fieldRef, literals), reader -> reader.visitIn(literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return visitParallel(
                () -> fileSelector.visitNotIn(fieldRef, literals),
                reader -> reader.visitNotIn(literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return visitParallel(
                () -> fileSelector.visitBetween(fieldRef, from, to),
                reader -> reader.visitBetween(from, to));
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitParallel(
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector,
            Function<BTreeIndexReader, Optional<GlobalIndexResult>> visitor) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = selector.get();
        if (!selectedOpt.isPresent()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.of(GlobalIndexResult.createEmpty()));
        }

        List<CompletableFuture<Optional<GlobalIndexResult>>> futures =
                new ArrayList<>(selected.size());
        for (GlobalIndexIOMeta meta : selected) {
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> visitor.apply(getOrCreateReader(meta)), executor));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> unionResults(futures));
    }

    private Optional<GlobalIndexResult> unionResults(
            List<CompletableFuture<Optional<GlobalIndexResult>>> futures) {
        Optional<GlobalIndexResult> result = Optional.empty();
        for (CompletableFuture<Optional<GlobalIndexResult>> future : futures) {
            Optional<GlobalIndexResult> current = future.join();
            if (!current.isPresent()) {
                continue;
            }
            if (!result.isPresent()) {
                result = current;
            } else {
                result = Optional.of(result.get().or(current.get()));
            }
        }
        return result;
    }

    private BTreeIndexReader getOrCreateReader(GlobalIndexIOMeta meta) {
        return readerCache.computeIfAbsent(meta.filePath(), k -> createBTreeReader(meta));
    }

    private BTreeIndexReader createBTreeReader(GlobalIndexIOMeta meta) {
        try {
            return new BTreeIndexReader(keySerializer, fileReader, meta, cacheManager);
        } catch (IOException e) {
            throw new RuntimeException("Can't create BTree index reader for " + meta.filePath(), e);
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (Map.Entry<Path, BTreeIndexReader> entry : this.readerCache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException ioe) {
                if (exception == null) {
                    exception = ioe;
                } else {
                    exception.addSuppressed(ioe);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
