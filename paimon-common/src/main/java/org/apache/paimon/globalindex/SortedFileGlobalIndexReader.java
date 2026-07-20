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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafBinaryFunction;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.LikeOptimization;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

/** Base reader for sorted global index files with manifest-level min/max pruning. */
public abstract class SortedFileGlobalIndexReader<R extends Closeable>
        implements GlobalIndexReader {

    private final List<GlobalIndexIOMeta> files;
    private final SortedFileMetaSelector fileSelector;
    private final long fallbackScanMaxSize;
    private final Map<Path, R> readerCache;
    private final ExecutorService executor;

    protected SortedFileGlobalIndexReader(
            List<GlobalIndexIOMeta> files,
            KeySerializer keySerializer,
            long fallbackScanMaxSize,
            ExecutorService executor) {
        this.files = files;
        this.fileSelector = new SortedFileMetaSelector(files, keySerializer);
        this.fallbackScanMaxSize = fallbackScanMaxSize;
        this.readerCache = new ConcurrentHashMap<>();
        this.executor = executor;
    }

    /** Fans out {@code visitor} over every file of this range and unions the per-file results. */
    protected CompletableFuture<Optional<GlobalIndexResult>> visitAllFiles(
            Function<R, Optional<GlobalIndexResult>> visitor) {
        return visitSelectedFiles(Optional.of(files), visitor);
    }

    /**
     * Conservative "all non-null rows" answer shared by the string negations that cannot prune
     * (ENDS WITH / CONTAINS / unoptimizable LIKE). The default is a budget-gated fallback scan;
     * subclasses that can derive it cheaply may override.
     */
    protected CompletableFuture<Optional<GlobalIndexResult>> visitConservativeString(
            FieldRef fieldRef,
            Object literal,
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector,
            Function<R, Optional<GlobalIndexResult>> visitor) {
        if (!canFallbackStringScan(fieldRef, literal)) {
            return unsupported();
        }
        return visitFallbackParallel(selector, visitor);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return visitParallel(() -> fileSelector.visitIsNotNull(fieldRef), this::visitIsNotNull);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return visitParallel(() -> fileSelector.visitIsNull(fieldRef), this::visitIsNull);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        if (!fieldRef.type().is(DataTypeFamily.CHARACTER_STRING) || literal == null) {
            return unsupported();
        }
        return visitParallel(
                () -> fileSelector.visitStartsWith(fieldRef, literal),
                reader -> visitStartsWith(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return visitConservativeString(
                fieldRef,
                literal,
                () -> fileSelector.visitEndsWith(fieldRef, literal),
                reader -> visitEndsWith(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return visitConservativeString(
                fieldRef,
                literal,
                () -> fileSelector.visitContains(fieldRef, literal),
                reader -> visitContains(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        if (!fieldRef.type().is(DataTypeFamily.CHARACTER_STRING) || literal == null) {
            return unsupported();
        }

        Optional<Pair<LeafBinaryFunction, Object>> optimized =
                LikeOptimization.tryOptimize(literal);
        if (!optimized.isPresent()) {
            return visitConservativeString(
                    fieldRef,
                    literal,
                    () -> fileSelector.visitLike(fieldRef, literal),
                    reader -> visitLike(reader, fieldRef, literal));
        }

        LeafBinaryFunction function = optimized.get().getKey();
        Object optimizedLiteral = optimized.get().getValue();
        if (function == Equal.INSTANCE) {
            return visitEqual(fieldRef, optimizedLiteral);
        }
        if (function == StartsWith.INSTANCE) {
            return visitStartsWith(fieldRef, optimizedLiteral);
        }
        if (function == EndsWith.INSTANCE) {
            return visitEndsWith(fieldRef, optimizedLiteral);
        }
        if (function == Contains.INSTANCE) {
            return visitContains(fieldRef, optimizedLiteral);
        }
        return unsupported();
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        if (!canFallbackScan(literal)) {
            return unsupported();
        }
        return visitFallbackParallel(
                () -> fileSelector.visitLessThan(fieldRef, literal),
                reader -> visitLessThan(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        if (!canFallbackScan(literal)) {
            return unsupported();
        }
        return visitFallbackParallel(
                () -> fileSelector.visitGreaterOrEqual(fieldRef, literal),
                reader -> visitGreaterOrEqual(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitNotEqual(fieldRef, literal),
                reader -> visitNotEqual(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        if (!canFallbackScan(literal)) {
            return unsupported();
        }
        return visitFallbackParallel(
                () -> fileSelector.visitLessOrEqual(fieldRef, literal),
                reader -> visitLessOrEqual(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return visitParallel(
                () -> fileSelector.visitEqual(fieldRef, literal),
                reader -> visitEqual(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        if (!canFallbackScan(literal)) {
            return unsupported();
        }
        return visitFallbackParallel(
                () -> fileSelector.visitGreaterThan(fieldRef, literal),
                reader -> visitGreaterThan(reader, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return visitParallel(
                () -> fileSelector.visitIn(fieldRef, literals),
                reader -> visitIn(reader, literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return visitParallel(
                () -> fileSelector.visitNotIn(fieldRef, literals),
                reader -> visitNotIn(reader, literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        if (!canFallbackScan(from) || to == null) {
            return unsupported();
        }
        return visitFallbackParallel(
                () -> fileSelector.visitBetween(fieldRef, from, to),
                reader -> visitBetween(reader, from, to));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotBetween(
            FieldRef fieldRef, Object from, Object to) {
        if (!canFallbackScan(from) || to == null) {
            return unsupported();
        }
        return visitFallbackParallel(
                () ->
                        fileSelector.visitOr(
                                Arrays.asList(
                                        fileSelector.visitLessThan(fieldRef, from),
                                        fileSelector.visitGreaterThan(fieldRef, to))),
                reader -> visitNotBetween(reader, from, to));
    }

    protected abstract Optional<GlobalIndexResult> visitIsNotNull(R reader);

    protected abstract Optional<GlobalIndexResult> visitIsNull(R reader);

    protected abstract Optional<GlobalIndexResult> visitStartsWith(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitEndsWith(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitContains(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitLessThan(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitGreaterOrEqual(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitNotEqual(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitLessOrEqual(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitEqual(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitGreaterThan(R reader, Object literal);

    protected abstract Optional<GlobalIndexResult> visitIn(R reader, List<Object> literals);

    protected abstract Optional<GlobalIndexResult> visitNotIn(R reader, List<Object> literals);

    protected abstract Optional<GlobalIndexResult> visitBetween(R reader, Object from, Object to);

    protected Optional<GlobalIndexResult> visitLike(R reader, FieldRef fieldRef, Object literal) {
        return createResult(like(reader, key -> Like.INSTANCE.test(fieldRef.type(), key, literal)));
    }

    protected Optional<GlobalIndexResult> visitNotBetween(R reader, Object from, Object to) {
        RoaringNavigableMap64 result = lessThan(reader, from);
        result.or(greaterThan(reader, to));
        return Optional.of(GlobalIndexResult.create(result));
    }

    protected RoaringNavigableMap64 like(R reader, Function<Object, Boolean> keyPredicate) {
        throw new UnsupportedOperationException();
    }

    protected abstract RoaringNavigableMap64 lessThan(R reader, Object literal);

    protected abstract RoaringNavigableMap64 greaterThan(R reader, Object literal);

    protected Optional<GlobalIndexResult> createResult(RoaringNavigableMap64 bitmap) {
        return Optional.of(GlobalIndexResult.create(bitmap));
    }

    protected abstract R openReader(GlobalIndexIOMeta meta);

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (R reader : readerCache.values()) {
            try {
                reader.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private boolean canFallbackStringScan(FieldRef fieldRef, Object literal) {
        return fallbackScanMaxSize > 0
                && fieldRef.type().is(DataTypeFamily.CHARACTER_STRING)
                && literal != null;
    }

    private boolean canFallbackScan(Object literal) {
        return fallbackScanMaxSize > 0 && literal != null;
    }

    private static boolean fallbackScanEnabled(List<GlobalIndexIOMeta> files, long maxSize) {
        if (maxSize <= 0) {
            return false;
        }
        long totalSize = 0;
        for (GlobalIndexIOMeta file : files) {
            if (Long.MAX_VALUE - totalSize < file.fileSize()) {
                return false;
            }
            totalSize += file.fileSize();
            if (totalSize > maxSize) {
                return false;
            }
        }
        return true;
    }

    private CompletableFuture<Optional<GlobalIndexResult>> unsupported() {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitParallel(
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector,
            Function<R, Optional<GlobalIndexResult>> visitor) {
        return visitSelectedFiles(selector.get(), visitor);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitFallbackParallel(
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector,
            Function<R, Optional<GlobalIndexResult>> visitor) {
        Optional<List<GlobalIndexIOMeta>> selectedOpt = selector.get();
        if (!selectedOpt.isPresent()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        List<GlobalIndexIOMeta> selected = selectedOpt.get();
        if (selected.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.of(GlobalIndexResult.createEmpty()));
        }
        if (!fallbackScanEnabled(selected, fallbackScanMaxSize)) {
            return unsupported();
        }
        return visitSelectedFiles(selectedOpt, visitor);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitSelectedFiles(
            Optional<List<GlobalIndexIOMeta>> selectedOpt,
            Function<R, Optional<GlobalIndexResult>> visitor) {
        if (!selectedOpt.isPresent()) {
            return unsupported();
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

    private R getOrCreateReader(GlobalIndexIOMeta meta) {
        return readerCache.computeIfAbsent(meta.filePath(), ignored -> openReader(meta));
    }

    private Optional<GlobalIndexResult> unionResults(
            List<CompletableFuture<Optional<GlobalIndexResult>>> futures) {
        Optional<GlobalIndexResult> result = Optional.empty();
        for (CompletableFuture<Optional<GlobalIndexResult>> future : futures) {
            Optional<GlobalIndexResult> current = future.join();
            if (!current.isPresent()) {
                continue;
            }
            result = result.isPresent() ? Optional.of(result.get().or(current.get())) : current;
        }
        return result;
    }
}
