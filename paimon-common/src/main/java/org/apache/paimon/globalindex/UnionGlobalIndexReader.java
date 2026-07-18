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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.ScalarSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link GlobalIndexReader} that combines results from multiple readers by performing a union
 * (OR) operation on their results.
 */
public class UnionGlobalIndexReader implements GlobalIndexReader {

    private final List<GlobalIndexReader> readers;

    public UnionGlobalIndexReader(List<GlobalIndexReader> readers) {
        this.readers = readers;
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return unionAsync(reader -> reader.visitIsNotNull(fieldRef));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return unionAsync(reader -> reader.visitIsNull(fieldRef));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitStartsWith(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitEndsWith(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitContains(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitLike(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitLessThan(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitNotEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitEqual(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return unionAsync(reader -> reader.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return unionAsync(reader -> reader.visitIn(fieldRef, literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return unionAsync(reader -> reader.visitNotIn(fieldRef, literals));
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return unionAsync(reader -> reader.visitBetween(fieldRef, from, to));
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(readers.size());
        for (GlobalIndexReader reader : readers) {
            futures.add(reader.visitVectorSearch(vectorSearch));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            Optional<ScoredGlobalIndexResult> result = Optional.empty();
                            for (CompletableFuture<Optional<ScoredGlobalIndexResult>> f : futures) {
                                Optional<ScoredGlobalIndexResult> current = f.join();
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
                        });
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitScalarSearch(
            ScalarSearch scalarSearch) {
        // Scalar results from child readers cover different row ranges. Returning a partial union
        // could drop the actual TopN rows, so every child must support and complete the search.
        if (readers.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        int concurrency = Math.min(readers.size(), scalarSearch.maxConcurrentReaders());
        if (concurrency == 0) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        long childMaxResultSize = scalarSearch.maxResultSize() / readers.size();
        long childMaxScannedRowIds = scalarSearch.maxScannedRowIds() / readers.size();
        int childMaxIndexFiles = scalarSearch.maxIndexFiles() / readers.size();
        long childMaxIndexBytes = scalarSearch.maxIndexBytes() / readers.size();
        int childMaxConcurrentReaders =
                Math.max(1, scalarSearch.maxConcurrentReaders() / concurrency);
        if (childMaxResultSize < scalarSearch.topN().limit()
                || childMaxScannedRowIds < scalarSearch.topN().limit()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        ScalarSearch childSearch =
                scalarSearch
                        .withMaxResultSize(childMaxResultSize)
                        .withMaxScannedRowIds(childMaxScannedRowIds)
                        .withMaxIndexFiles(childMaxIndexFiles)
                        .withMaxIndexBytes(childMaxIndexBytes)
                        .withMaxConcurrentReaders(childMaxConcurrentReaders);
        List<CompletableFuture<ScalarUnionAccumulator>> futures = new ArrayList<>(concurrency);
        for (int lane = 0; lane < concurrency; lane++) {
            futures.add(
                    visitScalarLane(childSearch, scalarSearch.maxResultSize(), lane, concurrency));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            ScalarUnionAccumulator accumulator = new ScalarUnionAccumulator();
                            for (CompletableFuture<ScalarUnionAccumulator> future : futures) {
                                accumulator.merge(future.join(), scalarSearch.maxResultSize());
                            }
                            return accumulator.result();
                        });
    }

    private CompletableFuture<ScalarUnionAccumulator> visitScalarLane(
            ScalarSearch childSearch, long maxResultSize, int start, int stride) {
        CompletableFuture<ScalarUnionAccumulator> future =
                CompletableFuture.completedFuture(new ScalarUnionAccumulator());
        for (int i = start; i < readers.size(); i += stride) {
            GlobalIndexReader reader = readers.get(i);
            future =
                    future.thenCompose(
                            current -> {
                                if (current.failed) {
                                    return CompletableFuture.completedFuture(current);
                                }
                                try {
                                    return reader.visitScalarSearch(childSearch)
                                            .thenApply(
                                                    child -> {
                                                        current.add(child, maxResultSize);
                                                        return current;
                                                    });
                                } catch (UnsupportedOperationException ignored) {
                                    current.failed = true;
                                    return CompletableFuture.completedFuture(current);
                                }
                            });
        }
        return future;
    }

    private CompletableFuture<Optional<GlobalIndexResult>> unionAsync(
            Function<GlobalIndexReader, CompletableFuture<Optional<GlobalIndexResult>>> visitor) {
        List<CompletableFuture<Optional<GlobalIndexResult>>> futures =
                new ArrayList<>(readers.size());
        for (GlobalIndexReader reader : readers) {
            futures.add(visitor.apply(reader));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            Optional<GlobalIndexResult> result = Optional.empty();
                            boolean exact = true;
                            for (CompletableFuture<Optional<GlobalIndexResult>> f : futures) {
                                Optional<GlobalIndexResult> current = f.join();
                                if (!current.isPresent()) {
                                    exact = false;
                                    continue;
                                }
                                if (!result.isPresent()) {
                                    result = current;
                                } else {
                                    result = Optional.of(result.get().or(current.get()));
                                }
                            }
                            if (!result.isPresent()) {
                                return result;
                            }
                            return Optional.of(
                                    result.get().withExact(exact && result.get().isExact()));
                        });
    }

    private static class ScalarUnionAccumulator {
        private final RoaringNavigableMap64 rowIds = new RoaringNavigableMap64();
        private boolean hasResult;
        private boolean failed;
        private long resultSize;

        private void add(Optional<GlobalIndexResult> result, long maxResultSize) {
            if (!result.isPresent()) {
                failed = true;
                return;
            }
            long currentSize = result.get().results().getLongCardinality();
            if (Long.MAX_VALUE - resultSize < currentSize) {
                failed = true;
                return;
            }
            resultSize += currentSize;
            if (resultSize > maxResultSize) {
                failed = true;
                return;
            }
            rowIds.or(result.get().results());
            hasResult = true;
        }

        private void merge(ScalarUnionAccumulator other, long maxResultSize) {
            if (failed) {
                return;
            }
            if (other.failed || Long.MAX_VALUE - resultSize < other.resultSize) {
                failed = true;
                return;
            }
            resultSize += other.resultSize;
            if (resultSize > maxResultSize) {
                failed = true;
                return;
            }
            rowIds.or(other.rowIds);
            hasResult |= other.hasResult;
        }

        private Optional<GlobalIndexResult> result() {
            if (failed || !hasResult) {
                return Optional.empty();
            }
            return Optional.of(GlobalIndexResult.create(rowIds));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(readers);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to close union global index readers", e);
        }
    }
}
