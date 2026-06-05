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

import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/** Predicate for filtering data using global indexes. */
public class GlobalIndexEvaluator implements Closeable {

    private final RowType rowType;
    private final IntFunction<Collection<GlobalIndexReader>> readersFunction;
    private final Map<Integer, Collection<GlobalIndexReader>> indexReadersCache;

    public GlobalIndexEvaluator(
            RowType rowType, IntFunction<Collection<GlobalIndexReader>> readersFunction) {
        this.rowType = rowType;
        this.readersFunction = readersFunction;
        this.indexReadersCache = new ConcurrentHashMap<>();
    }

    public Optional<GlobalIndexResult> evaluate(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Optional.empty();
        }
        try {
            return visitAsync(predicate).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during index evaluation", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitAsync(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            return visitLeafAsync((LeafPredicate) predicate);
        }
        return visitCompoundAsync((CompoundPredicate) predicate);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitLeafAsync(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        FieldRef fieldRef = fieldRefOptional.get();
        int fieldId = rowType.getField(fieldRef.name()).id();
        Collection<GlobalIndexReader> readers =
                indexReadersCache.computeIfAbsent(fieldId, readersFunction::apply);

        List<CompletableFuture<Optional<GlobalIndexResult>>> readerFutures =
                new ArrayList<>(readers.size());
        for (GlobalIndexReader reader : readers) {
            readerFutures.add(predicate.function().visit(reader, fieldRef, predicate.literals()));
        }

        return CompletableFuture.allOf(readerFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            Optional<GlobalIndexResult> compoundResult = Optional.empty();
                            for (CompletableFuture<Optional<GlobalIndexResult>> f : readerFutures) {
                                Optional<GlobalIndexResult> childResult = f.join();
                                if (!childResult.isPresent()) {
                                    continue;
                                }
                                if (compoundResult.isPresent()) {
                                    compoundResult =
                                            Optional.of(
                                                    compoundResult.get().and(childResult.get()));
                                } else {
                                    compoundResult = childResult;
                                }
                                if (compoundResult.get().results().isEmpty()) {
                                    return compoundResult;
                                }
                            }
                            return compoundResult;
                        });
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitCompoundAsync(
            CompoundPredicate predicate) {
        List<Predicate> children = flattenChildren(predicate);
        List<CompletableFuture<Optional<GlobalIndexResult>>> childFutures =
                new ArrayList<>(children.size());
        for (Predicate child : children) {
            childFutures.add(visitAsync(child));
        }

        return CompletableFuture.allOf(childFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            List<Optional<GlobalIndexResult>> results = new ArrayList<>();
                            for (CompletableFuture<Optional<GlobalIndexResult>> f : childFutures) {
                                results.add(f.join());
                            }
                            return combineResults(results, predicate);
                        });
    }

    private Optional<GlobalIndexResult> combineResults(
            List<Optional<GlobalIndexResult>> results, CompoundPredicate predicate) {
        if (predicate.function() instanceof Or) {
            GlobalIndexResult compoundResult = GlobalIndexResult.createEmpty();
            for (Optional<GlobalIndexResult> childResult : results) {
                if (!childResult.isPresent()) {
                    return Optional.empty();
                }
                compoundResult = compoundResult.or(childResult.get());
            }
            return Optional.of(compoundResult);
        } else {
            Optional<GlobalIndexResult> compoundResult = Optional.empty();
            for (Optional<GlobalIndexResult> childResult : results) {
                if (childResult.isPresent()) {
                    if (compoundResult.isPresent()) {
                        compoundResult = Optional.of(compoundResult.get().and(childResult.get()));
                    } else {
                        compoundResult = childResult;
                    }
                }
                if (compoundResult.isPresent() && compoundResult.get().results().isEmpty()) {
                    return compoundResult;
                }
            }
            return compoundResult;
        }
    }

    private List<Predicate> flattenChildren(CompoundPredicate predicate) {
        List<Predicate> result = new ArrayList<>();
        Deque<Predicate> stack = new ArrayDeque<>(predicate.children());
        while (!stack.isEmpty()) {
            Predicate child = stack.pollFirst();
            if (child instanceof CompoundPredicate) {
                CompoundPredicate compound = (CompoundPredicate) child;
                if (compound.function().equals(predicate.function())) {
                    List<Predicate> grandChildren = compound.children();
                    for (int i = grandChildren.size() - 1; i >= 0; i--) {
                        stack.addFirst(grandChildren.get(i));
                    }
                    continue;
                }
            }
            result.add(child);
        }
        return result;
    }

    @Override
    public void close() {
        IOUtils.closeAllQuietly(
                indexReadersCache.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
    }
}
