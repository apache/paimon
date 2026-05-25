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
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/** Predicate for filtering data using global indexes. */
public class GlobalIndexEvaluator
        implements Closeable, PredicateVisitor<Optional<GlobalIndexResult>> {

    private final RowType rowType;
    private final IntFunction<Collection<GlobalIndexReader>> readersFunction;
    private final Map<Integer, Collection<GlobalIndexReader>> indexReadersCache;
    @Nullable private final ExecutorService executorService;

    public GlobalIndexEvaluator(
            RowType rowType, IntFunction<Collection<GlobalIndexReader>> readersFunction) {
        this(rowType, readersFunction, null);
    }

    public GlobalIndexEvaluator(
            RowType rowType,
            IntFunction<Collection<GlobalIndexReader>> readersFunction,
            @Nullable ExecutorService executorService) {
        this.rowType = rowType;
        this.readersFunction = readersFunction;
        this.executorService = executorService;
        this.indexReadersCache =
                executorService != null ? new ConcurrentHashMap<>() : new HashMap<>();
    }

    public Optional<GlobalIndexResult> evaluate(@Nullable Predicate predicate) {
        return predicate == null ? Optional.empty() : predicate.visit(this);
    }

    @Override
    public Optional<GlobalIndexResult> visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
        if (!fieldRefOptional.isPresent()) {
            return Optional.empty();
        }
        Optional<GlobalIndexResult> compoundResult = Optional.empty();
        FieldRef fieldRef = fieldRefOptional.get();
        int fieldId = rowType.getField(fieldRef.name()).id();
        Collection<GlobalIndexReader> readers =
                indexReadersCache.computeIfAbsent(fieldId, readersFunction::apply);
        for (GlobalIndexReader fileIndexReader : readers) {
            Optional<GlobalIndexResult> childResult =
                    predicate.function().visit(fileIndexReader, fieldRef, predicate.literals());
            if (!childResult.isPresent()) {
                continue;
            }

            GlobalIndexResult result = childResult.get();

            // AND Operation
            if (compoundResult.isPresent()) {
                GlobalIndexResult r1 = compoundResult.get();
                compoundResult = Optional.of(r1.and(result));
            } else {
                compoundResult = Optional.of(result);
            }

            if (compoundResult.get().results().isEmpty()) {
                return compoundResult;
            }
        }
        return compoundResult;
    }

    @Override
    public Optional<GlobalIndexResult> visit(CompoundPredicate predicate) {
        if (executorService != null && predicate.children().size() > 1) {
            return visitParallel(predicate);
        }
        return visitSequential(predicate);
    }

    private Optional<GlobalIndexResult> visitSequential(CompoundPredicate predicate) {
        if (predicate.function() instanceof Or) {
            GlobalIndexResult compoundResult = GlobalIndexResult.createEmpty();
            for (Predicate predicate1 : predicate.children()) {
                Optional<GlobalIndexResult> childResult = predicate1.visit(this);

                if (!childResult.isPresent()) {
                    return Optional.empty();
                }
                compoundResult = compoundResult.or(childResult.get());
            }
            return Optional.of(compoundResult);
        } else {
            Optional<GlobalIndexResult> compoundResult = Optional.empty();
            for (Predicate predicate1 : predicate.children()) {
                Optional<GlobalIndexResult> childResult = predicate1.visit(this);

                // AND Operation
                if (childResult.isPresent()) {
                    if (compoundResult.isPresent()) {
                        GlobalIndexResult r1 = compoundResult.get();
                        GlobalIndexResult r2 = childResult.get();
                        compoundResult = Optional.of(r1.and(r2));
                    } else {
                        compoundResult = childResult;
                    }
                }

                // if not remain, no need to test anymore
                if (compoundResult.isPresent() && compoundResult.get().results().isEmpty()) {
                    return compoundResult;
                }
            }
            return compoundResult;
        }
    }

    private Optional<GlobalIndexResult> visitParallel(CompoundPredicate predicate) {
        List<Predicate> children = flattenChildren(predicate);
        List<Future<Optional<GlobalIndexResult>>> futures = new ArrayList<>(children.size());
        for (Predicate child : children) {
            futures.add(executorService.submit(() -> evaluateChildSequentially(child)));
        }

        List<Optional<GlobalIndexResult>> results = new ArrayList<>(children.size());
        for (Future<Optional<GlobalIndexResult>> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw new RuntimeException(cause);
            }
        }

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

    private Optional<GlobalIndexResult> evaluateChildSequentially(Predicate child) {
        if (child instanceof CompoundPredicate) {
            return visitSequential((CompoundPredicate) child);
        }
        return child.visit(this);
    }

    private List<Predicate> flattenChildren(CompoundPredicate predicate) {
        List<Predicate> result = new ArrayList<>();
        for (Predicate child : predicate.children()) {
            if (child instanceof CompoundPredicate) {
                CompoundPredicate compound = (CompoundPredicate) child;
                if (compound.function().getClass() == predicate.function().getClass()) {
                    result.addAll(flattenChildren(compound));
                    continue;
                }
            }
            result.add(child);
        }
        return result;
    }

    public void close() {
        IOUtils.closeAllQuietly(
                indexReadersCache.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
    }
}
