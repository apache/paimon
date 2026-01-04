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
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/** Predicate for filtering data using global indexes. */
public class GlobalIndexEvaluator
        implements Closeable, PredicateVisitor<Optional<GlobalIndexResult>> {

    private final RowType rowType;
    private final IntFunction<Collection<GlobalIndexReader>> readersFunction;
    private final Map<Integer, Collection<GlobalIndexReader>> indexReadersCache = new HashMap<>();

    public GlobalIndexEvaluator(
            RowType rowType, IntFunction<Collection<GlobalIndexReader>> readersFunction) {
        this.rowType = rowType;
        this.readersFunction = readersFunction;
    }

    public Optional<GlobalIndexResult> evaluate(
            @Nullable Predicate predicate, @Nullable VectorSearch vectorSearch) {
        Optional<GlobalIndexResult> compoundResult = Optional.empty();
        if (predicate != null) {
            compoundResult = predicate.visit(this);
        }
        if (vectorSearch != null) {
            int fieldId = rowType.getField(vectorSearch.fieldName()).id();
            Collection<GlobalIndexReader> readers =
                    indexReadersCache.computeIfAbsent(fieldId, readersFunction::apply);
            if (compoundResult.isPresent()) {
                vectorSearch = vectorSearch.withIncludeRowIds(compoundResult.get().results());
            }
            for (GlobalIndexReader fileIndexReader : readers) {
                Optional<GlobalIndexResult> childResult = vectorSearch.visit(fileIndexReader);
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
        }
        return compoundResult;
    }

    @Override
    public Optional<GlobalIndexResult> visit(LeafPredicate predicate) {
        Optional<GlobalIndexResult> compoundResult = Optional.empty();
        FieldRef fieldRef =
                new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
        int fieldId = rowType.getField(predicate.fieldName()).id();
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

    @Override
    public Optional<GlobalIndexResult> visit(TransformPredicate predicate) {
        return Optional.empty();
    }

    public void close() {
        IOUtils.closeAllQuietly(
                indexReadersCache.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()));
    }
}
