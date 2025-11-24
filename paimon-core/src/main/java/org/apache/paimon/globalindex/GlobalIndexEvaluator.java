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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Predicate for filtering data using global indexes. */
public class GlobalIndexEvaluator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexEvaluator.class);

    private final Function<Integer, Collection<GlobalIndexReader>> readersFunction;
    private final Map<String, DataField> fieldNameToField;
    private final Map<Integer, Collection<GlobalIndexReader>> indexReadersCache = new HashMap<>();

    public GlobalIndexEvaluator(
            Function<Integer, Collection<GlobalIndexReader>> readersFunction, RowType rowType) {
        this.readersFunction = readersFunction;
        this.fieldNameToField = new HashMap<>();
        for (DataField dataField : rowType.getFields()) {
            fieldNameToField.put(dataField.name(), dataField);
        }
    }

    public Optional<GlobalIndexResult> evaluate(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Optional.empty();
        }
        Set<Integer> requiredFieldIds = getRequiredFieldIds(predicate);
        requiredFieldIds.forEach(id -> indexReadersCache.computeIfAbsent(id, readersFunction));
        return new FileIndexPredicateTest(fieldNameToField, indexReadersCache).test(predicate);
    }

    public void close() {
        closePredicatorsQuietly(
                indexReadersCache.values().stream().flatMap(Collection::stream).iterator());
    }

    private void closePredicatorsQuietly(Iterator<GlobalIndexReader> iterator) {
        while (iterator.hasNext()) {
            try {
                iterator.next().close();
            } catch (IOException e) {
                LOG.warn("Failed to close GlobalIndexLeafPredicator", e);
            }
        }
    }

    private Set<Integer> getRequiredFieldIds(Predicate filePredicate) {
        return filePredicate.visit(
                new PredicateVisitor<Set<Integer>>() {

                    @Override
                    public Set<Integer> visit(LeafPredicate predicate) {
                        return Collections.singleton(
                                fieldNameToField.get(predicate.fieldName()).id());
                    }

                    @Override
                    public Set<Integer> visit(CompoundPredicate predicate) {
                        Set<Integer> result = new HashSet<>();
                        for (Predicate child : predicate.children()) {
                            child.visit(this);
                            result.addAll(child.visit(this));
                        }
                        return result;
                    }

                    @Override
                    public Set<Integer> visit(TransformPredicate predicate) {
                        return predicate.fieldNames().stream()
                                .map(f -> fieldNameToField.get(f).id())
                                .collect(Collectors.toSet());
                    }
                });
    }

    /** Predicate test worker. */
    private static class FileIndexPredicateTest
            implements PredicateVisitor<Optional<GlobalIndexResult>> {

        private final Map<String, DataField> fieldColumnToId;
        private final Map<Integer, Collection<GlobalIndexReader>> columnIndexReaders;

        public FileIndexPredicateTest(
                Map<String, DataField> fieldColumnToId,
                Map<Integer, Collection<GlobalIndexReader>> fileIndexReaders) {
            this.fieldColumnToId = fieldColumnToId;
            this.columnIndexReaders = fileIndexReaders;
        }

        public Optional<GlobalIndexResult> test(Predicate predicate) {
            return predicate.visit(this);
        }

        @Override
        public Optional<GlobalIndexResult> visit(LeafPredicate predicate) {
            Optional<GlobalIndexResult> compoundResult = Optional.empty();
            FieldRef fieldRef =
                    new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
            for (GlobalIndexReader fileIndexReader :
                    columnIndexReaders.get(fieldColumnToId.get(predicate.fieldName()).id())) {
                Optional<GlobalIndexResult> childResult =
                        predicate.function().visit(fileIndexReader, fieldRef, predicate.literals());

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

                if (compoundResult.isPresent() && !compoundResult.get().iterator().hasNext()) {
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
                    if (compoundResult.isPresent() && !compoundResult.get().iterator().hasNext()) {
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
    }
}
