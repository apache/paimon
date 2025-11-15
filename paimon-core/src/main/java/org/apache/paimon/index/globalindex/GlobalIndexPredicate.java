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

package org.apache.paimon.index.globalindex;

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
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.index.globalindex.GlobalIndexResult.ALL;

public class GlobalIndexPredicate implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexPredicate.class);

    private final Function<Integer, Collection<GlobalIndexLeafPredicator>> readersFunction;
    private final Map<String, DataField> fieldColumnToId;
    private final Map<Integer, Collection<GlobalIndexLeafPredicator>> indexReadersCache =
            new HashMap<>();

    public GlobalIndexPredicate(
            Function<Integer, Collection<GlobalIndexLeafPredicator>> readersFunction,
            RowType rowType) {
        this.readersFunction = readersFunction;
        this.fieldColumnToId = new HashMap<>();
        for (DataField dataField : rowType.getFields()) {
            fieldColumnToId.put(dataField.name(), dataField);
        }
    }

    public GlobalIndexResult evaluate(@Nullable Predicate predicate) {
        if (predicate == null) {
            return ALL;
        }
        Set<Integer> requiredFieldIds = getRequiredFieldIds(predicate);
        requiredFieldIds.forEach(id -> indexReadersCache.computeIfAbsent(id, readersFunction));
        GlobalIndexResult result =
                new GlobalIndexPredicate.FileIndexPredicateTest(fieldColumnToId, indexReadersCache)
                        .test(predicate);
        if (result.empty()) {
            LOG.debug("One shard has been filtered");
        }
        return result;
    }

    public void close() {
        closePredicatorsQuietly(
                indexReadersCache.values().stream().flatMap(Collection::stream).iterator());
    }

    private void closePredicatorsQuietly(Iterator<GlobalIndexLeafPredicator> iterator) {
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
                                fieldColumnToId.get(predicate.fieldName()).id());
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
                                .map(f -> fieldColumnToId.get(f).id())
                                .collect(Collectors.toSet());
                    }
                });
    }

    /** Predicate test worker. */
    private static class FileIndexPredicateTest implements PredicateVisitor<GlobalIndexResult> {

        private final Map<String, DataField> fieldColumnToId;
        private final Map<Integer, Collection<GlobalIndexLeafPredicator>> columnIndexReaders;

        public FileIndexPredicateTest(
                Map<String, DataField> fieldColumnToId,
                Map<Integer, Collection<GlobalIndexLeafPredicator>> fileIndexReaders) {
            this.fieldColumnToId = fieldColumnToId;
            this.columnIndexReaders = fileIndexReaders;
        }

        public GlobalIndexResult test(Predicate predicate) {
            return predicate.visit(this);
        }

        @Override
        public GlobalIndexResult visit(LeafPredicate predicate) {
            GlobalIndexResult compoundResult = ALL;
            FieldRef fieldRef =
                    new FieldRef(predicate.index(), predicate.fieldName(), predicate.type());
            for (GlobalIndexLeafPredicator fileIndexReader :
                    columnIndexReaders.get(fieldColumnToId.get(predicate.fieldName()).id())) {
                compoundResult =
                        compoundResult.and(
                                predicate
                                        .function()
                                        .visit(fileIndexReader, fieldRef, predicate.literals()));

                if (compoundResult.empty()) {
                    return compoundResult;
                }
            }
            return compoundResult;
        }

        @Override
        public GlobalIndexResult visit(CompoundPredicate predicate) {
            if (predicate.function() instanceof Or) {
                GlobalIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.or(predicate1.visit(this));
                }
                return compoundResult == null ? ALL : compoundResult;
            } else {
                GlobalIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.and(predicate1.visit(this));
                    // if not remain, no need to test anymore
                    if (compoundResult.empty()) {
                        return compoundResult;
                    }
                }
                return compoundResult == null ? ALL : compoundResult;
            }
        }

        @Override
        public GlobalIndexResult visit(TransformPredicate predicate) {
            return ALL;
        }
    }
}
