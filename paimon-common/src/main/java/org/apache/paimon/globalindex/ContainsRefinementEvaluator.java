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
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;

/** Evaluates candidate-aware refinement for conjunctive contains predicates. */
final class ContainsRefinementEvaluator {

    private final RowType rowType;
    private final IntFunction<Collection<GlobalIndexReader>> readersFunction;
    private final Function<Predicate, CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>>>
            predicateEvaluator;
    private final BiFunction<
                    List<Optional<GlobalIndexEvaluator.Evaluation>>,
                    CompoundPredicate,
                    Optional<GlobalIndexEvaluator.Evaluation>>
            resultCombiner;

    ContainsRefinementEvaluator(
            RowType rowType,
            IntFunction<Collection<GlobalIndexReader>> readersFunction,
            Function<Predicate, CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>>>
                    predicateEvaluator,
            BiFunction<
                            List<Optional<GlobalIndexEvaluator.Evaluation>>,
                            CompoundPredicate,
                            Optional<GlobalIndexEvaluator.Evaluation>>
                    resultCombiner) {
        this.rowType = rowType;
        this.readersFunction = readersFunction;
        this.predicateEvaluator = predicateEvaluator;
        this.resultCombiner = resultCombiner;
    }

    @Nullable
    CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>> evaluate(
            List<Predicate> children, CompoundPredicate predicate) {
        if (predicate.function() instanceof Or) {
            return null;
        }

        Map<Integer, ContainsGroup> groups = new LinkedHashMap<>();
        List<Predicate> remaining = new ArrayList<>();
        int containsCount = 0;
        for (Predicate child : children) {
            ContainsLeaf contains = containsLeaf(child);
            if (contains == null) {
                remaining.add(child);
                continue;
            }
            containsCount++;
            ContainsGroup group = groups.get(contains.fieldId);
            if (group == null) {
                group = new ContainsGroup(contains.fieldId, contains.fieldRef, contains.readers);
                groups.put(contains.fieldId, group);
            }
            group.literals.add(contains.literal);
        }
        if (containsCount == 0 || children.size() == 1) {
            return null;
        }

        List<CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>>> remainingFutures =
                new ArrayList<>(remaining.size());
        for (Predicate child : remaining) {
            remainingFutures.add(predicateEvaluator.apply(child));
        }
        // A completed index task may still own a SemaphoredDelegatingExecutor permit while its
        // completion callbacks run. Cross the phase boundary asynchronously before submitting the
        // next index tasks so a single-permit executor cannot deadlock on nested submission.
        return CompletableFuture.allOf(remainingFutures.toArray(new CompletableFuture[0]))
                .thenComposeAsync(
                        ignored -> {
                            List<Optional<GlobalIndexEvaluator.Evaluation>> remainingResults =
                                    new ArrayList<>();
                            for (CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>>
                                    future : remainingFutures) {
                                remainingResults.add(future.join());
                            }
                            Optional<GlobalIndexEvaluator.Evaluation> remainingEvaluation =
                                    resultCombiner.apply(remainingResults, predicate);
                            GlobalIndexResult candidates =
                                    remainingEvaluation.isPresent()
                                            ? remainingEvaluation.get().result()
                                            : null;
                            Set<Integer> emptyResultFields = new HashSet<>();
                            if (remainingEvaluation.isPresent()) {
                                emptyResultFields.addAll(
                                        remainingEvaluation.get().contributingFieldIds());
                                if (candidates.results().isEmpty()) {
                                    return CompletableFuture.completedFuture(
                                            Optional.of(
                                                    new GlobalIndexEvaluator.Evaluation(
                                                            GlobalIndexResult.createEmpty(),
                                                            emptyResultFields)));
                                }
                            }
                            List<ContainsGroup> groupedContains = new ArrayList<>(groups.values());
                            List<CompletableFuture<Optional<GlobalIndexResult>>> coarseFutures =
                                    new ArrayList<>(groupedContains.size());
                            for (ContainsGroup group : groupedContains) {
                                coarseFutures.add(visitContainsCandidates(group, candidates));
                            }
                            GlobalIndexResult initialCandidates = candidates;
                            // Keep the coarse-to-exact boundary outside the task which completes
                            // the last coarse future for the same single-permit executor case.
                            return CompletableFuture.allOf(
                                            coarseFutures.toArray(new CompletableFuture[0]))
                                    .thenComposeAsync(
                                            coarseIgnored -> {
                                                GlobalIndexResult refinedCandidates =
                                                        initialCandidates;
                                                for (int i = 0; i < coarseFutures.size(); i++) {
                                                    Optional<GlobalIndexResult> coarse =
                                                            coarseFutures.get(i).join();
                                                    if (!coarse.isPresent()) {
                                                        continue;
                                                    }
                                                    refinedCandidates =
                                                            refinedCandidates == null
                                                                    ? coarse.get()
                                                                    : refinedCandidates.and(
                                                                            coarse.get());
                                                    emptyResultFields.add(
                                                            groupedContains.get(i).fieldId);
                                                    if (refinedCandidates.results().isEmpty()) {
                                                        return CompletableFuture.completedFuture(
                                                                Optional.of(
                                                                        new GlobalIndexEvaluator
                                                                                .Evaluation(
                                                                                GlobalIndexResult
                                                                                        .createEmpty(),
                                                                                emptyResultFields)));
                                                    }
                                                }

                                                List<
                                                                CompletableFuture<
                                                                        Optional<
                                                                                GlobalIndexEvaluator
                                                                                        .Evaluation>>>
                                                        exactFutures =
                                                                new ArrayList<>(groups.size());
                                                for (ContainsGroup group : groups.values()) {
                                                    exactFutures.add(
                                                            visitContainsConjunction(
                                                                    group, refinedCandidates));
                                                }
                                                return CompletableFuture.allOf(
                                                                exactFutures.toArray(
                                                                        new CompletableFuture[0]))
                                                        .thenApply(
                                                                exactIgnored -> {
                                                                    List<
                                                                                    Optional<
                                                                                            GlobalIndexEvaluator
                                                                                                    .Evaluation>>
                                                                            results =
                                                                                    new ArrayList<>(
                                                                                            exactFutures
                                                                                                            .size()
                                                                                                    + 1);
                                                                    if (remainingEvaluation
                                                                            .isPresent()) {
                                                                        results.add(
                                                                                remainingEvaluation);
                                                                    }
                                                                    for (CompletableFuture<
                                                                                    Optional<
                                                                                            GlobalIndexEvaluator
                                                                                                    .Evaluation>>
                                                                            future : exactFutures) {
                                                                        results.add(future.join());
                                                                    }
                                                                    return resultCombiner.apply(
                                                                            results, predicate);
                                                                });
                                            });
                        });
    }

    @Nullable
    private ContainsLeaf containsLeaf(Predicate predicate) {
        if (!(predicate instanceof LeafPredicate)) {
            return null;
        }
        LeafPredicate leaf = (LeafPredicate) predicate;
        if (!(leaf.function() instanceof Contains) || !leaf.fieldRefOptional().isPresent()) {
            return null;
        }
        FieldRef fieldRef = leaf.fieldRefOptional().get();
        int fieldId = rowType.getField(fieldRef.name()).id();
        Collection<GlobalIndexReader> readers = readersFunction.apply(fieldId);
        if (readers.isEmpty()) {
            return null;
        }
        for (GlobalIndexReader reader : readers) {
            if (!(reader instanceof ContainsRefiningGlobalIndexReader)) {
                return null;
            }
        }
        return new ContainsLeaf(fieldId, fieldRef, leaf.literals().get(0), readers);
    }

    private CompletableFuture<Optional<GlobalIndexResult>> visitContainsCandidates(
            ContainsGroup group, @Nullable GlobalIndexResult candidates) {
        List<CompletableFuture<Optional<GlobalIndexResult>>> futures =
                new ArrayList<>(group.readers.size());
        for (GlobalIndexReader reader : group.readers) {
            futures.add(
                    ((ContainsRefiningGlobalIndexReader) reader)
                            .visitContainsCandidates(group.fieldRef, group.literals, candidates));
        }
        return intersectReaderResults(futures);
    }

    private CompletableFuture<Optional<GlobalIndexEvaluator.Evaluation>> visitContainsConjunction(
            ContainsGroup group, @Nullable GlobalIndexResult candidates) {
        List<CompletableFuture<Optional<GlobalIndexResult>>> futures =
                new ArrayList<>(group.readers.size());
        for (GlobalIndexReader reader : group.readers) {
            futures.add(
                    ((ContainsRefiningGlobalIndexReader) reader)
                            .visitContainsConjunction(group.fieldRef, group.literals, candidates));
        }
        return intersectReaderResults(futures)
                .thenApply(
                        result ->
                                result.map(
                                        value ->
                                                new GlobalIndexEvaluator.Evaluation(
                                                        value,
                                                        Collections.singleton(group.fieldId))));
    }

    private CompletableFuture<Optional<GlobalIndexResult>> intersectReaderResults(
            List<CompletableFuture<Optional<GlobalIndexResult>>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            Optional<GlobalIndexResult> result = Optional.empty();
                            for (CompletableFuture<Optional<GlobalIndexResult>> future : futures) {
                                Optional<GlobalIndexResult> current = future.join();
                                if (!current.isPresent()) {
                                    continue;
                                }
                                result =
                                        Optional.of(
                                                result.isPresent()
                                                        ? result.get().and(current.get())
                                                        : current.get());
                                if (result.get().results().isEmpty()) {
                                    break;
                                }
                            }
                            return result;
                        });
    }

    private static final class ContainsLeaf {

        private final int fieldId;
        private final FieldRef fieldRef;
        private final Object literal;
        private final Collection<GlobalIndexReader> readers;

        private ContainsLeaf(
                int fieldId,
                FieldRef fieldRef,
                Object literal,
                Collection<GlobalIndexReader> readers) {
            this.fieldId = fieldId;
            this.fieldRef = fieldRef;
            this.literal = literal;
            this.readers = readers;
        }
    }

    private static final class ContainsGroup {

        private final int fieldId;
        private final FieldRef fieldRef;
        private final Collection<GlobalIndexReader> readers;
        private final List<Object> literals = new ArrayList<>();

        private ContainsGroup(
                int fieldId, FieldRef fieldRef, Collection<GlobalIndexReader> readers) {
            this.fieldId = fieldId;
            this.fieldRef = fieldRef;
            this.readers = readers;
        }
    }
}
