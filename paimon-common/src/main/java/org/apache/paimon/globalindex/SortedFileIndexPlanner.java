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
import org.apache.paimon.predicate.LeafBinaryFunction;
import org.apache.paimon.predicate.LikeOptimization;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Metadata-only planning for deferred index queries, matching sorted index reader support and
 * budgets.
 *
 * <p>An absent result means the predicate is unsupported; a present empty list means no files can
 * match. Keeping this distinction before distribution preserves AND/OR semantics across splits.
 */
class SortedFileIndexPlanner extends SortedFileMetaSelector {

    private final List<GlobalIndexIOMeta> files;

    /** Byte budget for selected files in an original index group, before split-local pruning. */
    private final long fallbackScanMaxSize;

    SortedFileIndexPlanner(
            List<GlobalIndexIOMeta> files, KeySerializer keySerializer, long fallbackScanMaxSize) {
        super(files, keySerializer);
        this.files = files;
        this.fallbackScanMaxSize = fallbackScanMaxSize;
    }

    // Negative predicates use a complement in the reader; retain null-only files so their rows
    // are excluded from that complement, even when they cannot match the predicate themselves.
    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNotNull(FieldRef fieldRef) {
        return Optional.of(files);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(literal == null ? Collections.emptyList() : files);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(literals.contains(null) ? Collections.emptyList() : files);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNaN(FieldRef fieldRef) {
        return Optional.empty();
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitStartsWith(FieldRef fieldRef, Object literal) {
        return isString(fieldRef, literal)
                ? super.visitStartsWith(fieldRef, literal)
                : Optional.empty();
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEndsWith(FieldRef fieldRef, Object literal) {
        return isString(fieldRef, literal)
                ? fallback(() -> super.visitEndsWith(fieldRef, literal), literal)
                : Optional.empty();
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitContains(FieldRef fieldRef, Object literal) {
        return isString(fieldRef, literal)
                ? fallback(() -> super.visitContains(fieldRef, literal), literal)
                : Optional.empty();
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLike(FieldRef fieldRef, Object literal) {
        if (!isString(fieldRef, literal)) {
            return Optional.empty();
        }
        Optional<Pair<LeafBinaryFunction, Object>> optimized =
                LikeOptimization.tryOptimize(literal);
        if (optimized.isPresent()) {
            return optimized
                    .get()
                    .getKey()
                    .visit(this, fieldRef, Collections.singletonList(optimized.get().getValue()));
        }
        return fallback(() -> super.visitLike(fieldRef, literal), literal);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessThan(FieldRef fieldRef, Object literal) {
        return fallback(() -> super.visitLessThan(fieldRef, literal), literal);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return fallback(() -> super.visitLessOrEqual(fieldRef, literal), literal);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return fallback(() -> super.visitGreaterThan(fieldRef, literal), literal);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return fallback(() -> super.visitGreaterOrEqual(fieldRef, literal), literal);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return to == null
                ? Optional.empty()
                : fallback(() -> super.visitBetween(fieldRef, from, to), from);
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotBetween(
            FieldRef fieldRef, Object from, Object to) {
        return to == null
                ? Optional.empty()
                : fallback(
                        () ->
                                super.visitOr(
                                        Arrays.asList(
                                                super.visitLessThan(fieldRef, from),
                                                super.visitGreaterThan(fieldRef, to))),
                        from);
    }

    private boolean isString(FieldRef fieldRef, Object literal) {
        return fieldRef.type().is(DataTypeFamily.CHARACTER_STRING) && literal != null;
    }

    private Optional<List<GlobalIndexIOMeta>> fallback(
            Supplier<Optional<List<GlobalIndexIOMeta>>> selector, Object literal) {
        if (fallbackScanMaxSize <= 0 || literal == null) {
            return Optional.empty();
        }
        Optional<List<GlobalIndexIOMeta>> selected = selector.get();
        if (!selected.isPresent()) {
            return Optional.empty();
        }
        long remaining = fallbackScanMaxSize;
        for (GlobalIndexIOMeta file : selected.get()) {
            if (file.fileSize() > remaining) {
                return Optional.empty();
            }
            remaining -= file.fileSize();
        }
        return selected;
    }
}
