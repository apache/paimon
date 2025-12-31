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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.TransformPredicate;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@link FunctionVisitor} to select candidate btree index files. All files are expected to
 * belong to the same field. The current {@code RowRangeGlobalIndexScanner} can guarantee that.
 * Please do not break this premise if you want to implement your own index scanner.
 */
public class BTreeFileMetaSelector implements FunctionVisitor<Optional<List<GlobalIndexIOMeta>>> {

    private final List<Pair<GlobalIndexIOMeta, BTreeIndexMeta>> files;
    private final Comparator<Object> comparator;
    private final KeySerializer keySerializer;

    public BTreeFileMetaSelector(List<GlobalIndexIOMeta> files, KeySerializer keySerializer) {
        this.files =
                files.stream()
                        .map(meta -> Pair.of(meta, BTreeIndexMeta.deserialize(meta.metadata())))
                        .collect(Collectors.toList());
        this.comparator = keySerializer.createComparator();
        this.keySerializer = keySerializer;
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNotNull(FieldRef fieldRef) {
        return Optional.of(filter(meta -> !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNull(FieldRef fieldRef) {
        return Optional.of(filter(BTreeIndexMeta::hasNulls));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessThan(FieldRef fieldRef, Object literal) {
        // `<` means file.minKey < literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getFirstKey()), literal)
                                                < 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        // `>=` means file.maxKey >= literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getLastKey()), literal)
                                                >= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        // `<=` means file.minKey <= literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getFirstKey()), literal)
                                                <= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                filter(
                        meta -> {
                            if (meta.onlyNulls()) {
                                return false;
                            }
                            Object minKey = deserialize(meta.getFirstKey());
                            Object maxKey = deserialize(meta.getLastKey());
                            return comparator.compare(literal, minKey) >= 0
                                    && comparator.compare(literal, maxKey) <= 0;
                        }));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterThan(FieldRef fieldRef, Object literal) {
        // `>` means file.maxKey > literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getLastKey()), literal)
                                                > 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(
                filter(
                        meta -> {
                            if (meta.onlyNulls()) {
                                return false;
                            }
                            Object minKey = deserialize(meta.getFirstKey());
                            Object maxKey = deserialize(meta.getLastKey());
                            for (Object literal : literals) {
                                if (comparator.compare(literal, minKey) >= 0
                                        && comparator.compare(literal, maxKey) <= 0) {
                                    return true;
                                }
                            }
                            return false;
                        }));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        // we can't filter any file meta by NOT IN condition
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitAnd(
            List<Optional<List<GlobalIndexIOMeta>>> children) {
        HashSet<GlobalIndexIOMeta> result = null;
        for (Optional<List<GlobalIndexIOMeta>> child : children) {
            if (!child.isPresent()) {
                return Optional.empty();
            }
            if (result == null) {
                result = new HashSet<>(child.get());
            } else {
                result.retainAll(child.get());
            }
            if (result.isEmpty()) {
                return Optional.empty();
            }
        }
        return result == null ? Optional.empty() : Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitOr(
            List<Optional<List<GlobalIndexIOMeta>>> children) {
        HashSet<GlobalIndexIOMeta> result = new HashSet<>();
        for (Optional<List<GlobalIndexIOMeta>> child : children) {
            child.ifPresent(result::addAll);
        }
        return result.isEmpty() ? Optional.empty() : Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visit(TransformPredicate predicate) {
        return Optional.empty();
    }

    private Object deserialize(byte[] valueBytes) {
        return keySerializer.deserialize(MemorySlice.wrap(valueBytes));
    }

    private List<GlobalIndexIOMeta> filter(Predicate<BTreeIndexMeta> predicate) {
        return files.stream()
                .filter(pair -> predicate.test(pair.getRight()))
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }
}
