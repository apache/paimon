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

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Selects candidate global index files by per-index-file min/max metadata.
 *
 * <p>All files are expected to belong to the same field. The current {@code
 * RowRangeGlobalIndexScanner} can guarantee that. Please do not break this premise if you want to
 * implement your own index scanner.
 */
public class SortedFileMetaSelector implements FunctionVisitor<Optional<List<GlobalIndexIOMeta>>> {

    private final List<Pair<GlobalIndexIOMeta, SortedIndexFileMeta>> files;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;

    public SortedFileMetaSelector(List<GlobalIndexIOMeta> files, KeySerializer keySerializer) {
        this.files =
                files.stream()
                        .map(
                                meta ->
                                        Pair.of(
                                                meta,
                                                SortedIndexFileMeta.deserialize(meta.metadata())))
                        .collect(Collectors.toList());
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNotNull(FieldRef fieldRef) {
        return Optional.of(filter(meta -> !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNull(FieldRef fieldRef) {
        return Optional.of(filter(SortedIndexFileMeta::hasNulls));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitStartsWith(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }

        byte[] prefix = serialize(literal);
        if (prefix.length == 0) {
            return Optional.of(filter(meta -> !meta.onlyNulls()));
        }

        Object prefixKey = deserialize(prefix);
        byte[] upperBoundBytes = prefixUpperBound(prefix);
        Object upperBound = upperBoundBytes == null ? null : deserialize(upperBoundBytes);
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && compareLastKey(meta, prefixKey) >= 0
                                        && (upperBound == null
                                                || compareFirstKey(meta, upperBound) < 0)));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> literal != null && !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> literal != null && !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> literal != null && !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessThan(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls() && compareFirstKey(meta, literal) < 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls() && compareLastKey(meta, literal) >= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotEqual(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(
                filter(meta -> !meta.onlyNulls() && compareFirstKey(meta, literal) <= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEqual(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls() && overlaps(meta, literal, literal)));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterThan(FieldRef fieldRef, Object literal) {
        if (literal == null) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls() && compareLastKey(meta, literal) > 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(
                filter(
                        meta -> {
                            if (meta.onlyNulls()) {
                                return false;
                            }
                            for (Object literal : literals) {
                                if (literal != null && overlaps(meta, literal, literal)) {
                                    return true;
                                }
                            }
                            return false;
                        }));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        for (Object literal : literals) {
            if (literal == null) {
                return Optional.of(Collections.emptyList());
            }
        }
        return Optional.of(filter(meta -> !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        if (from == null || to == null || compareKeys(from, to) > 0) {
            return Optional.of(Collections.emptyList());
        }
        return Optional.of(filter(meta -> !meta.onlyNulls() && overlaps(meta, from, to)));
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
                break;
            }
        }
        return result == null ? Optional.empty() : Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitOr(
            List<Optional<List<GlobalIndexIOMeta>>> children) {
        HashSet<GlobalIndexIOMeta> result = new HashSet<>();
        for (Optional<List<GlobalIndexIOMeta>> child : children) {
            if (!child.isPresent()) {
                return Optional.empty();
            }
            result.addAll(child.get());
        }
        return Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNonFieldLeaf(LeafPredicate predicate) {
        return Optional.empty();
    }

    protected boolean overlaps(SortedIndexFileMeta meta, Object from, Object to) {
        return comparator.compare(from, deserialize(meta.lastKey())) <= 0
                && comparator.compare(to, deserialize(meta.firstKey())) >= 0;
    }

    protected int compareFirstKey(SortedIndexFileMeta meta, Object literal) {
        return comparator.compare(deserialize(meta.firstKey()), literal);
    }

    protected int compareLastKey(SortedIndexFileMeta meta, Object literal) {
        return comparator.compare(deserialize(meta.lastKey()), literal);
    }

    protected int compareKeys(Object left, Object right) {
        return comparator.compare(left, right);
    }

    protected byte[] serialize(Object key) {
        return keySerializer.serialize(key);
    }

    protected Object deserialize(byte[] valueBytes) {
        return keySerializer.deserialize(MemorySlice.wrap(valueBytes));
    }

    protected List<GlobalIndexIOMeta> filter(Predicate<SortedIndexFileMeta> predicate) {
        return files.stream()
                .filter(pair -> predicate.test(pair.getRight()))
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }

    protected static byte[] prefixUpperBound(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            int unsignedByte = prefix[i] & 0xFF;
            if (unsignedByte != 0xFF) {
                byte[] upperBound = new byte[i + 1];
                System.arraycopy(prefix, 0, upperBound, 0, i + 1);
                upperBound[i] = (byte) (unsignedByte + 1);
                return upperBound;
            }
        }
        return null;
    }
}
