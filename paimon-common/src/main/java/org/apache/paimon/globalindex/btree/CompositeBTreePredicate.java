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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/** An equality prefix followed by an optional range in physical composite-key order. */
public final class CompositeBTreePredicate {

    private CompositeBTreePredicate() {}

    public static Optional<Plan> plan(List<DataField> fields, Predicate predicate) {
        List<Predicate> conjuncts = PredicateBuilder.splitAnd(predicate);
        List<Predicate> covered = new ArrayList<>();
        List<Object> prefix = new ArrayList<>();
        List<LeafPredicate> range = Collections.emptyList();
        boolean empty = false;
        for (DataField field : fields) {
            List<LeafPredicate> column = new ArrayList<>();
            LeafPredicate equality = null;
            for (Predicate child : conjuncts) {
                if (!(child instanceof LeafPredicate)) {
                    continue;
                }
                LeafPredicate leaf = (LeafPredicate) child;
                if (!leaf.fieldRefOptional().isPresent()
                        || !field.name().equals(leaf.fieldRefOptional().get().name())
                        || !field.type().equalsIgnoreNullable(leaf.type())
                        || (!(leaf.function() instanceof Equal) && !isRange(leaf))) {
                    continue;
                }
                column.add(leaf);
                if (leaf.function() instanceof Equal) {
                    equality = leaf;
                }
            }
            if (equality == null) {
                range = column;
                covered.addAll(range);
                break;
            }
            Object value = equality.literals().get(0);
            for (LeafPredicate leaf : column) {
                // Validate all bounds before using a full-key point lookup or a prefix.
                empty |=
                        value == null || !leaf.function().test(leaf.type(), value, leaf.literals());
            }
            prefix.add(value);
            covered.addAll(column);
        }
        int equalColumns = prefix.size();
        int boundColumns = equalColumns + (range.isEmpty() ? 0 : 1);
        if (boundColumns == 0) {
            return Optional.empty();
        }
        Object[] values = prefix.toArray();
        Bound lower = new Bound(fields, values, false);
        Bound upper = new Bound(fields, values, true);
        if (!range.isEmpty()) {
            // Comparisons cannot match a NULL component, even with an unbounded lower endpoint.
            lower = new Bound(fields, append(values, null), true);
            for (LeafPredicate leaf : range) {
                if (leaf.literals().contains(null)) {
                    empty = true;
                    continue;
                }
                if (leaf.function() instanceof GreaterThan
                        || leaf.function() instanceof GreaterOrEqual
                        || leaf.function() instanceof Between) {
                    Bound next =
                            new Bound(
                                    fields,
                                    append(values, leaf.literals().get(0)),
                                    leaf.function() instanceof GreaterThan);
                    if (next.compareTo(lower) > 0) {
                        lower = next;
                    }
                }
                if (leaf.function() instanceof LessThan
                        || leaf.function() instanceof LessOrEqual
                        || leaf.function() instanceof Between) {
                    Object value = leaf.literals().get(leaf.function() instanceof Between ? 1 : 0);
                    Bound next =
                            new Bound(
                                    fields,
                                    append(values, value),
                                    !(leaf.function() instanceof LessThan));
                    if (next.compareTo(upper) < 0) {
                        upper = next;
                    }
                }
            }
        }
        return Optional.of(
                new Plan(
                        fields,
                        covered,
                        lower,
                        upper,
                        boundColumns,
                        equalColumns,
                        empty || lower.compareTo(upper) >= 0));
    }

    private static boolean isRange(LeafPredicate leaf) {
        return leaf.function() instanceof GreaterThan
                || leaf.function() instanceof GreaterOrEqual
                || leaf.function() instanceof LessThan
                || leaf.function() instanceof LessOrEqual
                || leaf.function() instanceof Between;
    }

    private static Object[] append(Object[] prefix, Object value) {
        Object[] result = Arrays.copyOf(prefix, prefix.length + 1);
        result[prefix.length] = value;
        return result;
    }

    /** A virtual boundary immediately before or after every key with the specified prefix. */
    public static final class Bound {
        private final Object[] values;
        private final boolean after;
        private final InternalRow.FieldGetter[] getters;
        private final Comparator<Object>[] comparators;

        @SuppressWarnings("unchecked")
        private Bound(List<DataField> fields, Object[] values, boolean after) {
            this.values = values;
            this.after = after;
            getters = new InternalRow.FieldGetter[values.length];
            comparators = new Comparator[values.length];
            for (int i = 0; i < values.length; i++) {
                getters[i] = InternalRow.createFieldGetter(fields.get(i).type().copy(true), i);
                comparators[i] = KeySerializer.create(fields.get(i).type()).createComparator();
            }
        }

        /** Compare a persisted key to this virtual endpoint. */
        public int compareKey(InternalRow key) {
            for (int i = 0; i < values.length; i++) {
                int comparison = compare(i, getters[i].getFieldOrNull(key), values[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return after ? -1 : 1;
        }

        private int compare(int position, Object left, Object right) {
            return left == null
                    ? (right == null ? 0 : -1)
                    : right == null ? 1 : comparators[position].compare(left, right);
        }

        private int compareTo(Bound other) {
            int count = Math.min(values.length, other.values.length);
            for (int i = 0; i < count; i++) {
                int comparison = compare(i, values[i], other.values[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            if (values.length != other.values.length) {
                return values.length < other.values.length
                        ? (after ? 1 : -1)
                        : (other.after ? -1 : 1);
            }
            return Boolean.compare(after, other.after);
        }
    }

    /** One tuple interval, with its covered query predicates and point-lookup classification. */
    public static final class Plan {
        private final List<DataField> fields;
        private final List<Predicate> predicates;
        private final Bound lower;
        private final Bound upper;
        private final int boundColumns;
        private final int equalColumns;
        private final boolean empty;

        private Plan(
                List<DataField> fields,
                List<Predicate> predicates,
                Bound lower,
                Bound upper,
                int boundColumns,
                int equalColumns,
                boolean empty) {
            this.fields = fields;
            this.predicates = predicates;
            this.lower = lower;
            this.upper = upper;
            this.boundColumns = boundColumns;
            this.equalColumns = equalColumns;
            this.empty = empty;
        }

        public List<Predicate> predicates() {
            return predicates;
        }

        public Bound lower() {
            return lower;
        }

        public Bound upper() {
            return upper;
        }

        public int boundColumns() {
            return boundColumns;
        }

        public int equalColumns() {
            return equalColumns;
        }

        public boolean isEmpty() {
            return empty;
        }

        public boolean isPointLookup() {
            return equalColumns == fields.size();
        }

        public GenericRow pointKey() {
            return GenericRow.of(lower.values);
        }

        /**
         * Point probes are unrestricted; other scans use the existing selected-file byte budget.
         */
        public boolean canScan(List<GlobalIndexIOMeta> selected, long budget) {
            if (isPointLookup() || selected.isEmpty()) {
                return true;
            }
            if (budget <= 0) {
                return false;
            }
            for (GlobalIndexIOMeta file : selected) {
                if (file.fileSize() > budget) {
                    return false;
                }
                budget -= file.fileSize();
            }
            return true;
        }

        public List<GlobalIndexIOMeta> selectFiles(List<GlobalIndexIOMeta> files) {
            if (empty) {
                return Collections.emptyList();
            }
            // Preserve conservative pruning when metadata is unavailable.
            if (files.stream().anyMatch(file -> file.metadata() == null)) {
                return files;
            }
            CompositeKeySerializer serializer = new CompositeKeySerializer(new RowType(fields));
            List<GlobalIndexIOMeta> result = new ArrayList<>();
            for (GlobalIndexIOMeta file : files) {
                SortedIndexFileMeta meta = SortedIndexFileMeta.deserialize(file.metadata());
                if (meta.firstKey() == null
                        || meta.lastKey() == null
                        || (lower.compareKey(
                                                (InternalRow)
                                                        serializer.deserialize(
                                                                MemorySlice.wrap(meta.lastKey())))
                                        > 0
                                && upper.compareKey(
                                                (InternalRow)
                                                        serializer.deserialize(
                                                                MemorySlice.wrap(meta.firstKey())))
                                        < 0)) {
                    result.add(file);
                }
            }
            return result;
        }
    }
}
