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
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Or;
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
import java.util.TreeSet;

/** Discrete equality/IN/NULL prefixes followed by an optional range in composite-key order. */
public final class CompositeBTreePredicate {

    public static final int MAX_INTERVALS = 256;

    private CompositeBTreePredicate() {}

    public static Optional<Plan> plan(List<DataField> fields, Predicate predicate) {
        List<Predicate> conjuncts = PredicateBuilder.splitAnd(predicate);
        List<Predicate> covered = new ArrayList<>();
        List<Object[]> prefixes = new ArrayList<>();
        prefixes.add(new Object[0]);
        List<LeafPredicate> range = Collections.emptyList();
        int equalColumns = 0;
        for (DataField field : fields) {
            List<LeafPredicate> column = new ArrayList<>();
            List<Predicate> originals = new ArrayList<>();
            for (Predicate child : conjuncts) {
                LeafPredicate leaf = asLeaf(child);
                if (leaf != null
                        && leaf.fieldRefOptional().isPresent()
                        && field.name().equals(leaf.fieldRefOptional().get().name())
                        && field.type().equalsIgnoreNullable(leaf.type())
                        && (isPoint(leaf) || isRange(leaf))) {
                    column.add(leaf);
                    originals.add(child);
                }
            }
            Optional<List<Object>> domain = pointValues(field, column);
            covered.addAll(originals);
            if (!domain.isPresent()) {
                range = column;
                break;
            }
            equalColumns++;
            if (domain.get().isEmpty()) {
                return Optional.of(
                        new Plan(
                                fields,
                                covered,
                                Collections.emptyList(),
                                equalColumns,
                                equalColumns));
            }
            if ((long) prefixes.size() * domain.get().size() > MAX_INTERVALS) {
                return Optional.empty();
            }
            List<Object[]> expanded = new ArrayList<>();
            for (Object[] prefix : prefixes) {
                for (Object value : domain.get()) {
                    expanded.add(append(prefix, value));
                }
            }
            prefixes = expanded;
        }
        int boundColumns = equalColumns + (range.isEmpty() ? 0 : 1);
        if (boundColumns == 0) {
            return Optional.empty();
        }
        List<Interval> intervals = new ArrayList<>();
        for (Object[] prefix : prefixes) {
            Bound lower = new Bound(fields, prefix, false);
            Bound upper = new Bound(fields, prefix, true);
            boolean empty = false;
            if (!range.isEmpty()) {
                // Comparisons and IS NOT NULL exclude the entire NULL-component suffix.
                lower = new Bound(fields, append(prefix, null), true);
                for (LeafPredicate leaf : range) {
                    if (leaf.function() instanceof IsNotNull) {
                        continue;
                    }
                    if (leaf.literals().contains(null)) {
                        empty = true;
                        break;
                    }
                    if (leaf.function() instanceof GreaterThan
                            || leaf.function() instanceof GreaterOrEqual
                            || leaf.function() instanceof Between) {
                        Bound next =
                                new Bound(
                                        fields,
                                        append(prefix, leaf.literals().get(0)),
                                        leaf.function() instanceof GreaterThan);
                        if (next.compareTo(lower) > 0) {
                            lower = next;
                        }
                    }
                    if (leaf.function() instanceof LessThan
                            || leaf.function() instanceof LessOrEqual
                            || leaf.function() instanceof Between) {
                        Object value =
                                leaf.literals().get(leaf.function() instanceof Between ? 1 : 0);
                        Bound next =
                                new Bound(
                                        fields,
                                        append(prefix, value),
                                        !(leaf.function() instanceof LessThan));
                        if (next.compareTo(upper) < 0) {
                            upper = next;
                        }
                    }
                }
            }
            if (!empty && lower.compareTo(upper) < 0) {
                intervals.add(new Interval(lower, upper));
            }
        }
        return Optional.of(new Plan(fields, covered, intervals, boundColumns, equalColumns));
    }

    private static Optional<List<Object>> pointValues(DataField field, List<LeafPredicate> column) {
        LeafPredicate selected = null;
        for (LeafPredicate leaf : column) {
            if (isPoint(leaf)
                    && (selected == null
                            || !(leaf.function() instanceof In)
                            || (selected.function() instanceof In
                                    && leaf.literals().size() < selected.literals().size()))) {
                selected = leaf;
            }
        }
        if (selected == null) {
            return Optional.empty();
        }
        List<Object> candidates =
                selected.function() instanceof IsNull
                        ? Collections.singletonList(null)
                        : selected.literals();
        Comparator<Object> comparator = KeySerializer.create(field.type()).createComparator();
        TreeSet<Object> values =
                new TreeSet<>(
                        (left, right) ->
                                left == null
                                        ? (right == null ? 0 : -1)
                                        : right == null ? 1 : comparator.compare(left, right));
        for (Object value : candidates) {
            if (value == null
                    && (!(selected.function() instanceof IsNull) || !field.type().isNullable())) {
                continue;
            }
            boolean matches = true;
            for (LeafPredicate leaf : column) {
                if (!leaf.function().test(leaf.type(), value, leaf.literals())) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                values.add(value);
                if (values.size() > MAX_INTERVALS) {
                    break;
                }
            }
        }
        return Optional.of(new ArrayList<>(values));
    }

    // PredicateBuilder represents small IN lists as ORs of equalities.
    private static LeafPredicate asLeaf(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            return (LeafPredicate) predicate;
        }
        if (!(predicate instanceof CompoundPredicate)
                || !(((CompoundPredicate) predicate).function() instanceof Or)) {
            return null;
        }
        LeafPredicate first = null;
        List<Object> literals = new ArrayList<>();
        for (Predicate child : PredicateBuilder.splitOr(predicate)) {
            if (!(child instanceof LeafPredicate)) {
                return null;
            }
            LeafPredicate leaf = (LeafPredicate) child;
            if (!(leaf.function() instanceof Equal)
                    || !leaf.fieldRefOptional().isPresent()
                    || (first != null
                            && !first.fieldRefOptional().equals(leaf.fieldRefOptional()))) {
                return null;
            }
            first = leaf;
            literals.add(leaf.literals().get(0));
        }
        return first == null
                ? null
                : new LeafPredicate(
                        In.INSTANCE,
                        first.type(),
                        first.fieldRefOptional().get().index(),
                        first.fieldRefOptional().get().name(),
                        literals);
    }

    private static boolean isPoint(LeafPredicate leaf) {
        return leaf.function() instanceof Equal
                || leaf.function() instanceof In
                || leaf.function() instanceof IsNull;
    }

    private static boolean isRange(LeafPredicate leaf) {
        return leaf.function() instanceof GreaterThan
                || leaf.function() instanceof GreaterOrEqual
                || leaf.function() instanceof LessThan
                || leaf.function() instanceof LessOrEqual
                || leaf.function() instanceof Between
                || leaf.function() instanceof IsNotNull;
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

    /** One disjoint tuple interval, in physical key order. */
    public static final class Interval {
        private final Bound lower;
        private final Bound upper;

        private Interval(Bound lower, Bound upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public Bound lower() {
            return lower;
        }

        public Bound upper() {
            return upper;
        }

        public GenericRow pointKey() {
            return GenericRow.of(lower.values);
        }
    }

    /** Tuple intervals and their covered predicates, rebuilt from the original query. */
    public static final class Plan {
        private final List<DataField> fields;
        private final List<Predicate> predicates;
        private final List<Interval> intervals;
        private final int boundColumns;
        private final int equalColumns;

        private Plan(
                List<DataField> fields,
                List<Predicate> predicates,
                List<Interval> intervals,
                int boundColumns,
                int equalColumns) {
            this.fields = fields;
            this.predicates = predicates;
            this.intervals = intervals;
            this.boundColumns = boundColumns;
            this.equalColumns = equalColumns;
        }

        public List<Predicate> predicates() {
            return predicates;
        }

        public List<Interval> intervals() {
            return intervals;
        }

        public int boundColumns() {
            return boundColumns;
        }

        public int equalColumns() {
            return equalColumns;
        }

        public boolean isEmpty() {
            return intervals.isEmpty();
        }

        public boolean isPointLookup() {
            return equalColumns == fields.size();
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
            if (isEmpty()) {
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
                if (meta.firstKey() == null || meta.lastKey() == null) {
                    result.add(file);
                    continue;
                }
                InternalRow first =
                        (InternalRow) serializer.deserialize(MemorySlice.wrap(meta.firstKey()));
                InternalRow last =
                        (InternalRow) serializer.deserialize(MemorySlice.wrap(meta.lastKey()));
                if (intervals.stream()
                        .anyMatch(
                                interval ->
                                        interval.lower.compareKey(last) > 0
                                                && interval.upper.compareKey(first) < 0)) {
                    result.add(file);
                }
            }
            return result;
        }
    }
}
