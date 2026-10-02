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
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.NotBetween;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.NotLike;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Equality/IN prefixes followed by one range, with remaining key predicates checked before
 * postings.
 */
public final class CompositeBTreePredicate {

    public static final int MAX_INTERVALS = 256;

    private CompositeBTreePredicate() {}

    public static Optional<Plan> plan(List<DataField> fields, Predicate predicate) {
        List<Predicate> originals = new ArrayList<>();
        List<LeafPredicate> filters = new ArrayList<>();
        for (Predicate child : PredicateBuilder.splitAnd(predicate)) {
            LeafPredicate leaf = asLeaf(child);
            if (leaf == null
                    || !supported(leaf.function())
                    || !leaf.fieldRefOptional().isPresent()) {
                continue;
            }
            for (int i = 0; i < fields.size(); i++) {
                DataField field = fields.get(i);
                if (field.name().equals(leaf.fieldRefOptional().get().name())
                        && field.type().equalsIgnoreNullable(leaf.type())) {
                    originals.add(child);
                    filters.add(
                            new LeafPredicate(
                                    leaf.function(),
                                    field.type().copy(true),
                                    i,
                                    field.name(),
                                    leaf.literals()));
                    break;
                }
            }
        }
        List<Object[]> prefixes = new ArrayList<>();
        prefixes.add(new Object[0]);
        int equalColumns = 0;
        for (; equalColumns < fields.size(); equalColumns++) {
            final int position = equalColumns;
            List<LeafPredicate> column =
                    filters.stream()
                            .filter(leaf -> leaf.fieldRefOptional().get().index() == position)
                            .collect(Collectors.toList());
            Optional<List<Object>> domain = pointValues(fields.get(position), column);
            if (!domain.isPresent()) {
                break;
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
        List<Interval> intervals = new ArrayList<>();
        int boundColumns = equalColumns;
        for (Object[] prefix : prefixes) {
            Bound lower = new Bound(fields, prefix, false);
            Bound upper = new Bound(fields, prefix, true);
            if (equalColumns < fields.size()) {
                for (LeafPredicate leaf : filters) {
                    if (leaf.fieldRefOptional().get().index() != equalColumns) {
                        continue;
                    }
                    LeafFunction function = leaf.function();
                    if (function instanceof GreaterThan
                            || function instanceof GreaterOrEqual
                            || function instanceof LessThan
                            || function instanceof LessOrEqual
                            || function instanceof Between
                            || function instanceof IsNotNull) {
                        boundColumns = equalColumns + 1;
                        if (!(function instanceof IsNotNull) && leaf.literals().contains(null)) {
                            lower = null;
                            break;
                        }
                        if (function instanceof GreaterThan
                                || function instanceof GreaterOrEqual
                                || function instanceof Between
                                || function instanceof IsNotNull) {
                            Object value =
                                    function instanceof IsNotNull ? null : leaf.literals().get(0);
                            Bound next =
                                    new Bound(
                                            fields,
                                            append(prefix, value),
                                            function instanceof GreaterThan
                                                    || function instanceof IsNotNull);
                            if (next.compareTo(lower) > 0) {
                                lower = next;
                            }
                        }
                        if (function instanceof LessThan
                                || function instanceof LessOrEqual
                                || function instanceof Between) {
                            Object value = leaf.literals().get(function instanceof Between ? 1 : 0);
                            Bound next =
                                    new Bound(
                                            fields,
                                            append(prefix, value),
                                            !(function instanceof LessThan));
                            if (next.compareTo(upper) < 0) {
                                upper = next;
                            }
                        }
                    }
                }
            }
            if (lower != null && lower.compareTo(upper) < 0) {
                intervals.add(new Interval(lower, upper));
            }
        }
        if (boundColumns == 0) {
            return Optional.empty();
        }
        return Optional.of(
                new Plan(
                        fields,
                        originals,
                        filters,
                        intervals,
                        boundColumns,
                        equalColumns == fields.size()));
    }

    private static Optional<List<Object>> pointValues(
            DataField field, List<LeafPredicate> filters) {
        for (LeafPredicate leaf : filters) {
            LeafFunction function = leaf.function();
            if (!(function instanceof Equal)
                    && !(function instanceof In)
                    && !(function instanceof IsNull)) {
                continue;
            }
            List<Object> values =
                    function instanceof IsNull ? Collections.singletonList(null) : leaf.literals();
            Comparator<Object> comparator = KeySerializer.create(field.type()).createComparator();
            TreeSet<Object> unique =
                    new TreeSet<>(
                            (a, b) ->
                                    a == null
                                            ? (b == null ? 0 : -1)
                                            : b == null ? 1 : comparator.compare(a, b));
            for (Object value : values) {
                if ((value == null && !(function instanceof IsNull))
                        || (value == null && !field.type().isNullable())) {
                    continue;
                }
                if (filters.stream()
                        .allMatch(
                                filter ->
                                        filter.function()
                                                .test(filter.type(), value, filter.literals()))) {
                    unique.add(value);
                    if (unique.size() > MAX_INTERVALS) {
                        return Optional.of(new ArrayList<>(unique));
                    }
                }
            }
            return Optional.of(new ArrayList<>(unique));
        }
        return Optional.empty();
    }

    // PredicateBuilder represents small IN lists as ORs of equalities.
    private static LeafPredicate asLeaf(Predicate predicate) {
        if (predicate instanceof LeafPredicate) {
            return (LeafPredicate) predicate;
        }
        CompoundPredicate compound = (CompoundPredicate) predicate;
        if (!(compound.function() instanceof Or)) {
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

    private static boolean supported(LeafFunction function) {
        return function instanceof Equal
                || function instanceof In
                || function instanceof IsNull
                || function instanceof IsNotNull
                || function instanceof GreaterThan
                || function instanceof GreaterOrEqual
                || function instanceof LessThan
                || function instanceof LessOrEqual
                || function instanceof Between
                || function instanceof NotEqual
                || function instanceof NotIn
                || function instanceof NotBetween
                || function instanceof StartsWith
                || function instanceof EndsWith
                || function instanceof Contains
                || function instanceof Like
                || function instanceof NotLike;
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
            this.getters = new InternalRow.FieldGetter[values.length];
            this.comparators = new Comparator[values.length];
            for (int i = 0; i < values.length; i++) {
                getters[i] = InternalRow.createFieldGetter(fields.get(i).type().copy(true), i);
                comparators[i] = KeySerializer.create(fields.get(i).type()).createComparator();
            }
        }

        public int compareKey(InternalRow row) {
            for (int i = 0; i < values.length; i++) {
                Object key = getters[i].getFieldOrNull(row);
                int comparison = compare(i, key, values[i]);
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

    /** A bounded tuple interval with virtual, unencoded endpoints. */
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

    /** Scan intervals and key predicates, rebuilt from the query and ordered index fields. */
    public static final class Plan {
        private final List<DataField> fields;
        private final List<Predicate> predicates;
        private final List<LeafPredicate> filters;
        private final List<Interval> intervals;
        private final int boundColumns;
        private final boolean pointLookup;

        private Plan(
                List<DataField> fields,
                List<Predicate> predicates,
                List<LeafPredicate> filters,
                List<Interval> intervals,
                int boundColumns,
                boolean pointLookup) {
            this.fields = fields;
            this.predicates = predicates;
            this.filters = filters;
            this.intervals = intervals;
            this.boundColumns = boundColumns;
            this.pointLookup = pointLookup;
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

        public int filterColumns() {
            return (int)
                    filters.stream()
                            .map(leaf -> leaf.fieldRefOptional().get().index())
                            .distinct()
                            .count();
        }

        public boolean isPointLookup() {
            return pointLookup;
        }

        public boolean test(InternalRow key) {
            for (LeafPredicate leaf : filters) {
                if (!leaf.test(key)) {
                    return false;
                }
            }
            return true;
        }

        /** Point probes are unrestricted; prefix/range scans use the selected-file byte budget. */
        public boolean canScan(List<GlobalIndexIOMeta> selected, long scanBudget) {
            if (pointLookup || selected.isEmpty()) {
                return true;
            }
            if (scanBudget <= 0) {
                return false;
            }
            long remaining = scanBudget;
            for (GlobalIndexIOMeta file : selected) {
                if (file.fileSize() > remaining) {
                    return false;
                }
                remaining -= file.fileSize();
            }
            return true;
        }

        public List<GlobalIndexIOMeta> selectFiles(List<GlobalIndexIOMeta> files) {
            CompositeKeySerializer serializer = new CompositeKeySerializer(new RowType(fields));
            List<GlobalIndexIOMeta> result = new ArrayList<>();
            for (GlobalIndexIOMeta file : files) {
                if (intervals.isEmpty()) {
                    break;
                }
                if (file.metadata() == null) {
                    result.add(file);
                    continue;
                }
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
