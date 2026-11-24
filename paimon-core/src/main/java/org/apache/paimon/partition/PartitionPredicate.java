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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.statistics.FullSimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternal;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A special predicate to filter partition only, just like {@link Predicate}, this should be thread
 * safe.
 *
 * @since 1.3.0
 */
@ThreadSafe
public interface PartitionPredicate extends Serializable {

    /**
     * Test based on the specific partition.
     *
     * @return return true when hit, false when not hit.
     */
    boolean test(BinaryRow partition);

    /**
     * Test based on the statistical information to determine whether a hit is possible.
     *
     * @return return true is likely to hit (there may also be false positives), return false is
     *     absolutely not possible to hit.
     */
    boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    /**
     * Compared to the multiple method, this approach can accept filtering of partially partitioned
     * fields.
     */
    @Nullable
    static PartitionPredicate fromPredicate(RowType partitionType, Predicate predicate) {
        if (partitionType.getFieldCount() == 0 || predicate == null) {
            return null;
        }

        return new DefaultPartitionPredicate(predicate);
    }

    /** Create {@link PartitionPredicate} from multiple partitions. */
    @Nullable
    static PartitionPredicate fromMultiple(RowType partitionType, List<BinaryRow> partitions) {
        return fromMultiple(partitionType, new HashSet<>(partitions));
    }

    /** Create {@link PartitionPredicate} from multiple partitions. */
    @Nullable
    static PartitionPredicate fromMultiple(RowType partitionType, Set<BinaryRow> partitions) {
        if (partitionType.getFieldCount() == 0 || partitions.isEmpty()) {
            return null;
        }

        return new MultiplePartitionPredicate(
                new RowDataToObjectArrayConverter(partitionType), partitions);
    }

    /** Creates {@link PartitionPredicate} that combines multiple predicates using logical AND. */
    @Nullable
    static PartitionPredicate and(List<PartitionPredicate> predicates) {
        if (predicates.isEmpty()) {
            return null;
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        return new AndPartitionPredicate(predicates);
    }

    PartitionPredicate ALWAYS_FALSE =
            new PartitionPredicate() {
                @Override
                public boolean test(BinaryRow part) {
                    return false;
                }

                @Override
                public boolean test(
                        long rowCount,
                        InternalRow minValues,
                        InternalRow maxValues,
                        InternalArray nullCounts) {
                    return false;
                }
            };

    PartitionPredicate ALWAYS_TRUE =
            new PartitionPredicate() {
                @Override
                public boolean test(BinaryRow part) {
                    return true;
                }

                @Override
                public boolean test(
                        long rowCount,
                        InternalRow minValues,
                        InternalRow maxValues,
                        InternalArray nullCounts) {
                    return true;
                }
            };

    /** A {@link PartitionPredicate} using {@link Predicate}. */
    class DefaultPartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 1L;

        private final Predicate predicate;

        private DefaultPartitionPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean test(BinaryRow part) {
            return predicate.test(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return predicate.test(rowCount, minValues, maxValues, nullCounts);
        }

        public Predicate predicate() {
            return predicate;
        }

        @Override
        public String toString() {
            return predicate.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DefaultPartitionPredicate that = (DefaultPartitionPredicate) o;
            return Objects.equals(predicate, that.predicate);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(predicate);
        }
    }

    /**
     * A {@link PartitionPredicate} optimizing for multiple partitions. Its FieldStats filtering
     * effect may not be as good as {@link DefaultPartitionPredicate}.
     */
    class MultiplePartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 2L;
        private static final int MIN_PARTITIONS_FOR_RANGE_SEARCH = 64;

        private final Set<BinaryRow> partitions;
        private final int fieldNum;
        private final Predicate[] min;
        private final Predicate[] max;
        @Nullable private final PartitionRangeIndex partitionRangeIndex;

        private MultiplePartitionPredicate(
                RowDataToObjectArrayConverter converter, Set<BinaryRow> partitions) {
            this.partitions = partitions;
            // For a single partition the combined bounds already describe its field values.
            Object[][] partitionValues =
                    partitions.size() > 1 ? new Object[partitions.size()][] : null;
            RowType partitionType = converter.rowType();
            this.fieldNum = partitionType.getFieldCount();
            @SuppressWarnings("unchecked")
            Serializer<Object>[] serializers = new Serializer[fieldNum];
            FullSimpleColStatsCollector[] collectors = new FullSimpleColStatsCollector[fieldNum];
            min = new Predicate[fieldNum];
            max = new Predicate[fieldNum];
            for (int i = 0; i < fieldNum; i++) {
                serializers[i] = InternalSerializers.create(partitionType.getTypeAt(i));
                collectors[i] = new FullSimpleColStatsCollector();
            }
            int index = 0;
            for (BinaryRow part : partitions) {
                Object[] fields = converter.convert(part);
                if (partitionValues != null) {
                    partitionValues[index++] = fields;
                }
                for (int i = 0; i < fields.length; i++) {
                    collectors[i].collect(fields[i], serializers[i]);
                }
            }
            PredicateBuilder builder = new PredicateBuilder(partitionType);
            SimpleColStats[] columnStats =
                    partitions.size() >= MIN_PARTITIONS_FOR_RANGE_SEARCH
                            ? new SimpleColStats[fieldNum]
                            : null;
            for (int i = 0; i < collectors.length; i++) {
                SimpleColStats stats = collectors[i].result();
                if (columnStats != null) {
                    columnStats[i] = stats;
                }
                Long nullCount = stats.nullCount();
                checkArgument(nullCount != null, "nullCount cannot be null!");
                if (nullCount == partitions.size()) {
                    min[i] = builder.isNull(i);
                    max[i] = builder.isNull(i);
                } else {
                    Object minValue = checkNotNull(stats.min());
                    Object maxValue = checkNotNull(stats.max());
                    min[i] = builder.greaterOrEqual(i, minValue);
                    max[i] = builder.lessOrEqual(i, maxValue);
                    if (nullCount > 0) {
                        min[i] = PredicateBuilder.or(builder.isNull(i), min[i]);
                        max[i] = PredicateBuilder.or(builder.isNull(i), max[i]);
                    }
                }
            }
            this.partitionRangeIndex =
                    partitionValues == null
                            ? null
                            : new PartitionRangeIndex(partitionValues, converter, columnStats);
        }

        @Override
        public boolean test(BinaryRow part) {
            return partitions.contains(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            if (fieldNum == 0
                    || minValues.getFieldCount() < fieldNum
                    || maxValues.getFieldCount() < fieldNum
                    || (nullCounts.size() > 0 && nullCounts.size() < fieldNum)) {
                return true;
            }

            for (int i = 0; i < fieldNum; i++) {
                if (!min[i].test(rowCount, minValues, maxValues, nullCounts)
                        || !max[i].test(rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return partitionRangeIndex == null
                    || rowCount <= 0
                    || partitionRangeIndex.test(rowCount, minValues, maxValues, nullCounts);
        }

        public Optional<BinaryRow> extractSinglePartition() {
            return partitions.size() == 1
                    ? Optional.of(partitions.iterator().next())
                    : Optional.empty();
        }

        public Set<BinaryRow> partitions() {
            return partitions;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MultiplePartitionPredicate that = (MultiplePartitionPredicate) o;
            return fieldNum == that.fieldNum
                    && Objects.equals(partitions, that.partitions)
                    && Objects.deepEquals(min, that.min)
                    && Objects.deepEquals(max, that.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitions, fieldNum, Arrays.hashCode(min), Arrays.hashCode(max));
        }

        /** Checks partition tuples, with a lazy range index for each partition column. */
        static final class PartitionRangeIndex implements Serializable {

            private static final long serialVersionUID = 1L;
            private static final int LINEAR_SCANS_BEFORE_SORT = 17;

            private final Object[][] partitionValues;
            private final RowDataToObjectArrayConverter converter;
            @Nullable private final ColumnRangeIndex[] columns;

            PartitionRangeIndex(
                    Object[][] partitionValues,
                    RowDataToObjectArrayConverter converter,
                    @Nullable SimpleColStats[] columnStats) {
                this.partitionValues = partitionValues;
                this.converter = converter;
                columns = columnStats == null ? null : new ColumnRangeIndex[columnStats.length];
                if (columns != null) {
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] =
                                new ColumnRangeIndex(
                                        i, converter.rowType().getTypeAt(i), columnStats[i]);
                    }
                }
            }

            boolean test(
                    long rowCount,
                    InternalRow minValues,
                    InternalRow maxValues,
                    InternalArray nullCounts) {
                ManifestLookup lookup =
                        new ManifestLookup(
                                partitionValues,
                                converter.convert(minValues),
                                converter.convert(maxValues),
                                nullCounts,
                                rowCount);
                if (columns == null) {
                    return matches(lookup);
                }

                ColumnRangeIndex ready = null;
                boolean trackRejections = false;
                long requiredRejections = (long) partitionValues.length * LINEAR_SCANS_BEFORE_SORT;
                for (ColumnRangeIndex column : columns) {
                    if (!column.restrict(lookup)) {
                        continue;
                    }
                    Object[][] sorted = column.sortedPartitions;
                    if (sorted != null) {
                        if (!column.selectCandidates(sorted, lookup)) {
                            return false;
                        }
                    } else {
                        trackRejections = true;
                        if (column.rejectedPartitions >= requiredRejections
                                && (ready == null
                                        || column.rejectedPartitions > ready.rejectedPartitions)) {
                            ready = column;
                        }
                    }
                }

                // Reuse a small candidate set instead of building another index for this lookup.
                if (ready != null
                        && (lookup.indexedField < 0
                                || lookup.candidateCount() >= MIN_PARTITIONS_FOR_RANGE_SEARCH)
                        && !ready.selectCandidates(ready.build(partitionValues), lookup)) {
                    return false;
                }

                lookup.rejections = trackRejections ? new int[columns.length] : null;
                boolean matches = matches(lookup);
                if (lookup.rejections != null) {
                    for (int i = 0; i < columns.length; i++) {
                        if (lookup.rejections[i] > 0 && columns[i].sortedPartitions == null) {
                            columns[i].recordRejections(lookup.rejections[i]);
                        }
                    }
                }
                return matches;
            }

            @Nullable
            Object[][] sortedPartitions(int field) {
                return columns == null ? null : columns[field].sortedPartitions;
            }

            private boolean matches(ManifestLookup lookup) {
                if (lookup.indexedField >= 0) {
                    lookup.clearBounds(lookup.indexedField);
                }
                return matchesAny(0, lookup.nullEnd, lookup)
                        || matchesAny(lookup.start, lookup.end, lookup);
            }

            private boolean matchesAny(int start, int end, ManifestLookup lookup) {
                int[] rejections = lookup.rejections;
                for (int i = start; i < end; i++) {
                    int rejectedField = firstMismatch(lookup.candidates[i], lookup);
                    if (rejectedField < 0) {
                        return true;
                    }
                    if (rejections != null) {
                        // Only count work that reached this column in schema order.
                        rejections[rejectedField]++;
                    }
                }
                return false;
            }

            private int firstMismatch(Object[] partition, ManifestLookup lookup) {
                for (int i = 0; i < partition.length; i++) {
                    Object value = partition[i];
                    if (value == null) {
                        if (lookup.nulls[i] == 0) {
                            return i;
                        }
                    } else if (lookup.nulls[i] == lookup.rowCount) {
                        return i;
                    } else if (lookup.lower[i] != null && lookup.upper[i] != null) {
                        DataType type = converter.rowType().getTypeAt(i);
                        if (compareLiteral(type, value, lookup.lower[i]) < 0
                                || compareLiteral(type, value, lookup.upper[i]) > 0) {
                            return i;
                        }
                    }
                }
                return -1;
            }

            private static final class ManifestLookup {

                private final Object[] lower;
                private final Object[] upper;
                private final long[] nulls;
                private final long rowCount;
                @Nullable private int[] rejections;
                private Object[][] candidates;
                private int indexedField = -1;
                private int nullEnd;
                private int start;
                private int end;

                private ManifestLookup(
                        Object[][] partitions,
                        Object[] lower,
                        Object[] upper,
                        InternalArray nullCounts,
                        long rowCount) {
                    this.candidates = partitions;
                    this.end = partitions.length;
                    this.lower = lower;
                    this.upper = upper;
                    this.rowCount = rowCount;
                    this.nulls = new long[lower.length];
                    for (int i = 0; i < lower.length; i++) {
                        nulls[i] =
                                nullCounts.size() == 0 || nullCounts.isNullAt(i)
                                        ? -1
                                        : nullCounts.getLong(i);
                    }
                }

                private void clearBounds(int field) {
                    lower[field] = null;
                    upper[field] = null;
                }

                private int candidateCount() {
                    return nullEnd + (end - start);
                }

                private boolean consider(
                        Object[][] sorted, int field, int nullEnd, int start, int end) {
                    int count = nullEnd + (end - start);
                    if (count == 0) {
                        return false;
                    }
                    if (indexedField < 0 || count < candidateCount()) {
                        candidates = sorted;
                        indexedField = field;
                        this.nullEnd = nullEnd;
                        this.start = start;
                        this.end = end;
                    }
                    return true;
                }
            }

            private static final class ColumnRangeIndex implements Serializable {

                private static final long serialVersionUID = 1L;

                private final int field;
                private final DataType type;
                private final int nullCount;
                @Nullable private final Object min;
                @Nullable private final Object max;
                private transient volatile long rejectedPartitions;
                @Nullable private transient volatile Object[][] sortedPartitions;

                private ColumnRangeIndex(int field, DataType type, SimpleColStats stats) {
                    this.field = field;
                    this.type = type;
                    this.nullCount = checkNotNull(stats.nullCount()).intValue();
                    this.min = stats.min();
                    this.max = stats.max();
                }

                private boolean restrict(ManifestLookup lookup) {
                    long manifestNulls = lookup.nulls[field];
                    if (min != null
                            && lookup.lower[field] != null
                            && lookup.upper[field] != null
                            && manifestNulls != lookup.rowCount
                            && (compareLiteral(type, min, lookup.lower[field]) < 0
                                    || compareLiteral(type, max, lookup.upper[field]) > 0)) {
                        return true;
                    }
                    lookup.clearBounds(field);
                    return (manifestNulls == 0 && nullCount != 0)
                            || (manifestNulls == lookup.rowCount && min != null);
                }

                private synchronized void recordRejections(int count) {
                    if (sortedPartitions == null) {
                        rejectedPartitions += count;
                    }
                }

                private synchronized Object[][] build(Object[][] partitions) {
                    if (sortedPartitions == null) {
                        sortedPartitions =
                                min instanceof Integer
                                        ? sortIntegerPartitions(partitions)
                                        : sortPartitions(partitions);
                    }
                    return sortedPartitions;
                }

                private Object[][] sortIntegerPartitions(Object[][] partitions) {
                    Object[][] sorted = new Object[partitions.length][];
                    long[] order = new long[partitions.length - nullCount];
                    int nextNull = 0;
                    int nextKey = 0;
                    for (int i = 0; i < partitions.length; i++) {
                        Integer key = (Integer) partitions[i][field];
                        if (key == null) {
                            sorted[nextNull++] = partitions[i];
                        } else {
                            // Keep signed ordering and every tuple that shares a key.
                            order[nextKey++] = ((long) key << Integer.SIZE) | i;
                        }
                    }
                    Arrays.sort(order);
                    for (int i = 0; i < order.length; i++) {
                        sorted[nullCount + i] = partitions[(int) order[i]];
                    }
                    return sorted;
                }

                private Object[][] sortPartitions(Object[][] partitions) {
                    Object[][] sorted = partitions.clone();
                    if (nullCount == 0) {
                        Arrays.sort(
                                sorted,
                                (left, right) -> compareLiteral(type, left[field], right[field]));
                    } else {
                        Comparator<Object> keys =
                                Comparator.nullsFirst(
                                        (left, right) -> compareLiteral(type, left, right));
                        Arrays.sort(
                                sorted, Comparator.comparing(partition -> partition[field], keys));
                    }
                    return sorted;
                }

                private boolean selectCandidates(Object[][] sorted, ManifestLookup lookup) {
                    long manifestNulls = lookup.nulls[field];
                    int nullEnd = manifestNulls == 0 ? 0 : nullCount;
                    int start = nullCount;
                    int end = nullCount;
                    if (manifestNulls != lookup.rowCount) {
                        Object lower = lookup.lower[field];
                        Object upper = lookup.upper[field];
                        if (lower != null && compareLiteral(type, min, lower) < 0) {
                            start = findBound(sorted, lower, nullCount, false);
                        }
                        end = start;
                        if (upper == null || compareLiteral(type, max, upper) <= 0) {
                            end = sorted.length;
                        } else if (start < sorted.length
                                && compareLiteral(type, sorted[start][field], upper) <= 0) {
                            end = findBound(sorted, upper, start + 1, true);
                        }
                    }
                    return lookup.consider(sorted, field, nullEnd, start, end);
                }

                private int findBound(
                        Object[][] sorted, Object value, int start, boolean afterEqual) {
                    int end = sorted.length;
                    while (start < end) {
                        int middle = (start + end) >>> 1;
                        int comparison = compareLiteral(type, sorted[middle][field], value);
                        if (comparison < 0 || (afterEqual && comparison == 0)) {
                            start = middle + 1;
                        } else {
                            end = middle;
                        }
                    }
                    return start;
                }
            }
        }
    }

    /** AND-combines multiple {@link PartitionPredicate}s. */
    class AndPartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 1L;

        private final List<PartitionPredicate> predicates;

        private AndPartitionPredicate(List<PartitionPredicate> predicates) {
            checkArgument(!predicates.isEmpty());
            this.predicates = Collections.unmodifiableList(new ArrayList<>(predicates));
        }

        public List<PartitionPredicate> predicates() {
            return predicates;
        }

        @Override
        public boolean test(BinaryRow partition) {
            for (PartitionPredicate predicate : predicates) {
                if (!predicate.test(partition)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            for (PartitionPredicate predicate : predicates) {
                if (!predicate.test(rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            if (predicates.size() == 1) {
                return predicates.get(0).toString();
            }
            return "AND" + "(" + predicates + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AndPartitionPredicate that = (AndPartitionPredicate) o;
            return Objects.equals(predicates, that.predicates);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(predicates);
        }
    }

    static Predicate createPartitionPredicate(RowType rowType, Map<String, Object> partition) {
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        for (Map.Entry<String, Object> entry : partition.entrySet()) {
            Object literal = entry.getValue();
            int idx = fieldNames.indexOf(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    static Predicate createPartitionPredicate(
            RowType rowType, RowDataToObjectArrayConverter converter, List<BinaryRow> partitions) {
        Predicate partFilter = null;
        for (BinaryRow partition : partitions) {
            if (partFilter == null) {
                partFilter = createSinglePartitionPredicate(rowType, converter, partition);
            } else {
                partFilter =
                        PredicateBuilder.or(
                                partFilter,
                                createSinglePartitionPredicate(rowType, converter, partition));
            }
        }
        return partFilter;
    }

    static Predicate createSinglePartitionPredicate(
            RowType rowType, RowDataToObjectArrayConverter converter, BinaryRow partition) {
        RowType partitionType = converter.rowType();
        Preconditions.checkArgument(
                partition.getFieldCount() == partitionType.getFieldCount(),
                "Partition's field count should be equal to partitionType's field count.");
        Object[] partitionSpec = converter.convert(partition);
        Map<String, Object> partitionMap = new HashMap<>(partitionSpec.length);
        for (int i = 0; i < partitionSpec.length; i++) {
            partitionMap.put(partitionType.getFields().get(i).name(), partitionSpec[i]);
        }
        return createPartitionPredicate(rowType, partitionMap);
    }

    @Nullable
    static Predicate createPartitionPredicate(
            Map<String, String> spec, RowType rowType, String defaultPartValue) {
        Map<String, Object> internalValues = convertSpecToInternal(spec, rowType, defaultPartValue);
        return createPartitionPredicate(rowType, internalValues);
    }

    static Predicate createPartitionPredicate(
            List<Map<String, String>> partitions, RowType rowType, String defaultPartValue) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> createPartitionPredicate(p, rowType, defaultPartValue))
                        .toArray(Predicate[]::new));
    }

    static List<BinaryRow> createBinaryPartitions(
            List<Map<String, String>> partitions, RowType partitionType, String defaultPartValue) {
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        List<BinaryRow> result = new ArrayList<>();
        for (Map<String, String> spec : partitions) {
            GenericRow row = convertSpecToInternalRow(spec, partitionType, defaultPartValue);
            result.add(serializer.toBinaryRow(row).copy());
        }
        return result;
    }

    static PartitionPredicate fromMap(
            RowType partitionType, Map<String, String> values, String defaultPartValue) {
        return fromPredicate(
                partitionType, createPartitionPredicate(values, partitionType, defaultPartValue));
    }

    static PartitionPredicate fromMaps(
            RowType partitionType, List<Map<String, String>> values, String defaultPartValue) {
        return fromMultiple(
                partitionType, createBinaryPartitions(values, partitionType, defaultPartValue));
    }

    static Optional<PartitionPredicate> splitPartitionPredicate(
            Predicate dataPredicates, RowType tableType, List<String> partitionKeys) {
        return splitPartitionPredicatesAndDataPredicates(dataPredicates, tableType, partitionKeys)
                .getLeft();
    }

    static Pair<Optional<PartitionPredicate>, List<Predicate>>
            splitPartitionPredicatesAndDataPredicates(
                    Predicate dataPredicates, RowType tableType, List<String> partitionKeys) {
        return splitPartitionPredicatesAndDataPredicates(
                PredicateBuilder.splitAnd(dataPredicates), tableType, partitionKeys);
    }

    static Pair<Optional<PartitionPredicate>, List<Predicate>>
            splitPartitionPredicatesAndDataPredicates(
                    List<Predicate> dataPredicates, RowType tableType, List<String> partitionKeys) {
        if (partitionKeys.isEmpty()) {
            return Pair.of(Optional.empty(), dataPredicates);
        }

        RowType partitionType = tableType.project(partitionKeys);
        PredicateProjectionConverter partitionProjection =
                PredicateProjectionConverter.fromProjection(
                        tableType.projectIndexes(partitionKeys));

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        Set<String> partitionKeySet = new HashSet<>(partitionKeys);
        for (Predicate p : dataPredicates) {
            Optional<Predicate> projected = p.visit(partitionProjection);
            projected.ifPresent(partitionFilters::add);
            if (!partitionKeySet.containsAll(PredicateVisitor.collectFieldNames(p))) {
                nonPartitionFilters.add(p);
            }
        }
        PartitionPredicate partitionPredicate =
                partitionFilters.isEmpty()
                        ? null
                        : PartitionPredicate.fromPredicate(
                                partitionType, PredicateBuilder.and(partitionFilters));
        return Pair.of(Optional.ofNullable(partitionPredicate), nonPartitionFilters);
    }
}
