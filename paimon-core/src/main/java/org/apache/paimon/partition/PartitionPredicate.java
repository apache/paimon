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
     * Tests manifest statistics with the number of later manifests in the current scan. The count
     * is a hint for index construction and does not affect the result.
     */
    default boolean test(
            long rowCount,
            InternalRow minValues,
            InternalRow maxValues,
            InternalArray nullCounts,
            int remainingManifests) {
        return test(rowCount, minValues, maxValues, nullCounts);
    }

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

        private static final long serialVersionUID = 1L;
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
            int rangeField = -1;
            Object rangeMin = null;
            Object rangeMax = null;
            for (int i = 0; i < collectors.length; i++) {
                SimpleColStats stats = collectors[i].result();
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
                    } else if (rangeField == -1
                            && partitions.size() >= MIN_PARTITIONS_FOR_RANGE_SEARCH
                            && compareLiteral(partitionType.getTypeAt(i), minValue, maxValue) < 0) {
                        rangeField = i;
                        rangeMin = minValue;
                        rangeMax = maxValue;
                    }
                }
            }
            this.partitionRangeIndex =
                    partitionValues == null
                            ? null
                            : new PartitionRangeIndex(
                                    partitionValues, converter, rangeField, rangeMin, rangeMax);
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
            return test(rowCount, minValues, maxValues, nullCounts, 0);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts,
                int remainingManifests) {
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
                    || partitionRangeIndex.test(
                            rowCount, minValues, maxValues, nullCounts, remainingManifests);
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

        /** Checks partition tuples, with a lazy index for repeated selective manifest lookups. */
        static final class PartitionRangeIndex implements Serializable {

            private static final long serialVersionUID = 1L;
            private static final int LINEAR_LOOKUPS_BEFORE_SORT = 16;
            private static final int MANIFESTS_PER_SORT_LEVEL = 3;

            private final Object[][] partitionValues;
            private final RowDataToObjectArrayConverter converter;
            private final int field;
            @Nullable private final Object min;
            @Nullable private final Object max;
            private final int minimumRemainingManifests;
            private transient int selectiveLookups;
            @Nullable private transient volatile Object[][] sortedPartitions;

            PartitionRangeIndex(
                    Object[][] partitionValues,
                    RowDataToObjectArrayConverter converter,
                    int field,
                    @Nullable Object min,
                    @Nullable Object max) {
                this.partitionValues = partitionValues;
                this.converter = converter;
                this.field = field;
                this.min = min;
                this.max = max;
                // Estimate how many O(n) scans it takes to amortize O(n log n) sorting.
                int sortLevels =
                        Integer.SIZE - Integer.numberOfLeadingZeros(partitionValues.length - 1);
                this.minimumRemainingManifests = MANIFESTS_PER_SORT_LEVEL * sortLevels;
            }

            private boolean test(
                    long rowCount,
                    InternalRow minValues,
                    InternalRow maxValues,
                    InternalArray nullCounts,
                    int remainingManifests) {
                Object[] lower = converter.convert(minValues);
                Object[] upper = converter.convert(maxValues);
                long[] nulls = new long[lower.length];
                for (int i = 0; i < lower.length; i++) {
                    nulls[i] =
                            nullCounts.size() == 0 || nullCounts.isNullAt(i)
                                    ? -1
                                    : nullCounts.getLong(i);
                }
                if (field >= 0 && lower[field] != null && upper[field] != null) {
                    DataType type = converter.rowType().getTypeAt(field);
                    boolean restrictStart = compareLiteral(type, min, lower[field]) < 0;
                    boolean restrictEnd = compareLiteral(type, max, upper[field]) > 0;
                    if (restrictStart || restrictEnd) {
                        Object[][] sorted = sortedPartitions(remainingManifests);
                        if (sorted != null) {
                            int start =
                                    restrictStart
                                            ? findBound(sorted, type, lower[field], 0, false)
                                            : 0;
                            int end =
                                    restrictEnd
                                            ? findBound(sorted, type, upper[field], start, true)
                                            : sorted.length;
                            // The index has already checked the bounds for these candidates.
                            lower[field] = null;
                            return matchesAny(sorted, start, end, lower, upper, nulls, rowCount);
                        }
                    } else {
                        // The manifest covers the entire range of the indexed field.
                        lower[field] = null;
                    }
                }
                return matchesAny(
                        partitionValues, 0, partitionValues.length, lower, upper, nulls, rowCount);
            }

            /** Returns the cached index or builds one after enough eligible lookups. */
            @Nullable
            Object[][] sortedPartitions(int remainingManifests) {
                Object[][] sorted = sortedPartitions;
                if (sorted != null) {
                    return sorted;
                }
                if (remainingManifests < minimumRemainingManifests) {
                    return null;
                }
                synchronized (this) {
                    if (sortedPartitions == null) {
                        if (selectiveLookups < LINEAR_LOOKUPS_BEFORE_SORT) {
                            selectiveLookups++;
                            return null;
                        }
                        sorted = partitionValues.clone();
                        DataType type = converter.rowType().getTypeAt(field);
                        Arrays.sort(
                                sorted,
                                (left, right) -> compareLiteral(type, left[field], right[field]));
                        sortedPartitions = sorted;
                    }
                    return sortedPartitions;
                }
            }

            private int findBound(
                    Object[][] candidates,
                    DataType type,
                    Object value,
                    int start,
                    boolean afterEqual) {
                int end = candidates.length;
                while (start < end) {
                    int middle = (start + end) >>> 1;
                    int comparison = compareLiteral(type, candidates[middle][field], value);
                    if (comparison < 0 || (afterEqual && comparison == 0)) {
                        start = middle + 1;
                    } else {
                        end = middle;
                    }
                }
                return start;
            }

            private boolean matchesAny(
                    Object[][] candidates,
                    int start,
                    int end,
                    Object[] lower,
                    Object[] upper,
                    long[] nulls,
                    long rowCount) {
                for (int i = start; i < end; i++) {
                    if (mayContainPartition(candidates[i], lower, upper, nulls, rowCount)) {
                        return true;
                    }
                }
                return false;
            }

            private boolean mayContainPartition(
                    Object[] partition,
                    Object[] lower,
                    Object[] upper,
                    long[] nulls,
                    long rowCount) {
                for (int i = 0; i < partition.length; i++) {
                    Object value = partition[i];
                    if (value == null) {
                        if (nulls[i] == 0) {
                            return false;
                        }
                    } else if (nulls[i] == rowCount) {
                        return false;
                    } else if (lower[i] != null && upper[i] != null) {
                        DataType type = converter.rowType().getTypeAt(i);
                        if (compareLiteral(type, value, lower[i]) < 0
                                || compareLiteral(type, value, upper[i]) > 0) {
                            return false;
                        }
                    }
                }
                return true;
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
            return test(rowCount, minValues, maxValues, nullCounts, 0);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts,
                int remainingManifests) {
            for (PartitionPredicate predicate : predicates) {
                if (!predicate.test(
                        rowCount, minValues, maxValues, nullCounts, remainingManifests)) {
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
