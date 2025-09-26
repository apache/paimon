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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ArrayUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.FIELDS_SEPARATOR;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE;
import static org.apache.paimon.CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final boolean ignoreDelete;
    private final List<WrapperWithFieldIndex<FieldsComparator>> fieldSeqComparators;
    private final boolean fieldSequenceEnabled;
    private final List<WrapperWithFieldIndex<FieldAggregator>> fieldAggregators;
    private final boolean removeRecordOnDelete;
    private final Set<Integer> sequenceGroupPartialDelete;
    private final boolean[] nullables;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private GenericRow row;
    private KeyValue reused;
    private boolean currentDeleteRow;
    private boolean notNullColumnFilled;

    /**
     * If the first value is retract, and no insert record is received, the row kind should be
     * RowKind.DELETE. (Partial update sequence group may not correctly set currentDeleteRow if no
     * RowKind.INSERT value is received)
     */
    private boolean meetInsert;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            boolean ignoreDelete,
            Map<Integer, FieldsComparator> fieldSeqComparators,
            Map<Integer, FieldAggregator> fieldAggregators,
            boolean fieldSequenceEnabled,
            boolean removeRecordOnDelete,
            Set<Integer> sequenceGroupPartialDelete,
            boolean[] nullables) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
        this.fieldSeqComparators = getKeySortedListFromMap(fieldSeqComparators);
        this.fieldAggregators = getKeySortedListFromMap(fieldAggregators);
        this.fieldSequenceEnabled = fieldSequenceEnabled;
        this.removeRecordOnDelete = removeRecordOnDelete;
        this.sequenceGroupPartialDelete = sequenceGroupPartialDelete;
        this.nullables = nullables;
    }

    @Override
    public void reset() {
        this.currentKey = null;
        this.meetInsert = false;
        this.notNullColumnFilled = false;
        this.row = new GenericRow(getters.length);
        this.latestSequenceNumber = 0;
        fieldAggregators.forEach(w -> w.getValue().reset());
    }

    @Override
    public void add(KeyValue kv) {
        // refresh key object to avoid reference overwritten
        currentKey = kv.key();
        currentDeleteRow = false;
        if (kv.valueKind().isRetract()) {

            if (!notNullColumnFilled) {
                initRow(row, kv.value());
                notNullColumnFilled = true;
            }

            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            if (ignoreDelete) {
                return;
            }

            latestSequenceNumber = kv.sequenceNumber();

            if (fieldSequenceEnabled) {
                retractWithSequenceGroup(kv);
                return;
            }

            if (removeRecordOnDelete) {
                if (kv.valueKind() == RowKind.DELETE) {
                    currentDeleteRow = true;
                    row = new GenericRow(getters.length);
                    initRow(row, kv.value());
                }
                return;
            }

            String msg =
                    String.join(
                            "\n",
                            "By default, Partial update can not accept delete records,"
                                    + " you can choose one of the following solutions:",
                            "1. Configure 'ignore-delete' to ignore delete records.",
                            "2. Configure 'partial-update.remove-record-on-delete' to remove the whole row when receiving delete records.",
                            "3. Configure 'sequence-group's to retract partial columns. Also configure 'partial-update.remove-record-on-sequence-group' to remove the whole row when receiving deleted records of `specified sequence group`.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        if (fieldSeqComparators.isEmpty()) {
            updateNonNullFields(kv);
        } else {
            updateWithSequenceGroup(kv);
        }
        meetInsert = true;
        notNullColumnFilled = true;
    }

    private void updateNonNullFields(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            if (field != null) {
                row.setField(i, field);
            } else {
                if (!nullables[i]) {
                    throw new IllegalArgumentException("Field " + i + " can not be null");
                }
            }
        }
    }

    private void updateWithSequenceGroup(KeyValue kv) {

        Iterator<WrapperWithFieldIndex<FieldsComparator>> comparatorIter =
                fieldSeqComparators.iterator();
        WrapperWithFieldIndex<FieldsComparator> curComparator =
                comparatorIter.hasNext() ? comparatorIter.next() : null;
        Iterator<WrapperWithFieldIndex<FieldAggregator>> aggIter = fieldAggregators.iterator();
        WrapperWithFieldIndex<FieldAggregator> curAgg = aggIter.hasNext() ? aggIter.next() : null;

        boolean[] isProcessedSequenceField = new boolean[getters.length];
        for (int i = 0; i < getters.length; i++) {
            FieldsComparator seqComparator = null;
            if (curComparator != null && curComparator.fieldIndex == i) {
                seqComparator = curComparator.getValue();
                curComparator = comparatorIter.hasNext() ? comparatorIter.next() : null;
            }

            FieldAggregator aggregator = null;
            if (curAgg != null && curAgg.fieldIndex == i) {
                aggregator = curAgg.getValue();
                curAgg = aggIter.hasNext() ? aggIter.next() : null;
            }

            Object accumulator = row.getField(i);
            if (seqComparator != null) {
                // Skip if this field has already been processed as part of a sequence group
                if (isProcessedSequenceField[i]) {
                    continue;
                }

                if (isEmptySequenceGroup(kv, seqComparator, isProcessedSequenceField)) {
                    // skip null sequence group
                    continue;
                }

                Object field = getters[i].getFieldOrNull(kv.value());
                if (seqComparator.compare(kv.value(), row) >= 0) {
                    int index = i;

                    // Multiple sequence fields should be updated at once.
                    if (Arrays.stream(seqComparator.compareFields())
                            .anyMatch(seqIndex -> seqIndex == index)) {
                        for (int fieldIndex : seqComparator.compareFields()) {
                            row.setField(
                                    fieldIndex, getters[fieldIndex].getFieldOrNull(kv.value()));
                            // Mark these sequence fields as processed
                            isProcessedSequenceField[fieldIndex] = true;
                        }
                        continue;
                    }
                    row.setField(
                            i, aggregator == null ? field : aggregator.agg(accumulator, field));
                } else if (aggregator != null) {
                    row.setField(i, aggregator.aggReversed(accumulator, field));
                }
            } else {
                Object field = getters[i].getFieldOrNull(kv.value());
                if (aggregator != null) {
                    row.setField(i, aggregator.agg(accumulator, field));
                } else if (field != null) {
                    row.setField(i, field);
                }
            }
        }
    }

    private boolean isEmptySequenceGroup(
            KeyValue kv, FieldsComparator comparator, boolean[] isProcessedSequenceField) {
        for (int fieldIndex : comparator.compareFields()) {
            if (getters[fieldIndex].getFieldOrNull(kv.value()) != null) {
                return false;
            }
        }

        // Mark these sequence fields as processed
        for (int fieldIndex : comparator.compareFields()) {
            isProcessedSequenceField[fieldIndex] = true;
        }

        return true;
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        Set<Integer> updatedSequenceFields = new HashSet<>();
        Iterator<WrapperWithFieldIndex<FieldsComparator>> comparatorIter =
                fieldSeqComparators.iterator();
        WrapperWithFieldIndex<FieldsComparator> curComparator =
                comparatorIter.hasNext() ? comparatorIter.next() : null;
        Iterator<WrapperWithFieldIndex<FieldAggregator>> aggIter = fieldAggregators.iterator();
        WrapperWithFieldIndex<FieldAggregator> curAgg = aggIter.hasNext() ? aggIter.next() : null;

        boolean[] isProcessedSequenceField = new boolean[getters.length];
        for (int i = 0; i < getters.length; i++) {
            FieldsComparator seqComparator = null;
            if (curComparator != null && curComparator.fieldIndex == i) {
                seqComparator = curComparator.getValue();
                curComparator = comparatorIter.hasNext() ? comparatorIter.next() : null;
            }

            FieldAggregator aggregator = null;
            if (curAgg != null && curAgg.fieldIndex == i) {
                aggregator = curAgg.getValue();
                curAgg = aggIter.hasNext() ? aggIter.next() : null;
            }

            if (seqComparator != null) {
                // Skip if this field has already been processed as part of a sequence group
                if (isProcessedSequenceField[i]) {
                    continue;
                }

                if (isEmptySequenceGroup(kv, seqComparator, isProcessedSequenceField)) {
                    // skip null sequence group
                    continue;
                }

                if (seqComparator.compare(kv.value(), row) >= 0) {
                    int index = i;

                    // Multiple sequence fields should be updated at once.
                    if (Arrays.stream(seqComparator.compareFields())
                            .anyMatch(field -> field == index)) {
                        for (int field : seqComparator.compareFields()) {
                            if (!updatedSequenceFields.contains(field)) {
                                if (kv.valueKind() == RowKind.DELETE
                                        && sequenceGroupPartialDelete.contains(field)) {
                                    currentDeleteRow = true;
                                    row = new GenericRow(getters.length);
                                    initRow(row, kv.value());
                                    return;
                                } else {
                                    row.setField(field, getters[field].getFieldOrNull(kv.value()));
                                    updatedSequenceFields.add(field);
                                }
                            }
                            // Mark these sequence fields as processed
                            isProcessedSequenceField[field] = true;
                        }
                    } else {
                        // retract normal field
                        if (aggregator == null) {
                            row.setField(i, null);
                        } else {
                            // retract agg field
                            Object accumulator = getters[i].getFieldOrNull(row);
                            row.setField(
                                    i,
                                    aggregator.retract(
                                            accumulator, getters[i].getFieldOrNull(kv.value())));
                        }
                    }
                } else if (aggregator != null) {
                    // retract agg field for old sequence
                    Object accumulator = getters[i].getFieldOrNull(row);
                    row.setField(
                            i,
                            aggregator.retract(accumulator, getters[i].getFieldOrNull(kv.value())));
                }
            }
        }
    }

    private void initRow(GenericRow row, InternalRow value) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(value);
            if (!nullables[i]) {
                if (field != null) {
                    row.setField(i, field);
                } else {
                    throw new IllegalArgumentException("Field " + i + " can not be null");
                }
            }
        }
    }

    @Override
    public KeyValue getResult() {
        if (reused == null) {
            reused = new KeyValue();
        }

        RowKind rowKind = currentDeleteRow || !meetInsert ? RowKind.DELETE : RowKind.INSERT;
        return reused.replace(currentKey, latestSequenceNumber, rowKind, row);
    }

    @Override
    public boolean requireCopy() {
        return false;
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options options, RowType rowType, List<String> primaryKeys) {
        return new Factory(options, rowType, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final boolean ignoreDelete;
        private final RowType rowType;

        private final List<DataType> tableTypes;

        private final Map<Integer, Supplier<FieldsComparator>> fieldSeqComparators;

        private final Map<Integer, Supplier<FieldAggregator>> fieldAggregators;

        private final boolean removeRecordOnDelete;

        private Set<Integer> sequenceGroupPartialDelete;

        private Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.ignoreDelete = options.get(CoreOptions.IGNORE_DELETE);
            this.rowType = rowType;
            this.tableTypes = rowType.getFieldTypes();
            this.removeRecordOnDelete = options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE);
            String removeRecordOnSequenceGroup =
                    options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP);
            this.sequenceGroupPartialDelete = new HashSet<>();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSeqComparators = new HashMap<>();
            Map<String, Integer> sequenceGroupMap = new HashMap<>();
            List<String> allSequenceFields = new ArrayList<>();
            List<String> fieldsProtectedBySequenceGroup = new ArrayList<>();
            for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                    int[] sequenceFields =
                            Arrays.stream(
                                            k.substring(
                                                            FIELDS_PREFIX.length() + 1,
                                                            k.length()
                                                                    - SEQUENCE_GROUP.length()
                                                                    - 1)
                                                    .split(FIELDS_SEPARATOR))
                                    .mapToInt(fieldName -> requireField(fieldName, fieldNames))
                                    .toArray();

                    Supplier<FieldsComparator> userDefinedSeqComparator =
                            () -> UserDefinedSeqComparator.create(rowType, sequenceFields, true);
                    Arrays.stream(v.split(FIELDS_SEPARATOR))
                            .map(fieldName -> requireField(fieldName, fieldNames))
                            .forEach(
                                    field -> {
                                        if (fieldSeqComparators.containsKey(field)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s is defined repeatedly by multiple groups: %s",
                                                            fieldNames.get(field), k));
                                        }
                                        fieldSeqComparators.put(field, userDefinedSeqComparator);
                                        fieldsProtectedBySequenceGroup.add(fieldNames.get(field));
                                    });

                    // add self
                    for (int index : sequenceFields) {
                        allSequenceFields.add(fieldNames.get(index));
                        String fieldName = fieldNames.get(index);
                        fieldSeqComparators.put(index, userDefinedSeqComparator);
                        sequenceGroupMap.put(fieldName, index);
                    }
                }
            }
            this.fieldAggregators =
                    createFieldAggregators(
                            rowType,
                            primaryKeys,
                            allSequenceFields,
                            fieldsProtectedBySequenceGroup,
                            new CoreOptions(options));

            // check if partial-update.remove-record-on-delete and ignore-delete are enabled at the
            Preconditions.checkState(
                    !(removeRecordOnDelete && ignoreDelete),
                    String.format(
                            "%s and %s have conflicting behavior so should not be enabled at the same time.",
                            CoreOptions.IGNORE_DELETE.key(),
                            PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key()));

            // check if partial-update.remove-record-on-sequence-grou and ignore-delete are enabled
            // at the same time.
            Preconditions.checkState(
                    !(removeRecordOnSequenceGroup != null && ignoreDelete),
                    String.format(
                            "%s and %s have conflicting behavior so should not be enabled at the same time.",
                            CoreOptions.IGNORE_DELETE.key(),
                            PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP.key()));

            // check if partial-update.remove-record-on-delete and sequence-group are enabled at the
            // same time.
            Preconditions.checkState(
                    !removeRecordOnDelete || fieldSeqComparators.isEmpty(),
                    String.format(
                            "%s and %s have conflicting behavior so should not be enabled at the same time.",
                            SEQUENCE_GROUP, PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE.key()));

            if (removeRecordOnSequenceGroup != null) {
                List<String> sequenceGroupFields =
                        Arrays.asList(removeRecordOnSequenceGroup.split(FIELDS_SEPARATOR));
                Preconditions.checkState(
                        sequenceGroupMap.keySet().containsAll(sequenceGroupFields),
                        String.format(
                                "field '%s' defined in '%s' option must be part of sequence groups",
                                removeRecordOnSequenceGroup,
                                PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP.key()));
                sequenceGroupPartialDelete =
                        sequenceGroupFields.stream()
                                .map(sequenceGroupMap::get)
                                .collect(Collectors.toSet());
            }
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            if (projection != null) {
                Map<Integer, FieldsComparator> projectedSeqComparators = new HashMap<>();
                Map<Integer, FieldAggregator> projectedAggregators = new HashMap<>();
                int[] projects = Projection.of(projection).toTopLevelIndexes();
                Map<Integer, Integer> indexMap = new HashMap<>();
                List<DataField> dataFields = rowType.getFields();
                List<DataType> newDataTypes = new ArrayList<>();

                for (int i = 0; i < projects.length; i++) {
                    indexMap.put(projects[i], i);
                    newDataTypes.add(dataFields.get(projects[i]).type());
                }
                RowType newRowType = RowType.builder().fields(newDataTypes).build();

                fieldSeqComparators.forEach(
                        (field, comparatorSupplier) -> {
                            FieldsComparator comparator = comparatorSupplier.get();
                            int newField = indexMap.getOrDefault(field, -1);
                            if (newField != -1) {
                                int[] newSequenceFields =
                                        Arrays.stream(comparator.compareFields())
                                                .map(
                                                        index -> {
                                                            int newIndex =
                                                                    indexMap.getOrDefault(
                                                                            index, -1);
                                                            if (newIndex == -1) {
                                                                throw new RuntimeException(
                                                                        String.format(
                                                                                "Can not find new sequence field "
                                                                                        + "for new field. new field "
                                                                                        + "index is %s",
                                                                                newField));
                                                            } else {
                                                                return newIndex;
                                                            }
                                                        })
                                                .toArray();
                                projectedSeqComparators.put(
                                        newField,
                                        UserDefinedSeqComparator.create(
                                                newRowType, newSequenceFields, true));
                            }
                        });
                for (int i = 0; i < projects.length; i++) {
                    if (fieldAggregators.containsKey(projects[i])) {
                        projectedAggregators.put(i, fieldAggregators.get(projects[i]).get());
                    }
                }

                List<DataType> projectedTypes = Projection.of(projection).project(tableTypes);
                return new PartialUpdateMergeFunction(
                        createFieldGetters(projectedTypes),
                        ignoreDelete,
                        projectedSeqComparators,
                        projectedAggregators,
                        !fieldSeqComparators.isEmpty(),
                        removeRecordOnDelete,
                        sequenceGroupPartialDelete,
                        ArrayUtils.toPrimitiveBoolean(
                                projectedTypes.stream()
                                        .map(DataType::isNullable)
                                        .toArray(Boolean[]::new)));
            } else {
                Map<Integer, FieldsComparator> fieldSeqComparators = new HashMap<>();
                this.fieldSeqComparators.forEach(
                        (f, supplier) -> fieldSeqComparators.put(f, supplier.get()));
                Map<Integer, FieldAggregator> fieldAggregators = new HashMap<>();
                this.fieldAggregators.forEach(
                        (f, supplier) -> fieldAggregators.put(f, supplier.get()));
                return new PartialUpdateMergeFunction(
                        createFieldGetters(tableTypes),
                        ignoreDelete,
                        fieldSeqComparators,
                        fieldAggregators,
                        !fieldSeqComparators.isEmpty(),
                        removeRecordOnDelete,
                        sequenceGroupPartialDelete,
                        ArrayUtils.toPrimitiveBoolean(
                                rowType.getFieldTypes().stream()
                                        .map(DataType::isNullable)
                                        .toArray(Boolean[]::new)));
            }
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            if (fieldSeqComparators.isEmpty()) {
                return new AdjustedProjection(projection, null);
            }

            if (projection == null) {
                return new AdjustedProjection(null, null);
            }
            LinkedHashSet<Integer> extraFields = new LinkedHashSet<>();
            int[] topProjects = Projection.of(projection).toTopLevelIndexes();
            Set<Integer> indexSet = Arrays.stream(topProjects).boxed().collect(Collectors.toSet());
            for (int index : topProjects) {
                Supplier<FieldsComparator> comparatorSupplier = fieldSeqComparators.get(index);
                if (comparatorSupplier == null) {
                    continue;
                }

                FieldsComparator comparator = comparatorSupplier.get();
                for (int field : comparator.compareFields()) {
                    if (!indexSet.contains(field)) {
                        extraFields.add(field);
                    }
                }
            }

            int[] allProjects =
                    Stream.concat(Arrays.stream(topProjects).boxed(), extraFields.stream())
                            .mapToInt(Integer::intValue)
                            .toArray();

            int[][] pushDown = Projection.of(allProjects).toNestedIndexes();
            int[][] outer =
                    Projection.of(IntStream.range(0, topProjects.length).toArray())
                            .toNestedIndexes();
            return new AdjustedProjection(pushDown, outer);
        }

        private int requireField(String fieldName, List<String> fieldNames) {
            int field = fieldNames.indexOf(fieldName);
            if (field == -1) {
                throw new IllegalArgumentException(
                        String.format("Field %s can not be found in table schema", fieldName));
            }

            return field;
        }

        /**
         * Creating aggregation function for the columns.
         *
         * @return The aggregators for each column.
         */
        private Map<Integer, Supplier<FieldAggregator>> createFieldAggregators(
                RowType rowType,
                List<String> primaryKeys,
                List<String> allSequenceFields,
                List<String> fieldsProtectedBySequenceGroup,
                CoreOptions options) {

            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            Map<Integer, Supplier<FieldAggregator>> fieldAggregators = new HashMap<>();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);

                String aggFuncName =
                        getAggFuncName(
                                fieldName,
                                options,
                                primaryKeys,
                                allSequenceFields,
                                fieldsProtectedBySequenceGroup);
                if (aggFuncName != null) {
                    fieldAggregators.put(
                            i,
                            () ->
                                    FieldAggregatorFactory.create(
                                            fieldType, fieldName, aggFuncName, options));
                }
            }
            return fieldAggregators;
        }
    }

    @Nullable
    public static String getAggFuncName(
            String fieldName,
            CoreOptions options,
            List<String> primaryKeys,
            List<String> sequenceFields,
            List<String> fieldsProtectedBySequenceGroup) {
        if (sequenceFields.contains(fieldName)) {
            // no agg for sequence fields
            return null;
        }

        if (primaryKeys.contains(fieldName)) {
            // aggregate by primary keys, so they do not aggregate
            return FieldPrimaryKeyAggFactory.NAME;
        }

        String aggFuncName = options.fieldAggFunc(fieldName);
        if (aggFuncName == null) {
            aggFuncName = options.fieldsDefaultFunc();
        }

        if (aggFuncName != null) {
            // last_non_null_value doesn't require sequence group
            checkArgument(
                    aggFuncName.equals(FieldLastNonNullValueAggFactory.NAME)
                            || fieldsProtectedBySequenceGroup.contains(fieldName),
                    "Must use sequence group for aggregation functions but not found for field %s.",
                    fieldName);
        }
        return aggFuncName;
    }

    private <T> List<WrapperWithFieldIndex<T>> getKeySortedListFromMap(Map<Integer, T> map) {
        List<WrapperWithFieldIndex<T>> res = new ArrayList<>();
        map.forEach(
                (index, value) -> {
                    res.add(new WrapperWithFieldIndex<>(value, index));
                });
        Collections.sort(res);
        return res;
    }

    private static class WrapperWithFieldIndex<T> implements Comparable<WrapperWithFieldIndex<T>> {
        private final T value;
        private final int fieldIndex;

        WrapperWithFieldIndex(T value, int fieldIndex) {
            this.value = value;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int compareTo(PartialUpdateMergeFunction.WrapperWithFieldIndex<T> o) {
            return this.fieldIndex - o.fieldIndex;
        }

        public T getValue() {
            return value;
        }
    }
}
