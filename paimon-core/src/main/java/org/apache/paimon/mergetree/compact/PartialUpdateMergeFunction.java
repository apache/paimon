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
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import static org.apache.paimon.mergetree.compact.aggregate.FieldAggregator.createFieldAggregator;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final boolean ignoreDelete;
    private final Map<Integer, FieldsComparator> fieldSeqComparators;
    private final boolean fieldSequenceEnabled;
    private final Map<Integer, FieldAggregator> fieldAggregators;
    private final boolean removeRecordOnDelete;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private GenericRow row;
    private KeyValue reused;
    private boolean currentDeleteRow;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            boolean ignoreDelete,
            Map<Integer, FieldsComparator> fieldSeqComparators,
            Map<Integer, FieldAggregator> fieldAggregators,
            boolean fieldSequenceEnabled,
            boolean removeRecordOnDelete) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
        this.fieldSeqComparators = fieldSeqComparators;
        this.fieldAggregators = fieldAggregators;
        this.fieldSequenceEnabled = fieldSequenceEnabled;
        this.removeRecordOnDelete = removeRecordOnDelete;
    }

    @Override
    public void reset() {
        this.currentKey = null;
        this.row = new GenericRow(getters.length);
        fieldAggregators.values().forEach(FieldAggregator::reset);
    }

    @Override
    public void add(KeyValue kv) {
        // refresh key object to avoid reference overwritten
        currentKey = kv.key();
        currentDeleteRow = false;

        if (kv.valueKind().isRetract()) {
            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            if (ignoreDelete) {
                return;
            }

            if (fieldSequenceEnabled) {
                retractWithSequenceGroup(kv);
                return;
            }

            if (removeRecordOnDelete) {
                if (kv.valueKind() == RowKind.DELETE) {
                    currentDeleteRow = true;
                    row = new GenericRow(getters.length);
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
                            "3. Configure 'sequence-group's to retract partial columns.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        if (fieldSeqComparators.isEmpty()) {
            updateNonNullFields(kv);
        } else {
            updateWithSequenceGroup(kv);
        }
    }

    private void updateNonNullFields(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            if (field != null) {
                row.setField(i, field);
            }
        }
    }

    private void updateWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            FieldsComparator seqComparator = fieldSeqComparators.get(i);
            FieldAggregator aggregator = fieldAggregators.get(i);
            Object accumulator = getters[i].getFieldOrNull(row);
            if (seqComparator == null) {
                if (aggregator != null) {
                    row.setField(i, aggregator.agg(accumulator, field));
                } else if (field != null) {
                    row.setField(i, field);
                }
            } else {
                if (isEmptySequenceGroup(kv, seqComparator)) {
                    // skip null sequence group
                    continue;
                }

                if (seqComparator.compare(kv.value(), row) >= 0) {
                    int index = i;

                    // Multiple sequence fields should be updated at once.
                    if (Arrays.stream(seqComparator.compareFields())
                            .anyMatch(seqIndex -> seqIndex == index)) {
                        for (int fieldIndex : seqComparator.compareFields()) {
                            row.setField(
                                    fieldIndex, getters[fieldIndex].getFieldOrNull(kv.value()));
                        }
                    }
                    row.setField(
                            i, aggregator == null ? field : aggregator.agg(accumulator, field));
                } else if (aggregator != null) {
                    row.setField(i, aggregator.aggReversed(accumulator, field));
                }
            }
        }
    }

    private boolean isEmptySequenceGroup(KeyValue kv, FieldsComparator comparator) {
        for (int fieldIndex : comparator.compareFields()) {
            if (getters[fieldIndex].getFieldOrNull(kv.value()) != null) {
                return false;
            }
        }

        return true;
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        Set<Integer> updatedSequenceFields = new HashSet<>();

        for (int i = 0; i < getters.length; i++) {
            FieldsComparator seqComparator = fieldSeqComparators.get(i);
            if (seqComparator != null) {
                FieldAggregator aggregator = fieldAggregators.get(i);
                if (isEmptySequenceGroup(kv, seqComparator)) {
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
                                row.setField(field, getters[field].getFieldOrNull(kv.value()));
                                updatedSequenceFields.add(field);
                            }
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

    @Override
    public KeyValue getResult() {
        if (reused == null) {
            reused = new KeyValue();
        }

        RowKind rowKind = currentDeleteRow ? RowKind.DELETE : RowKind.INSERT;
        return reused.replace(currentKey, latestSequenceNumber, rowKind, row);
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

        private Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.ignoreDelete = options.get(CoreOptions.IGNORE_DELETE);
            this.rowType = rowType;
            this.tableTypes = rowType.getFieldTypes();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSeqComparators = new HashMap<>();
            for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                    List<String> sequenceFields =
                            Arrays.stream(
                                            k.substring(
                                                            FIELDS_PREFIX.length() + 1,
                                                            k.length()
                                                                    - SEQUENCE_GROUP.length()
                                                                    - 1)
                                                    .split(FIELDS_SEPARATOR))
                                    .map(fieldName -> validateFieldName(fieldName, fieldNames))
                                    .collect(Collectors.toList());

                    Supplier<FieldsComparator> userDefinedSeqComparator =
                            () -> UserDefinedSeqComparator.create(rowType, sequenceFields);
                    Arrays.stream(v.split(FIELDS_SEPARATOR))
                            .map(
                                    fieldName ->
                                            fieldNames.indexOf(
                                                    validateFieldName(fieldName, fieldNames)))
                            .forEach(
                                    field -> {
                                        if (fieldSeqComparators.containsKey(field)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s is defined repeatedly by multiple groups: %s",
                                                            fieldNames.get(field), k));
                                        }
                                        fieldSeqComparators.put(field, userDefinedSeqComparator);
                                    });

                    // add self
                    sequenceFields.forEach(
                            fieldName -> {
                                int index = fieldNames.indexOf(fieldName);
                                fieldSeqComparators.put(index, userDefinedSeqComparator);
                            });
                }
            }
            this.fieldAggregators =
                    createFieldAggregators(rowType, primaryKeys, new CoreOptions(options));
            if (!fieldAggregators.isEmpty() && fieldSeqComparators.isEmpty()) {
                throw new IllegalArgumentException(
                        "Must use sequence group for aggregation functions.");
            }

            removeRecordOnDelete = options.get(PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE);

            Preconditions.checkState(
                    !(removeRecordOnDelete && ignoreDelete),
                    String.format(
                            "%s and %s have conflicting behavior so should not be enabled at the same time.",
                            CoreOptions.IGNORE_DELETE, PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE));
            Preconditions.checkState(
                    !removeRecordOnDelete || fieldSeqComparators.isEmpty(),
                    String.format(
                            "sequence group and %s have conflicting behavior so should not be enabled at the same time.",
                            PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE));
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
                                                newRowType, newSequenceFields));
                            }
                        });
                for (int i = 0; i < projects.length; i++) {
                    if (fieldAggregators.containsKey(projects[i])) {
                        projectedAggregators.put(i, fieldAggregators.get(projects[i]).get());
                    }
                }

                return new PartialUpdateMergeFunction(
                        createFieldGetters(Projection.of(projection).project(tableTypes)),
                        ignoreDelete,
                        projectedSeqComparators,
                        projectedAggregators,
                        !fieldSeqComparators.isEmpty(),
                        removeRecordOnDelete);
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
                        removeRecordOnDelete);
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

        private String validateFieldName(String fieldName, List<String> fieldNames) {
            int field = fieldNames.indexOf(fieldName);
            if (field == -1) {
                throw new IllegalArgumentException(
                        String.format("Field %s can not be found in table schema", fieldName));
            }

            return fieldName;
        }

        /**
         * Creating aggregation function for the columns.
         *
         * @return The aggregators for each column.
         */
        private Map<Integer, Supplier<FieldAggregator>> createFieldAggregators(
                RowType rowType, List<String> primaryKeys, CoreOptions options) {

            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            Map<Integer, Supplier<FieldAggregator>> fieldAggregators = new HashMap<>();
            String defaultAggFunc = options.fieldsDefaultFunc();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc = options.fieldAggFunc(fieldName);
                boolean ignoreRetract = options.fieldAggIgnoreRetract(fieldName);

                if (strAggFunc != null) {
                    fieldAggregators.put(
                            i,
                            () ->
                                    createFieldAggregator(
                                            fieldType,
                                            strAggFunc,
                                            ignoreRetract,
                                            isPrimaryKey,
                                            options,
                                            fieldName));
                } else if (defaultAggFunc != null) {
                    fieldAggregators.put(
                            i,
                            () ->
                                    createFieldAggregator(
                                            fieldType,
                                            defaultAggFunc,
                                            ignoreRetract,
                                            isPrimaryKey,
                                            options,
                                            fieldName));
                }
            }
            return fieldAggregators;
        }
    }
}
