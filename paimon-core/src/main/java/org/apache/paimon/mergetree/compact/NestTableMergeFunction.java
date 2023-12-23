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
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.SequenceGenerator;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge,support nest table.
 */
public class NestTableMergeFunction implements MergeFunction<KeyValue> {
    private static final String AGG_FUNC = "agg-func";
    private static final String NESTED_APPEND = "nest-append";
    private static final String NESTED_UPDATE = "nest-update";
    private final boolean ignoreDelete;
    private final Map<Integer, NestTable> nestTableContainer;
    private final Map<Integer, InternalRow.FieldGetter[]> nestTableFieldGetter;
    private final Map<Integer, RowType> nestTableFieldRowType;

    private final Map<Integer, SequenceGenerator> fieldSequences;

    private final InternalRow.FieldGetter[] getters;
    private InternalRow currentKey;

    private GenericRow mergedRow;

    private KeyValue reused;

    private Long latestSequenceNumber;

    public NestTableMergeFunction(
            boolean ignoreDelete,
            Map<Integer, InternalRow.FieldGetter[]> nestTableFieldGetter,
            Map<Integer, RowType> nestTableFieldRowType,
            InternalRow.FieldGetter[] getters,
            Map<Integer, SequenceGenerator> fieldSequences) {
        this.nestTableContainer = new HashMap<>();
        this.mergedRow = new GenericRow(getters.length);
        this.nestTableFieldGetter = nestTableFieldGetter;
        this.nestTableFieldRowType = nestTableFieldRowType;
        this.getters = getters;
        this.fieldSequences = fieldSequences;
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        nestTableContainer.clear();
        this.mergedRow = new GenericRow(getters.length);
        latestSequenceNumber = 0L;
    }

    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            }
            throw new RuntimeException("currently nest-table engine do not support delete");
        }
        latestSequenceNumber = kv.sequenceNumber();
        currentKey = kv.key();
        InternalRow value = kv.value();
        for (int i = 0; i < getters.length; i++) {
            if (nestTableFieldGetter.containsKey(i)) {
                // nest table
                BinaryArray nestTable = (BinaryArray) getters[i].getFieldOrNull(value);
                Long sequence = getSequence(kv, i);
                if (nestTable != null) {
                    // sort by sequence
                    nestTableContainer
                            .computeIfAbsent(i, ignore -> new NestTable())
                            .addRow(new NestTableRow(sequence, nestTable));
                }
            } else {
                Object field = getters[i].getFieldOrNull(value);
                if (field != null) {
                    mergedRow.setField(i, field);
                }
            }
        }
    }

    private Long getSequence(KeyValue kv, int fieldIndex) {
        SequenceGenerator sequenceGenerator = fieldSequences.get(fieldIndex);
        if (sequenceGenerator != null) {
            return sequenceGenerator.generateNullable(kv.value());
        } else {
            return kv.sequenceNumber();
        }
    }

    @Nullable
    @Override
    public KeyValue getResult() {
        if (reused == null) {
            reused = new KeyValue();
        }

        for (Map.Entry<Integer, NestTable> indexAndNestTable : nestTableContainer.entrySet()) {
            NestTable nestTable = indexAndNestTable.getValue();
            Integer fieldIndex = indexAndNestTable.getKey();

            PriorityQueue<NestTableRow> rows = nestTable.rows;
            RowType rowType = nestTableFieldRowType.get(fieldIndex);
            InternalRow[] mergedRowArray = mergeRowArray(nestTable.length, rows, rowType);

            mergedRow.setField(fieldIndex, new GenericArray(mergedRowArray));
        }

        return reused.replace(currentKey, latestSequenceNumber, RowKind.INSERT, mergedRow);
    }

    private static InternalRow[] mergeRowArray(
            int length, PriorityQueue<NestTableRow> nestTableRows, RowType rowType) {
        InternalRow[] mergedRowArray = new InternalRow[length];
        int offset = 0;
        while (!nestTableRows.isEmpty()) {
            NestTableRow nestTableRow = nestTableRows.poll();
            InternalRow[] rows = nestTableRow.rows.toObjectArray(rowType);
            System.arraycopy(rows, 0, mergedRowArray, offset, rows.length);
            offset += rows.length;
        }
        return mergedRowArray;
    }

    /** nest row. */
    private class NestTable {
        private final PriorityQueue<NestTableRow> rows;
        private int length;

        public NestTable() {
            this.rows =
                    new PriorityQueue<>(
                            Comparator.comparing(
                                    NestTableRow::sequence,
                                    Comparator.nullsFirst(Long::compareTo)));
        }

        public void addRow(NestTableRow row) {
            rows.add(row);
            length += row.rowNum();
        }
    }

    /** the row of Nest table. */
    private class NestTableRow {
        private final Long sequence;
        private final BinaryArray rows;

        public Long sequence() {
            return sequence;
        }

        public NestTableRow(@Nullable Long sequence, BinaryArray rows) {
            this.sequence = sequence;
            this.rows = rows;
        }

        int rowNum() {
            return rows.size();
        }
    }

    /** A Factory class for NestTableMergeFunction. */
    public static class Factory implements MergeFunctionFactory<KeyValue> {
        private boolean ignoreDelete;

        private List<DataType> tableTypes;

        private final Map<Integer, SequenceGenerator> fieldSequences;
        private final Map<Integer, InternalRow.FieldGetter[]> nestTableFieldGetters;

        private final Map<Integer, Projection> nestTableKeyProject;

        private final Map<Integer, RowType> nestTableFieldRowType;

        public Factory(Options options, RowType rowType) {
            this.ignoreDelete = options.get(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE);
            this.tableTypes = rowType.getFieldTypes();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSequences = new HashMap<>();
            this.nestTableFieldGetters = new HashMap<>();
            this.nestTableFieldRowType = new HashMap<>();
            this.nestTableKeyProject = new HashMap<>();
            for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                    String sequenceFieldName =
                            k.substring(
                                    FIELDS_PREFIX.length() + 1,
                                    k.length() - SEQUENCE_GROUP.length() - 1);
                    SequenceGenerator sequenceGen =
                            new SequenceGenerator(sequenceFieldName, rowType);
                    Arrays.stream(v.split(","))
                            .map(
                                    fieldName -> {
                                        int field = fieldNames.indexOf(fieldName);
                                        if (field == -1) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s can not be found in table schema",
                                                            fieldName));
                                        }
                                        return field;
                                    })
                            .forEach(
                                    field -> {
                                        if (fieldSequences.containsKey(field)) {
                                            throw new IllegalArgumentException(
                                                    String.format(
                                                            "Field %s is defined repeatedly by multiple groups: %s",
                                                            fieldNames.get(field), k));
                                        }
                                        fieldSequences.put(field, sequenceGen);
                                    });

                    // add self
                    fieldSequences.put(sequenceGen.index(), sequenceGen);
                }

                if (k.startsWith(FIELDS_PREFIX) && k.endsWith(AGG_FUNC)) {
                    String nestTableField =
                            k.substring(
                                    FIELDS_PREFIX.length() + 1, k.length() - AGG_FUNC.length() - 1);

                    int fieldIndex;
                    if ((fieldIndex = fieldNames.indexOf(nestTableField)) != -1) {
                        DataType dataType = rowType.getTypeAt(fieldIndex);
                        RowType nestTableTypeInfo = getNestTableTypeInfo(dataType);
                        if (NESTED_UPDATE.equals(v)) {
                            //                            int[] keyProject = getKeyProject(options,
                            // k, nestTableTypeInfo);
                        }
                        nestTableFieldRowType.put(fieldIndex, nestTableTypeInfo);
                        nestTableFieldGetters.put(
                                fieldIndex, createFieldGetters(nestTableTypeInfo.getFieldTypes()));
                    }
                }
            }
        }

        private static int[] getKeyProject(Options options, String k, RowType nestTableTypeInfo) {
            int[] projectionArray = null;

            Set<String> keyName =
                    Arrays.stream(options.get(String.format("%s.%s", k, "nested-key")).split(","))
                            .collect(Collectors.toSet());
            List<Integer> idx = new ArrayList<>();
            List<String> nestTableFieldName = nestTableTypeInfo.getFieldNames();
            for (int i = 0; i < nestTableFieldName.size(); i++) {
                if (keyName.contains(nestTableFieldName.get(i))) {
                    idx.add(i);
                }
            }

            if (!nestTableFieldName.isEmpty()) {
                projectionArray = idx.stream().mapToInt(i -> i).toArray();
            }
            return projectionArray;
        }

        private RowType getNestTableTypeInfo(DataType dataType) {
            if ((dataType instanceof ArrayType)) {
                DataType elementType = ((ArrayType) dataType).getElementType();
                if (elementType instanceof RowType) {
                    return (RowType) elementType;
                }
            }
            throw new RuntimeException("the datatype of nest-table field should be Array<Row> ");
        }

        @Override
        public AdjustedProjection adjustProjection(@Nullable int[][] projection) {
            if (fieldSequences.isEmpty()) {
                return new AdjustedProjection(projection, null);
            }

            if (projection == null) {
                return new AdjustedProjection(null, null);
            }
            LinkedHashSet<Integer> extraFields = new LinkedHashSet<>();
            int[] topProjects = Projection.of(projection).toTopLevelIndexes();
            Set<Integer> indexSet = Arrays.stream(topProjects).boxed().collect(Collectors.toSet());
            for (int index : topProjects) {
                SequenceGenerator generator = fieldSequences.get(index);
                if (generator != null && !indexSet.contains(generator.index())) {
                    extraFields.add(generator.index());
                }
            }

            int[] allProjects =
                    Stream.concat(Arrays.stream(topProjects).boxed(), extraFields.stream())
                            .mapToInt(Integer::intValue)
                            .toArray();

            int[][] pushdown = Projection.of(allProjects).toNestedIndexes();
            int[][] outer =
                    Projection.of(IntStream.range(0, topProjects.length).toArray())
                            .toNestedIndexes();
            return new AdjustedProjection(pushdown, outer);
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            if (projection != null) {
                Map<Integer, SequenceGenerator> projectedSequences = new HashMap<>();
                int[] projects = Projection.of(projection).toTopLevelIndexes();
                Map<Integer, Integer> indexMap = new HashMap<>();
                for (int i = 0; i < projects.length; i++) {
                    indexMap.put(projects[i], i);
                }
                fieldSequences.forEach(
                        (field, sequence) -> {
                            int newField = indexMap.getOrDefault(field, -1);
                            if (newField != -1) {
                                int newSequenceId = indexMap.getOrDefault(sequence.index(), -1);
                                if (newSequenceId == -1) {
                                    throw new RuntimeException(
                                            String.format(
                                                    "Can not find new sequence field for new field. new field index is %s",
                                                    newField));
                                } else {
                                    projectedSequences.put(
                                            newField,
                                            new SequenceGenerator(
                                                    newSequenceId, sequence.fieldType()));
                                }
                            }
                        });

                return new NestTableMergeFunction(
                        ignoreDelete,
                        nestTableFieldGetters,
                        nestTableFieldRowType,
                        createFieldGetters(Projection.of(projection).project(tableTypes)),
                        projectedSequences);
            } else {
                return new NestTableMergeFunction(
                        ignoreDelete,
                        nestTableFieldGetters,
                        nestTableFieldRowType,
                        createFieldGetters(tableTypes),
                        fieldSequences);
            }
        }
    }

    public static MergeFunctionFactory<KeyValue> factory(Options options, RowType rowType) {
        return new NestTableMergeFunction.Factory(options, rowType);
    }
}
