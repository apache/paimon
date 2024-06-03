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
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final Map<Integer, SequenceGenerator> fieldSequences;
    private final boolean fieldSequenceEnabled;
    private final Map<Integer, FieldAggregator> fieldAggregators;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private GenericRow row;
    private KeyValue reused;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            Map<Integer, SequenceGenerator> fieldSequences,
            Map<Integer, FieldAggregator> fieldAggregators,
            boolean fieldSequenceEnabled) {
        this.getters = getters;
        this.fieldSequences = fieldSequences;
        this.fieldAggregators = fieldAggregators;
        this.fieldSequenceEnabled = fieldSequenceEnabled;
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

        if (kv.valueKind().isRetract()) {
            if (fieldSequenceEnabled) {
                retractWithSequenceGroup(kv);
                return;
            }

            String msg =
                    String.join(
                            "\n",
                            "By default, Partial update can not accept delete records,"
                                    + " you can choose one of the following solutions:",
                            "1. Configure 'ignore-delete' to ignore delete records.",
                            "2. Configure 'sequence-group's to retract partial columns.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        if (fieldSequences.isEmpty()) {
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
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            FieldAggregator aggregator = fieldAggregators.get(i);
            Object accumulator = getters[i].getFieldOrNull(row);
            if (sequenceGen == null) {
                if (aggregator != null) {
                    row.setField(i, aggregator.agg(accumulator, field));
                } else if (field != null) {
                    row.setField(i, field);
                }
            } else {
                Long currentSeq = sequenceGen.generate(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generate(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        row.setField(
                                i, aggregator == null ? field : aggregator.agg(accumulator, field));
                    } else if (aggregator != null) {
                        row.setField(i, aggregator.agg(field, accumulator));
                    }
                }
            }
        }
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {
            SequenceGenerator sequenceGen = fieldSequences.get(i);
            if (sequenceGen != null) {
                Long currentSeq = sequenceGen.generate(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generate(row);
                    FieldAggregator aggregator = fieldAggregators.get(i);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        if (sequenceGen.index() == i) {
                            // update sequence field
                            row.setField(i, getters[i].getFieldOrNull(kv.value()));
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
                                                accumulator,
                                                getters[i].getFieldOrNull(kv.value())));
                            }
                        }
                    } else if (aggregator != null) {
                        // retract agg field for old sequence
                        Object accumulator = getters[i].getFieldOrNull(row);
                        row.setField(
                                i,
                                aggregator.retract(
                                        accumulator, getters[i].getFieldOrNull(kv.value())));
                    }
                }
            }
        }
    }

    @Override
    public KeyValue getResult() {
        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(currentKey, latestSequenceNumber, RowKind.INSERT, row);
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options options, RowType rowType, List<String> primaryKeys) {
        return new Factory(options, rowType, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final List<DataType> tableTypes;
        private final Map<Integer, SequenceGenerator> fieldSequences;

        private final Map<Integer, FieldAggregator> fieldAggregators;

        private Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.tableTypes = rowType.getFieldTypes();

            List<String> fieldNames = rowType.getFieldNames();
            this.fieldSequences = new HashMap<>();
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
            }
            this.fieldAggregators =
                    createFieldAggregators(rowType, primaryKeys, new CoreOptions(options));
            if (fieldAggregators.size() > 0 && fieldSequences.isEmpty()) {
                throw new IllegalArgumentException(
                        "Must use sequence group for aggregation functions.");
            }
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            if (projection != null) {
                Map<Integer, SequenceGenerator> projectedSequences = new HashMap<>();
                Map<Integer, FieldAggregator> projectedAggregators = new HashMap<>();
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
                for (int i = 0; i < projects.length; i++) {
                    if (fieldAggregators.containsKey(projects[i])) {
                        projectedAggregators.put(i, fieldAggregators.get(projects[i]));
                    }
                }

                return new PartialUpdateMergeFunction(
                        createFieldGetters(Projection.of(projection).project(tableTypes)),
                        projectedSequences,
                        projectedAggregators,
                        !fieldSequences.isEmpty());
            } else {
                return new PartialUpdateMergeFunction(
                        createFieldGetters(tableTypes),
                        fieldSequences,
                        fieldAggregators,
                        !fieldSequences.isEmpty());
            }
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

        /**
         * Creating aggregation function for the columns.
         *
         * @return The aggregators for each column.
         */
        private Map<Integer, FieldAggregator> createFieldAggregators(
                RowType rowType, List<String> primaryKeys, CoreOptions options) {

            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            Map<Integer, FieldAggregator> fieldAggregators = new HashMap<>();
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
                            FieldAggregator.createFieldAggregator(
                                    fieldType,
                                    strAggFunc,
                                    ignoreRetract,
                                    isPrimaryKey,
                                    options,
                                    fieldName));
                } else if (defaultAggFunc != null) {
                    fieldAggregators.put(
                            i,
                            FieldAggregator.createFieldAggregator(
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

    private static class SequenceGenerator {

        private final int index;

        private final Generator generator;
        private final DataType fieldType;

        private SequenceGenerator(String field, RowType rowType) {
            index = rowType.getFieldNames().indexOf(field);
            if (index == -1) {
                throw new RuntimeException(
                        String.format(
                                "Can not find sequence field %s in table schema: %s",
                                field, rowType));
            }
            fieldType = rowType.getTypeAt(index);
            generator = fieldType.accept(new SequenceGeneratorVisitor());
        }

        public SequenceGenerator(int index, DataType dataType) {
            this.index = index;

            this.fieldType = dataType;
            if (index == -1) {
                throw new RuntimeException(String.format("Index : %s is invalid", index));
            }
            generator = fieldType.accept(new SequenceGeneratorVisitor());
        }

        public int index() {
            return index;
        }

        public DataType fieldType() {
            return fieldType;
        }

        @Nullable
        public Long generate(InternalRow row) {
            return generator.generateNullable(row, index);
        }

        private interface Generator {
            long generate(InternalRow row, int i);

            @Nullable
            default Long generateNullable(InternalRow row, int i) {
                if (row.isNullAt(i)) {
                    return null;
                }
                return generate(row, i);
            }
        }

        private static class SequenceGeneratorVisitor extends DataTypeDefaultVisitor<Generator> {

            @Override
            public Generator visit(CharType charType) {
                return stringGenerator();
            }

            @Override
            public Generator visit(VarCharType varCharType) {
                return stringGenerator();
            }

            private Generator stringGenerator() {
                return (row, i) -> Long.parseLong(row.getString(i).toString());
            }

            @Override
            public Generator visit(DecimalType decimalType) {
                return (row, i) ->
                        InternalRowUtils.castToIntegral(
                                row.getDecimal(
                                        i, decimalType.getPrecision(), decimalType.getScale()));
            }

            @Override
            public Generator visit(TinyIntType tinyIntType) {
                return InternalRow::getByte;
            }

            @Override
            public Generator visit(SmallIntType smallIntType) {
                return InternalRow::getShort;
            }

            @Override
            public Generator visit(IntType intType) {
                return InternalRow::getInt;
            }

            @Override
            public Generator visit(BigIntType bigIntType) {
                return InternalRow::getLong;
            }

            @Override
            public Generator visit(FloatType floatType) {
                return (row, i) -> (long) row.getFloat(i);
            }

            @Override
            public Generator visit(DoubleType doubleType) {
                return (row, i) -> (long) row.getDouble(i);
            }

            @Override
            public Generator visit(DateType dateType) {
                return InternalRow::getInt;
            }

            @Override
            public Generator visit(TimestampType timestampType) {
                return (row, i) ->
                        row.getTimestamp(i, timestampType.getPrecision()).getMillisecond();
            }

            @Override
            public Generator visit(LocalZonedTimestampType localZonedTimestampType) {
                return (row, i) ->
                        row.getTimestamp(i, localZonedTimestampType.getPrecision())
                                .getMillisecond();
            }

            @Override
            protected Generator defaultMethod(DataType dataType) {
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
            }
        }
    }
}
