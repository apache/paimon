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
import org.apache.paimon.mergetree.SequenceGenerator;
import org.apache.paimon.mergetree.SequenceGenerator.Seq;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.mergetree.compact.MergeFunctionUtils.validateSequence;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    public static final String SEQUENCE_GROUP = "sequence-group";

    private final InternalRow.FieldGetter[] getters;
    private final boolean ignoreDelete;
    private final FieldAggregator[] fieldAggregators;
    private final Seq[] sequences;
    private final boolean fieldSequenceEnabled;

    private InternalRow currentKey;
    private long latestSequenceNumber;
    private boolean isEmpty;
    private GenericRow row;
    private KeyValue reused;

    protected PartialUpdateMergeFunction(
            InternalRow.FieldGetter[] getters,
            boolean ignoreDelete,
            Seq[] fieldSequences,
            FieldAggregator[] fieldAggregators,
            boolean fieldSequenceEnabled) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
        this.fieldAggregators = fieldAggregators;
        this.sequences = fieldSequences;
        this.fieldSequenceEnabled = fieldSequenceEnabled;
    }

    @Override
    public void reset() {
        this.currentKey = null;
        this.row = new GenericRow(getters.length);
        Arrays.stream(fieldAggregators).filter(Objects::nonNull).forEach(FieldAggregator::reset);
        this.isEmpty = true;
    }

    @Override
    public void add(KeyValue kv) {
        // refresh key object to avoid reference overwritten
        currentKey = kv.key();

        // ignore delete?
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            }

            if (fieldSequenceEnabled) {
                retractWithSequenceGroup(kv);
                return;
            }

            String msg =
                    String.join(
                            "\n",
                            "By default, Partial update can not accept delete records,"
                                    + " you can choose one of the following solutions:",
                            "1. Configure 'partial-update.ignore-delete' to ignore delete records.",
                            "2. Configure 'sequence-group's to retract partial columns.");

            throw new IllegalArgumentException(msg);
        }

        latestSequenceNumber = kv.sequenceNumber();
        isEmpty = false;
        if (sequences.length == 0) {
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

            @Nullable Seq sequence = sequences[i];
            @Nullable SequenceGenerator sequenceGen = sequence == null ? null : sequence.generator;
            @Nullable FieldAggregator aggregator = fieldAggregators[i];

            // handle the sequence field.
            if (sequence != null && sequence.generator != null && sequence.generator.index() == i) {
                if (!sequence.isExclusive) {
                    Long previousSeq = sequence.generator.generate(row);
                    Long currenSeq = sequence.generator.generate(kv.value());
                    if (currenSeq != null) {
                        if (previousSeq == null || currenSeq > previousSeq) {
                            row.setField(i, getters[i].getFieldOrNull(kv.value()));
                        }
                    }
                }

                continue;
            }

            Object field = getters[i].getFieldOrNull(kv.value());
            Object accumulator = getters[i].getFieldOrNull(row);
            if (sequenceGen == null) {
                if (aggregator != null) {
                    row.setField(i, aggregator.agg(accumulator, field, null));
                } else if (field != null) {
                    row.setField(i, field);
                }
            } else {
                Long currentSeq = sequenceGen.generate(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generate(row);
                    if (previousSeq == null || currentSeq >= previousSeq) {
                        row.setField(
                                i,
                                aggregator == null
                                        ? field
                                        : aggregator.agg(
                                                accumulator,
                                                field,
                                                getters[sequenceGen.index()].getFieldOrNull(
                                                        kv.value())));
                    } else if (aggregator != null) {
                        row.setField(
                                i,
                                aggregator.aggForOldSeq(
                                        accumulator,
                                        field,
                                        getters[sequenceGen.index()].getFieldOrNull(kv.value())));
                    }
                }
                // UPDATE The exclusive sequence field.
                if (aggregator != null
                        && aggregator.requireSequence()
                        && aggregator.getSeq() != null) {
                    row.setField(sequenceGen.index(), aggregator.getSeq());
                }
            }
        }
    }

    private void retractWithSequenceGroup(KeyValue kv) {
        for (int i = 0; i < getters.length; i++) {

            @Nullable Seq sequence = sequences[i];
            @Nullable SequenceGenerator sequenceGen = sequence == null ? null : sequence.generator;
            @Nullable FieldAggregator aggregator = fieldAggregators[i];

            // handle the sequence field.
            if (sequence != null && sequence.generator != null && sequence.generator.index() == i) {
                if (!sequence.isExclusive) {
                    Long previousSeq = sequence.generator.generate(row);
                    Long currenSeq = sequence.generator.generate(kv.value());
                    if (currenSeq != null) {
                        if (previousSeq == null || currenSeq > previousSeq) {
                            row.setField(i, getters[i].getFieldOrNull(kv.value()));
                        }
                    }
                }

                continue;
            }

            if (sequenceGen != null) {
                Long currentSeq = sequenceGen.generate(kv.value());
                if (currentSeq != null) {
                    Long previousSeq = sequenceGen.generate(row);
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
                                                getters[i].getFieldOrNull(kv.value()),
                                                getters[sequenceGen.index()].getFieldOrNull(
                                                        kv.value())));
                            }
                        }
                    } else if (aggregator != null) {
                        // retract agg field for old sequence
                        Object accumulator = getters[i].getFieldOrNull(row);
                        row.setField(
                                i,
                                aggregator.retract(
                                        accumulator,
                                        getters[i].getFieldOrNull(kv.value()),
                                        getters[sequenceGen.index()].getFieldOrNull(kv.value())));
                    }
                }
                // UPDATE The exclusive sequence field.
                if (aggregator != null
                        && aggregator.requireSequence()
                        && aggregator.getSeq() != null) {
                    row.setField(sequenceGen.index(), aggregator.getSeq());
                }
            }
        }
    }

    @Override
    @Nullable
    public KeyValue getResult() {
        if (isEmpty) {
            return null;
        }

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

        private final boolean ignoreDelete;
        private final List<DataType> tableTypes;
        private final Map<Integer, SequenceGenerator> fieldSequences;
        private final List<String> fieldNames;

        private final FieldAggregator[] fieldAggregators;

        private Factory(Options options, RowType rowType, List<String> primaryKeys) {
            this.ignoreDelete = options.get(CoreOptions.PARTIAL_UPDATE_IGNORE_DELETE);
            this.tableTypes = rowType.getFieldTypes();

            this.fieldNames = rowType.getFieldNames();
            this.fieldSequences =
                    MergeFunctionUtils.getSequenceGenerator(
                            new CoreOptions(options), fieldNames, rowType);
            this.fieldAggregators =
                    createFieldAggregators(rowType, primaryKeys, new CoreOptions(options));
            if (Arrays.stream(fieldAggregators).anyMatch(Objects::nonNull)
                    && fieldSequences.isEmpty()) {
                throw new IllegalArgumentException(
                        "Must use sequence group for aggregation functions.");
            }
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            Map<Integer, SequenceGenerator> generators = fieldSequences;
            FieldAggregator[] aggregators = fieldAggregators;
            InternalRow.FieldGetter[] getters = createFieldGetters(tableTypes);
            List<String> fieldNames = this.fieldNames;
            if (projection != null) {
                Projection project = Projection.of(projection);
                fieldNames = project.project(fieldNames);
                generators = MergeFunctionUtils.projectSequence(projection, fieldSequences);
                getters = createFieldGetters(Projection.of(projection).project(tableTypes));
                int[] projects = Projection.of(projection).toTopLevelIndexes();
                aggregators = new FieldAggregator[projects.length];
                for (int i = 0; i < projects.length; i++) {
                    if (fieldAggregators[projects[i]] != null) {
                        aggregators[i] = fieldAggregators[projects[i]];
                    }
                }
            }

            validateSequence(aggregators, generators, fieldNames);
            return new PartialUpdateMergeFunction(
                    getters,
                    ignoreDelete,
                    MergeFunctionUtils.toSeq(generators, aggregators),
                    aggregators,
                    !generators.isEmpty());
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
        private FieldAggregator[] createFieldAggregators(
                RowType rowType, List<String> primaryKeys, CoreOptions options) {

            List<String> fieldNames = rowType.getFieldNames();
            List<DataType> fieldTypes = rowType.getFieldTypes();
            FieldAggregator[] fieldAggregators = new FieldAggregator[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc = options.fieldAggFunc(fieldName);
                boolean ignoreRetract = options.fieldAggIgnoreRetract(fieldName);

                if (strAggFunc != null) {
                    fieldAggregators[i] =
                            FieldAggregator.createFieldAggregator(
                                    fieldType,
                                    strAggFunc,
                                    ignoreRetract,
                                    isPrimaryKey,
                                    options,
                                    fieldName);
                }
            }
            return fieldAggregators;
        }
    }
}
