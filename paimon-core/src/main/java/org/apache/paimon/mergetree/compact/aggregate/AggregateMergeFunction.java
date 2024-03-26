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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.SequenceGenerator;
import org.apache.paimon.mergetree.SequenceGenerator.Seq;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionUtils;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.paimon.mergetree.compact.MergeFunctionUtils.validateSequence;
import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * pre-aggregate non-null fields on merge.
 */
public class AggregateMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRow.FieldGetter[] getters;
    /**
     * The aggregator for the fields, if the fields do not contain an aggregation, then its null.
     */
    private final FieldAggregator[] aggregators;

    /** The sequence generator for the fields. */
    private final Seq[] sequences;

    private KeyValue latestKv;
    private GenericRow row;
    private KeyValue reused;

    public AggregateMergeFunction(
            InternalRow.FieldGetter[] getters, FieldAggregator[] aggregators, Seq[] sequences) {
        this.getters = getters;
        this.aggregators = aggregators;
        this.sequences = sequences;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRow(getters.length);
        Arrays.stream(aggregators).filter(Objects::nonNull).forEach(FieldAggregator::reset);
    }

    @Override
    public void add(KeyValue kv) {
        latestKv = kv;
        boolean isRetract =
                kv.valueKind() != RowKind.INSERT && kv.valueKind() != RowKind.UPDATE_AFTER;

        for (int i = 0; i < getters.length; i++) {
            @Nullable FieldAggregator fieldAggregator = aggregators[i];
            @Nullable Seq sequence = sequences[i];

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
            Preconditions.checkNotNull(fieldAggregator, "The fieldAggregator should be present");

            Object accumulator = getters[i].getFieldOrNull(row);
            Object inputField = getters[i].getFieldOrNull(kv.value());
            SequenceGenerator sequenceGenerator = sequence == null ? null : sequence.generator;

            if (sequenceGenerator != null) {
                Long currenSeq = sequenceGenerator.generate(kv.value());
                if (currenSeq != null) {
                    Long previousSeq = sequenceGenerator.generate(row);
                    if (previousSeq == null || currenSeq >= previousSeq) {
                        Object seqField =
                                getters[sequenceGenerator.index()].getFieldOrNull(kv.value());
                        row.setField(
                                i,
                                isRetract
                                        ? fieldAggregator.retract(accumulator, inputField, seqField)
                                        : fieldAggregator.agg(accumulator, inputField, seqField));
                    } else {
                        Object seqField =
                                getters[sequenceGenerator.index()].getFieldOrNull(kv.value());
                        row.setField(
                                i,
                                isRetract
                                        ? fieldAggregator.retract(accumulator, inputField, seqField)
                                        : fieldAggregator.aggForOldSeq(
                                                accumulator, inputField, seqField));
                    }
                    // UPDATE The exclusive sequence field.
                    if (fieldAggregator.requireSequence() && fieldAggregator.getSeq() != null) {
                        row.setField(sequenceGenerator.index(), fieldAggregator.getSeq());
                    }
                }
            } else {
                row.setField(
                        i,
                        isRetract
                                ? fieldAggregator.retract(accumulator, inputField, null)
                                : fieldAggregator.agg(accumulator, inputField, null));
            }
        }
    }

    @Nullable
    @Override
    public KeyValue getResult() {
        checkNotNull(
                latestKv,
                "Trying to get result from merge function without any input. This is unexpected.");

        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(latestKv.key(), latestKv.sequenceNumber(), RowKind.INSERT, row);
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options conf, RowType rowType, List<String> primaryKeys) {
        return new Factory(conf, rowType, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final CoreOptions options;
        private final List<String> tableNames;
        private final List<DataType> tableTypes;
        private final List<String> primaryKeys;
        private final Map<Integer, SequenceGenerator> fieldSequences;

        private Factory(Options conf, RowType rowType, List<String> primaryKeys) {
            this.options = new CoreOptions(conf);
            this.tableNames = rowType.getFieldNames();
            this.tableTypes = rowType.getFieldTypes();
            this.primaryKeys = primaryKeys;
            this.fieldSequences =
                    MergeFunctionUtils.getSequenceGenerator(options, tableNames, rowType);
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            List<String> fieldNames = tableNames;
            List<DataType> fieldTypes = tableTypes;
            Map<Integer, SequenceGenerator> generators = fieldSequences;
            if (projection != null) {
                Projection project = Projection.of(projection);
                fieldNames = project.project(tableNames);
                fieldTypes = project.project(tableTypes);
                generators = MergeFunctionUtils.projectSequence(projection, fieldSequences);
            }

            FieldAggregator[] fieldAggregators = new FieldAggregator[fieldNames.size()];
            Map<Integer, Set<Integer>> seq2Field = MergeFunctionUtils.seqIndex2Field(generators);
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc = options.fieldAggFunc(fieldName);
                boolean ignoreRetract = options.fieldAggIgnoreRetract(fieldName);
                // If the field is a sequence field, do not use aggregation.
                fieldAggregators[i] =
                        seq2Field.containsKey(i)
                                ? null
                                : FieldAggregator.createFieldAggregator(
                                        fieldType,
                                        strAggFunc,
                                        ignoreRetract,
                                        isPrimaryKey,
                                        options,
                                        fieldName);
            }
            validateSequence(fieldAggregators, generators, fieldNames);

            return new AggregateMergeFunction(
                    createFieldGetters(fieldTypes),
                    fieldAggregators,
                    MergeFunctionUtils.toSeq(generators, fieldAggregators));
        }
    }
}
