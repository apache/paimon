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
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * pre-aggregate non-null fields on merge.
 */
public class AggregateMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRow.FieldGetter[] getters;
    private final FieldAggregator[] aggregators;

    private KeyValue latestKv;
    private GenericRow row;
    private KeyValue reused;

    public AggregateMergeFunction(
            InternalRow.FieldGetter[] getters, FieldAggregator[] aggregators) {
        this.getters = getters;
        this.aggregators = aggregators;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRow(getters.length);
        Arrays.stream(aggregators).forEach(FieldAggregator::reset);
    }

    @Override
    public void add(KeyValue kv) {
        latestKv = kv;
        boolean isRetract =
                kv.valueKind() != RowKind.INSERT && kv.valueKind() != RowKind.UPDATE_AFTER;
        for (int i = 0; i < getters.length; i++) {
            FieldAggregator fieldAggregator = aggregators[i];
            Object accumulator = getters[i].getFieldOrNull(row);
            Object inputField = getters[i].getFieldOrNull(kv.value());
            Object mergedField =
                    isRetract
                            ? fieldAggregator.retract(accumulator, inputField)
                            : fieldAggregator.agg(accumulator, inputField);
            row.setField(i, mergedField);
        }
    }

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
            Options conf,
            List<String> tableNames,
            List<DataType> tableTypes,
            List<String> primaryKeys) {
        return new Factory(conf, tableNames, tableTypes, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final CoreOptions options;
        private final List<String> tableNames;
        private final List<DataType> tableTypes;
        private final List<String> primaryKeys;

        private Factory(
                Options conf,
                List<String> tableNames,
                List<DataType> tableTypes,
                List<String> primaryKeys) {
            this.options = new CoreOptions(conf);
            this.tableNames = tableNames;
            this.tableTypes = tableTypes;
            this.primaryKeys = primaryKeys;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            List<String> fieldNames = tableNames;
            List<DataType> fieldTypes = tableTypes;
            if (projection != null) {
                Projection project = Projection.of(projection);
                fieldNames = project.project(tableNames);
                fieldTypes = project.project(tableTypes);
            }

            FieldAggregator[] fieldAggregators = new FieldAggregator[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc = options.fieldAggFunc(fieldName);
                boolean ignoreRetract = options.fieldAggIgnoreRetract(fieldName);
                fieldAggregators[i] =
                        FieldAggregator.createFieldAggregator(
                                fieldType,
                                strAggFunc,
                                ignoreRetract,
                                isPrimaryKey,
                                options,
                                fieldName);
            }

            return new AggregateMergeFunction(createFieldGetters(fieldTypes), fieldAggregators);
        }
    }
}
