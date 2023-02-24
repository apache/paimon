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

package org.apache.flink.table.store.file.mergetree.compact.aggregate;

import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.Projection;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.table.store.options.ConfigOptions.key;
import static org.apache.flink.table.store.utils.Preconditions.checkNotNull;
import static org.apache.flink.table.store.utils.RowDataUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * pre-aggregate non-null fields on merge.
 */
public class AggregateMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRow.FieldGetter[] getters;
    private final RowAggregator rowAggregator;

    private KeyValue latestKv;
    private GenericRow row;
    private KeyValue reused;

    protected AggregateMergeFunction(
            InternalRow.FieldGetter[] getters, RowAggregator rowAggregator) {
        this.getters = getters;
        this.rowAggregator = rowAggregator;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRow(getters.length);
    }

    @Override
    public void add(KeyValue kv) {
        latestKv = kv;
        boolean isRetract =
                kv.valueKind() != RowKind.INSERT && kv.valueKind() != RowKind.UPDATE_AFTER;
        for (int i = 0; i < getters.length; i++) {
            FieldAggregator fieldAggregator = rowAggregator.getFieldAggregatorAtPos(i);
            Object accumulator = getters[i].getFieldOrNull(row);
            Object inputField = getters[i].getFieldOrNull(kv.value());
            Object mergedField =
                    isRetract
                            ? fieldAggregator.retract(accumulator, inputField)
                            : fieldAggregator.agg(accumulator, inputField);
            row.setField(i, mergedField);
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

    /** Provide an Aggregator for merge a new row data. */
    public static class RowAggregator implements Serializable {
        public static final String FIELDS = "fields";
        public static final String AGG_FUNCTION = "aggregate-function";
        public static final String IGNORE_RETRACT = "ignore-retract";

        private final FieldAggregator[] fieldAggregators;

        public RowAggregator(
                Options sqlConf,
                List<String> fieldNames,
                List<DataType> fieldTypes,
                List<String> primaryKeys) {
            fieldAggregators = new FieldAggregator[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc =
                        sqlConf.get(
                                key(FIELDS + "." + fieldName + "." + AGG_FUNCTION)
                                        .stringType()
                                        .noDefaultValue()
                                        .withDescription(
                                                "Get " + fieldName + "'s aggregate function"));
                boolean ignoreRetract =
                        sqlConf.get(
                                key(FIELDS + "." + fieldName + "." + IGNORE_RETRACT)
                                        .booleanType()
                                        .defaultValue(false));
                fieldAggregators[i] =
                        FieldAggregator.createFieldAggregator(
                                fieldType, strAggFunc, ignoreRetract, isPrimaryKey);
            }
        }

        public FieldAggregator getFieldAggregatorAtPos(int fieldPos) {
            return fieldAggregators[fieldPos];
        }
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

        private final Options conf;
        private final List<String> tableNames;
        private final List<DataType> tableTypes;
        private final List<String> primaryKeys;

        private Factory(
                Options conf,
                List<String> tableNames,
                List<DataType> tableTypes,
                List<String> primaryKeys) {
            this.conf = conf;
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

            return new AggregateMergeFunction(
                    createFieldGetters(fieldTypes),
                    new AggregateMergeFunction.RowAggregator(
                            conf, fieldNames, fieldTypes, primaryKeys));
        }
    }
}
