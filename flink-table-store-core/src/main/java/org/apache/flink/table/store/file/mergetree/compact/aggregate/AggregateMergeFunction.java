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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.MergeFunctionFactory;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.store.utils.RowDataUtils.createFieldGetters;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * pre-aggregate non-null fields on merge.
 */
public class AggregateMergeFunction implements MergeFunction<KeyValue> {

    private final RowData.FieldGetter[] getters;
    private final RowAggregator rowAggregator;

    private KeyValue latestKv;
    private GenericRowData row;
    private KeyValue reused;

    protected AggregateMergeFunction(RowData.FieldGetter[] getters, RowAggregator rowAggregator) {
        this.getters = getters;
        this.rowAggregator = rowAggregator;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(KeyValue kv) {
        checkArgument(
                kv.valueKind() == RowKind.INSERT || kv.valueKind() == RowKind.UPDATE_AFTER,
                "Pre-aggregate can not accept delete records!");
        latestKv = kv;
        for (int i = 0; i < getters.length; i++) {
            FieldAggregator fieldAggregator = rowAggregator.getFieldAggregatorAtPos(i);
            Object accumulator = getters[i].getFieldOrNull(row);
            Object inputField = getters[i].getFieldOrNull(kv.value());
            Object mergedField = fieldAggregator.agg(accumulator, inputField);
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

        private final FieldAggregator[] fieldAggregators;

        public RowAggregator(
                Configuration sqlConf,
                List<String> fieldNames,
                List<LogicalType> fieldTypes,
                List<String> primaryKeys) {
            fieldAggregators = new FieldAggregator[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                LogicalType fieldType = fieldTypes.get(i);
                // aggregate by primary keys, so they do not aggregate
                boolean isPrimaryKey = primaryKeys.contains(fieldName);
                String strAggFunc =
                        sqlConf.getString(
                                key(FIELDS + "." + fieldName + "." + AGG_FUNCTION)
                                        .stringType()
                                        .noDefaultValue()
                                        .withDescription(
                                                "Get " + fieldName + "'s aggregate function"));
                fieldAggregators[i] =
                        FieldAggregator.createFieldAggregator(fieldType, strAggFunc, isPrimaryKey);
            }
        }

        public FieldAggregator getFieldAggregatorAtPos(int fieldPos) {
            return fieldAggregators[fieldPos];
        }
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Configuration conf,
            List<String> tableNames,
            List<LogicalType> tableTypes,
            List<String> primaryKeys) {
        return new Factory(conf, tableNames, tableTypes, primaryKeys);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final Configuration conf;
        private final List<String> tableNames;
        private final List<LogicalType> tableTypes;
        private final List<String> primaryKeys;

        private Factory(
                Configuration conf,
                List<String> tableNames,
                List<LogicalType> tableTypes,
                List<String> primaryKeys) {
            this.conf = conf;
            this.tableNames = tableNames;
            this.tableTypes = tableTypes;
            this.primaryKeys = primaryKeys;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            List<String> fieldNames = tableNames;
            List<LogicalType> fieldTypes = tableTypes;
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
