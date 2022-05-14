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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationFunctionFactory;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.aggregation.UnsupportedAggregationTypeException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public class AggregationMergeFunction implements MergeFunction {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] getters;
    private final RowType primaryKeyType;
    private final RowType rowType;
    private final Set<String> primaryKeyNames;
    private final ArrayList<String> rowNames;

    private final ArrayList<AggregationFunction<Object>> types;
    private transient GenericRowData row;

    public AggregationMergeFunction(
            RowData.FieldGetter[] fieldGetters, RowType primaryKeyType, RowType rowType) {
        this.getters = fieldGetters;
        this.primaryKeyType = primaryKeyType;
        this.rowType = rowType;
        this.primaryKeyNames = new HashSet<>(primaryKeyType.getFieldNames());
        this.rowNames = new ArrayList<>(rowType.getFieldNames());
        this.types = new ArrayList<>(rowType.getFieldCount());
        AggregationFunctionFactory factory = Aggregations.SUM.getFactory();
        for (LogicalType type : rowType.getChildren()) {
            try {
                AggregationFunction<Object> f =
                        factory.createAggregationFunction(
                                (Class<Object>) type.getDefaultConversion());
                types.add(f);
            } catch (UnsupportedAggregationTypeException e) {
                types.add(null);
            }
        }
    }

    @Override
    public void reset() {
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(RowData value) {
        for (int i = 0; i < getters.length; i++) {
            Object currentField = getters[i].getFieldOrNull(value);
            AggregationFunction<Object> f = types.get(i);
            if (primaryKeyNames.contains(rowNames.get(i))) {
                // primary key
                if (currentField != null) {
                    row.setField(i, currentField);
                }
            } else {
                if (f != null) {
                    f.initializeAggregate();
                    Object oldValue = row.getField(i);
                    if (oldValue != null) {
                        f.aggregate(oldValue);
                    }
                    switch (row.getRowKind()) {
                        case INSERT:
                            f.aggregate(currentField);
                            break;
                        case DELETE:
                        case UPDATE_AFTER:
                        case UPDATE_BEFORE:
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported row kind: " + row.getRowKind());
                    }
                    Object result = f.getAggregate();
                    if (result != null) {
                        row.setField(i, result);
                    }
                }
            }
        }
    }

    @Override
    @Nullable
    public RowData getValue() {
        return row;
    }

    @Override
    public MergeFunction copy() {
        // RowData.FieldGetter is thread safe
        return new AggregationMergeFunction(getters, primaryKeyType, rowType);
    }
}
