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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record,
 * aggregate specifies field on merge.
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public class AggregateMergeFunction implements MergeFunction {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] getters;

    private final RowType rowType;
    private final ArrayList<ColumnAggregateFunction<?>> aggregateFunctions;
    private final boolean[] isPrimaryKey;
    private final RowType primaryKeyType;
    private transient GenericRowData row;
    private final Map<String, AggregationKind> aggregationKindMap;

    public AggregateMergeFunction(
            RowType primaryKeyType,
            RowType rowType,
            Map<String, AggregationKind> aggregationKindMap) {
        this.primaryKeyType = primaryKeyType;
        this.rowType = rowType;
        this.aggregationKindMap = aggregationKindMap;

        List<LogicalType> fieldTypes = rowType.getChildren();
        this.getters = new RowData.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            getters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
        }

        this.isPrimaryKey = new boolean[this.getters.length];
        List<String> rowNames = rowType.getFieldNames();
        for (String primaryKeyName : primaryKeyType.getFieldNames()) {
            isPrimaryKey[rowNames.indexOf(primaryKeyName)] = true;
        }

        this.aggregateFunctions = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            ColumnAggregateFunction<?> f = null;
            if (aggregationKindMap.containsKey(rowNames.get(i))) {
                f =
                        ColumnAggregateFunctionFactory.getColumnAggregateFunction(
                                aggregationKindMap.get(rowNames.get(i)), rowType.getTypeAt(i));
            } else {
                if (!isPrimaryKey[i]) {
                    throw new IllegalArgumentException(
                            "should  set aggregate function for every column not part of primary key");
                }
            }
            // PrimaryKey column 's aggregateFunction is null
            // Any other column must have aggregateFunction
            aggregateFunctions.add(f);
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
            ColumnAggregateFunction<?> f = aggregateFunctions.get(i);
            if (isPrimaryKey[i]) {
                // primary key
                if (currentField != null) {
                    row.setField(i, currentField);
                }
            } else {
                f.reset();
                Object oldValue = row.getField(i);
                if (oldValue != null) {
                    f.aggregate(oldValue);
                }
                switch (value.getRowKind()) {
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
                row.setField(i, f.getResult());
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
        return new AggregateMergeFunction(primaryKeyType, rowType, aggregationKindMap);
    }

    /**
     * used to get the merge function by configuration.
     *
     * @param options the options
     * @param primaryKeyType the primary key type
     * @param rowType the row type
     * @return the merge function
     */
    public static MergeFunction getAggregateFunction(
            FileStoreOptions options, RowType primaryKeyType, RowType rowType) {

        Map<String, String> rightConfMap =
                options.getFilterConf(e -> e.getKey().endsWith(".aggregate-function"));
        Map<String, AggregationKind> aggregationKindMap =
                rightConfMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().split(".aggregate-function")[0],
                                        e -> AggregationKind.valueOf(e.getValue().toUpperCase())));

        return new AggregateMergeFunction(primaryKeyType, rowType, aggregationKindMap);
    }
}
