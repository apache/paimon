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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public class AggregationMergeFunction implements MergeFunction {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] getters;

    private final RowType rowType;
    private final ArrayList<AggregateFunction<?>> aggregateFunctions;
    private final boolean[] isPrimaryKey;
    private final RowType primaryKeyType;
    private transient GenericRowData row;

    private final Set<String> aggregateColumnNames;

    public AggregationMergeFunction(
            RowType primaryKeyType, RowType rowType, Set<String> aggregateColumnNames) {
        this.primaryKeyType = primaryKeyType;
        this.rowType = rowType;
        this.aggregateColumnNames = aggregateColumnNames;

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
            AggregateFunction<?> f = null;
            if (aggregateColumnNames.contains(rowNames.get(i))) {
                f = choiceRightAggregateFunction(rowType.getTypeAt(i).getDefaultConversion());
            }
            aggregateFunctions.add(f);
        }
    }

    private AggregateFunction<?> choiceRightAggregateFunction(Class<?> c) {
        AggregateFunction<?> f = null;
        if (Double.class.equals(c)) {
            f = new DoubleAggregateFunction();
        } else if (Long.class.equals(c)) {
            f = new LongAggregateFunction();
        } else if (Integer.class.equals(c)) {
            f = new IntegerAggregateFunction();
        } else if (Float.class.equals(c)) {
            f = new FloatAggregateFunction();
        }
        return f;
    }

    @Override
    public void reset() {
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(RowData value) {
        for (int i = 0; i < getters.length; i++) {
            Object currentField = getters[i].getFieldOrNull(value);
            AggregateFunction<?> f = aggregateFunctions.get(i);
            if (isPrimaryKey[i]) {
                // primary key
                if (currentField != null) {
                    row.setField(i, currentField);
                }
            } else {
                if (f != null) {
                    f.reset();
                    Object oldValue = row.getField(i);
                    if (oldValue != null) {
                        f.aggregate(oldValue);
                    }
                    switch (row.getRowKind()) {
                        case INSERT:
                            f.aggregate(currentField);
                            break;
                        case DELETE:
                            f.retract(currentField);
                            break;
                        case UPDATE_AFTER:
                        case UPDATE_BEFORE:
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported row kind: " + row.getRowKind());
                    }
                    Object result = f.getResult();
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
        return new AggregationMergeFunction(primaryKeyType, rowType, aggregateColumnNames);
    }
}
