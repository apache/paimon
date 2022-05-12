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
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
@SuppressWarnings("checkstyle:RegexpSingleline")
public class AggregationMergeFunction implements MergeFunction {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] getters;

    private transient GenericRowData row;

    public AggregationMergeFunction(RowData.FieldGetter[] getters) {
        this.getters = getters;
    }

    @Override
    public void reset() {
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(RowData value) {
        for (int i = 0; i < getters.length; i++) {
            Object currentField = getters[i].getFieldOrNull(value);
            Object oldValue = row.getField(i);
            Object result = sum(value.getRowKind(), oldValue, currentField);
            if (result != null) {
                row.setField(i, result);
            }
        }
    }

    private Object sum(RowKind kind, Object oldValue, Object currentField) {
        switch (kind) {
            case DELETE:
                if (oldValue == null) {
                    return null;
                }
                if (currentField == null) {
                    return oldValue;
                }
                if (oldValue instanceof Integer && currentField instanceof Integer) {
                    return Integer.sum((Integer) oldValue, -(Integer) currentField);
                } else if (oldValue instanceof Long && currentField instanceof Long) {
                    return Long.sum((Long) oldValue, -(Long) currentField);
                } else if (oldValue instanceof Double && currentField instanceof Double) {
                    return Double.sum((Double) oldValue, -(Double) currentField);
                } else if (oldValue instanceof Float && currentField instanceof Float) {
                    return Float.sum((Float) oldValue, -(Float) currentField);
                }
                throw new UnsupportedOperationException(
                        "Cannot support " + oldValue + " delete " + currentField);
            case INSERT:
                if (currentField == null) {
                    return null;
                }
                if (oldValue == null) {
                    return currentField;
                }
                if (oldValue instanceof Integer && currentField instanceof Integer) {
                    return Integer.sum((Integer) oldValue, (Integer) currentField);
                } else if (oldValue instanceof Long && currentField instanceof Long) {
                    return Long.sum((Long) oldValue, (Long) currentField);
                } else if (oldValue instanceof Double && currentField instanceof Double) {
                    return Double.sum((Double) oldValue, (Double) currentField);
                } else if (oldValue instanceof Float && currentField instanceof Float) {
                    return Float.sum((Float) oldValue, (Float) currentField);
                }
                throw new UnsupportedOperationException(
                        "Cannot support " + oldValue + " sum " + currentField);
            case UPDATE_AFTER:
            case UPDATE_BEFORE:
            default:
                throw new UnsupportedOperationException("Unsupported row kind: " + kind);
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
        return new AggregationMergeFunction(getters);
    }
}
