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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.flink.table.store.utils.RowDataUtils.createFieldGetters;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction<KeyValue> {

    private final RowData.FieldGetter[] getters;
    private final boolean ignoreDelete;

    private KeyValue latestKv;
    private GenericRowData row;
    private KeyValue reused;

    protected PartialUpdateMergeFunction(RowData.FieldGetter[] getters, boolean ignoreDelete) {
        this.getters = getters;
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        this.latestKv = null;
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind() == RowKind.UPDATE_BEFORE || kv.valueKind() == RowKind.DELETE) {
            if (ignoreDelete) {
                return;
            }

            if (kv.valueKind() == RowKind.UPDATE_BEFORE) {
                throw new IllegalArgumentException(
                        "Partial update can not accept update_before records, it is a bug.");
            }

            throw new IllegalArgumentException(
                    "Partial update can not accept delete records. Partial delete is not supported!");
        }

        latestKv = kv;
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(kv.value());
            if (field != null) {
                row.setField(i, field);
            }
        }
    }

    @Override
    @Nullable
    public KeyValue getResult() {
        if (latestKv == null) {
            if (ignoreDelete) {
                return null;
            }

            throw new IllegalArgumentException(
                    "Trying to get result from merge function without any input. This is unexpected.");
        }

        if (reused == null) {
            reused = new KeyValue();
        }
        return reused.replace(latestKv.key(), latestKv.sequenceNumber(), RowKind.INSERT, row);
    }

    public static MergeFunctionFactory<KeyValue> factory(
            boolean ignoreDelete, List<LogicalType> tableTypes) {
        return new Factory(ignoreDelete, tableTypes);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;

        private final boolean ignoreDelete;
        private final List<LogicalType> tableTypes;

        private Factory(boolean ignoreDelete, List<LogicalType> tableTypes) {
            this.ignoreDelete = ignoreDelete;
            this.tableTypes = tableTypes;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            List<LogicalType> fieldTypes = tableTypes;
            if (projection != null) {
                fieldTypes = Projection.of(projection).project(tableTypes);
            }
            return new PartialUpdateMergeFunction(createFieldGetters(fieldTypes), ignoreDelete);
        }
    }
}
