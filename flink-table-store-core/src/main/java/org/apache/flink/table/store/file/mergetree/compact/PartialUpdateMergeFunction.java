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

import java.util.List;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the partial record, update
 * non-null fields on merge.
 */
public class PartialUpdateMergeFunction implements MergeFunction {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] getters;

    private final RowType rowType;

    private transient GenericRowData row;

    public PartialUpdateMergeFunction(RowType rowType) {
        List<LogicalType> fieldTypes = rowType.getChildren();
        this.getters = new RowData.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            this.getters[i] = RowData.createFieldGetter(fieldTypes.get(i), i);
        }
        this.rowType = rowType;
    }

    @Override
    public void reset() {
        this.row = new GenericRowData(getters.length);
    }

    @Override
    public void add(RowData value) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(value);
            if (field != null) {
                row.setField(i, field);
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
        return new PartialUpdateMergeFunction(rowType);
    }
}
