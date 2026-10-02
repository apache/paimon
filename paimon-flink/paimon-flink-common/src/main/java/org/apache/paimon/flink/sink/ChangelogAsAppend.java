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

package org.apache.paimon.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

/** Materializes changelog payloads as independent log records before the append writer. */
final class ChangelogAsAppend implements MapFunction<RowData, RowData> {
    private static final long serialVersionUID = 1L;

    private final RowType rowType;
    private final int kindField;
    private final int timeField;
    private transient RowDataSerializer serializer;
    private transient RowData.FieldGetter[] getters;

    ChangelogAsAppend(RowType rowType, String kindField, String timeField) {
        this.rowType = rowType;
        this.kindField = field(rowType, kindField, LogicalTypeRoot.VARCHAR);
        this.timeField = field(rowType, timeField, LogicalTypeRoot.BIGINT);
        if (this.kindField == this.timeField) {
            throw new IllegalArgumentException("Changelog metadata fields must be distinct.");
        }
    }

    private static int field(RowType rowType, String name, LogicalTypeRoot type) {
        int index = rowType.getFieldNames().indexOf(name);
        if (index < 0 || rowType.getTypeAt(index).getTypeRoot() != type) {
            throw new IllegalArgumentException(
                    "Changelog metadata requires a physical " + type + " field: " + name);
        }
        if (!rowType.getTypeAt(index).isNullable()
                || (type == LogicalTypeRoot.VARCHAR
                        && ((VarCharType) rowType.getTypeAt(index)).getLength() < 13)) {
            throw new IllegalArgumentException(
                    "Changelog metadata fields must be nullable; kind field must fit UPDATE_BEFORE.");
        }
        return index;
    }

    @Override
    public RowData map(RowData input) {
        if (serializer == null) {
            serializer = new RowDataSerializer(rowType);
            getters = new RowData.FieldGetter[rowType.getFieldCount()];
            for (int i = 0; i < getters.length; i++) {
                getters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
            }
        }
        // Deep copy first: upstream operators may reuse both rows and nested values.
        RowData copy = serializer.copy(input);
        GenericRowData output = new GenericRowData(RowKind.INSERT, getters.length);
        for (int i = 0; i < getters.length; i++) {
            output.setField(i, getters[i].getFieldOrNull(copy));
        }
        output.setField(kindField, StringData.fromString(input.getRowKind().name()));
        output.setField(timeField, System.currentTimeMillis());
        return output;
    }
}
