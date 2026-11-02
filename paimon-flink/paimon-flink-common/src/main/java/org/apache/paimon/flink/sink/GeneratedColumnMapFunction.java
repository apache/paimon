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

import org.apache.paimon.flink.FlinkRowDataWithBlob;
import org.apache.paimon.flink.FlinkRowWrapper;
import org.apache.paimon.flink.ProjectedRowData;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.GeneratedColumnRow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;

/** Fills omitted physical generated columns before partitioning and bucket assignment. */
class GeneratedColumnMapFunction implements MapFunction<RowData, RowData> {

    private final RowType rowType;
    private final int[] generatedColumns;

    private transient GeneratedColumnRow generator;
    private transient ProjectedRowData inputProjection;
    private transient RowData.FieldGetter[] getters;
    private transient FlinkRowDataWithBlob generatedRow;

    GeneratedColumnMapFunction(RowType rowType, int[] generatedColumns) {
        this.rowType = rowType;
        this.generatedColumns = generatedColumns;
    }

    private void initialize() {
        BitSet omitted = new BitSet(rowType.getFieldCount());
        for (int index : generatedColumns) {
            omitted.set(index);
        }
        int[] inputFields =
                IntStream.range(0, rowType.getFieldCount()).filter(i -> !omitted.get(i)).toArray();
        inputProjection = ProjectedRowData.from(inputFields);
        generator = new GeneratedColumnRow(rowType, rowType.project(inputFields));
        getters = new RowData.FieldGetter[rowType.getFieldCount()];
        Set<Integer> blobFields = new HashSet<>();
        for (int i = 0; i < getters.length; i++) {
            getters[i] =
                    RowData.createFieldGetter(toLogicalType(rowType.getTypeAt(i)).copy(true), i);
            if (BlobType.isBlobFileField(rowType.getTypeAt(i))) {
                blobFields.add(i);
            }
        }
        generatedRow = new FlinkRowDataWithBlob(null, blobFields, true);
    }

    @Override
    public RowData map(RowData row) {
        if (generator == null) {
            initialize();
        }
        // Remove only omitted generated columns; supplied columns, including NULLs, stay in input.
        generatedRow.replace(
                generator.replaceRow(new FlinkRowWrapper(inputProjection.replaceRow(row))));
        GenericRowData result = new GenericRowData(row.getRowKind(), getters.length);
        // Preserve Flink's representation of supplied columns, including BLOB bytes and nested
        // rows.
        for (int i = 0; i < getters.length; i++) {
            result.setField(i, getters[i].getFieldOrNull(row));
        }
        for (int index : generatedColumns) {
            result.setField(index, getters[index].getFieldOrNull(generatedRow));
        }
        return result;
    }
}
