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

package org.apache.flink.table.store.file.stats;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/** Serializer for array of {@link FieldStats}. */
public class FieldStatsArraySerializer extends ObjectSerializer<FieldStats[]> {

    private static final long serialVersionUID = 1L;

    private final RowData.FieldGetter[] fieldGetters;

    public FieldStatsArraySerializer(RowType rowType) {
        super(schema(rowType));
        this.fieldGetters = createFieldGetters(toAllFieldsNullableRowType(rowType));
    }

    @Override
    public RowData toRow(FieldStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRowData minValues = new GenericRowData(rowFieldCount);
        GenericRowData maxValues = new GenericRowData(rowFieldCount);
        long[] nullCounts = new long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            minValues.setField(i, stats[i].minValue());
            maxValues.setField(i, stats[i].maxValue());
            nullCounts[i] = stats[i].nullCount();
        }
        return GenericRowData.of(minValues, maxValues, new GenericArrayData(nullCounts));
    }

    @Override
    public FieldStats[] fromRow(RowData row) {
        int rowFieldCount = fieldGetters.length;
        RowData minValues = row.getRow(0, rowFieldCount);
        RowData maxValues = row.getRow(1, rowFieldCount);
        long[] nullValues = row.getArray(2).toLongArray();

        FieldStats[] stats = new FieldStats[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            stats[i] =
                    new FieldStats(
                            fieldGetters[i].getFieldOrNull(minValues),
                            fieldGetters[i].getFieldOrNull(maxValues),
                            nullValues[i]);
        }
        return stats;
    }

    public static RowType schema(RowType rowType) {
        rowType = toAllFieldsNullableRowType(rowType);
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_MIN_VALUES", rowType));
        fields.add(new RowType.RowField("_MAX_VALUES", rowType));
        fields.add(new RowType.RowField("_NULL_COUNTS", new ArrayType(new BigIntType(false))));
        return new RowType(fields);
    }

    public static RowData.FieldGetter[] createFieldGetters(RowType rowType) {
        return IntStream.range(0, rowType.getFieldCount())
                .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                .toArray(RowData.FieldGetter[]::new);
    }

    private static RowType toAllFieldsNullableRowType(RowType rowType) {
        // as stated in SstFile.RollingFile#finish, field stats are not collected currently so
        // min/max values are all nulls
        return RowType.of(
                rowType.getFields().stream()
                        .map(f -> f.getType().copy(true))
                        .toArray(LogicalType[]::new),
                rowType.getFieldNames().toArray(new String[0]));
    }
}
