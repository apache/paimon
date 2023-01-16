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

import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.RowDataSerializer;
import org.apache.flink.table.store.file.casting.CastExecutor;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.RowDataUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.store.file.utils.SerializationUtils.newBytesType;

/** Serializer for array of {@link FieldStats}. */
public class FieldStatsArraySerializer {

    private final RowDataSerializer serializer;

    private final InternalRow.FieldGetter[] fieldGetters;

    @Nullable private final int[] indexMapping;
    @Nullable private final CastExecutor<Object, Object>[] converterMapping;

    public FieldStatsArraySerializer(RowType type) {
        this(type, null, null);
    }

    public FieldStatsArraySerializer(
            RowType type, int[] indexMapping, CastExecutor<Object, Object>[] converterMapping) {
        RowType safeType = toAllFieldsNullableRowType(type);
        this.serializer = new RowDataSerializer(safeType);
        this.fieldGetters =
                IntStream.range(0, safeType.getFieldCount())
                        .mapToObj(
                                i ->
                                        RowDataUtils.createNullCheckingFieldGetter(
                                                safeType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
        this.indexMapping = indexMapping;
        this.converterMapping = converterMapping;
    }

    public BinaryTableStats toBinary(FieldStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRow minValues = new GenericRow(rowFieldCount);
        GenericRow maxValues = new GenericRow(rowFieldCount);
        long[] nullCounts = new long[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            minValues.setField(i, stats[i].minValue());
            maxValues.setField(i, stats[i].maxValue());
            nullCounts[i] = stats[i].nullCount();
        }
        return new BinaryTableStats(
                serializer.toBinaryRow(minValues).copy(),
                serializer.toBinaryRow(maxValues).copy(),
                nullCounts,
                stats);
    }

    public FieldStats[] fromBinary(BinaryTableStats array) {
        return fromBinary(array, null);
    }

    public FieldStats[] fromBinary(BinaryTableStats array, @Nullable Long rowCount) {
        int fieldCount = indexMapping == null ? fieldGetters.length : indexMapping.length;
        FieldStats[] stats = new FieldStats[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            int fieldIndex = indexMapping == null ? i : indexMapping[i];
            if (fieldIndex < 0 || fieldIndex >= array.min().getFieldCount()) {
                // simple evolution for add column
                if (rowCount == null) {
                    throw new RuntimeException("Schema Evolution for stats needs row count.");
                }
                stats[i] = new FieldStats(null, null, rowCount);
            } else {
                CastExecutor<Object, Object> converter =
                        converterMapping == null ? null : converterMapping[i];
                Object min = fieldGetters[fieldIndex].getFieldOrNull(array.min());
                min = converter == null || min == null ? min : converter.cast(min);

                Object max = fieldGetters[fieldIndex].getFieldOrNull(array.max());
                max = converter == null || max == null ? max : converter.cast(max);

                stats[i] = new FieldStats(min, max, array.nullCounts()[fieldIndex]);
            }
        }
        return stats;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_MIN_VALUES", newBytesType(false)));
        fields.add(new DataField(1, "_MAX_VALUES", newBytesType(false)));
        fields.add(new DataField(2, "_NULL_COUNTS", new ArrayType(new BigIntType(false))));
        return new RowType(fields);
    }

    private static RowType toAllFieldsNullableRowType(RowType rowType) {
        // as stated in RollingFile.Writer#finish, field stats are not collected currently so
        // min/max values are all nulls
        return RowType.builder()
                .fields(
                        rowType.getFields().stream()
                                .map(f -> f.type().copy(true))
                                .toArray(DataType[]::new),
                        rowType.getFieldNames().toArray(new String[0]))
                .build();
    }
}
