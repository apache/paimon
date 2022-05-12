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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.utils.RowDataToObjectArrayConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.store.file.utils.SerializationUtils.newBytesType;

/** Serializer for array of {@link FieldStats}. */
public class FieldStatsArraySerializer {

    private final RowDataSerializer serializer;

    private final RowDataToObjectArrayConverter converter;

    public FieldStatsArraySerializer(RowType type) {
        this.serializer = new RowDataSerializer(type);
        this.converter = new RowDataToObjectArrayConverter(toAllFieldsNullableRowType(type));
    }

    public BinaryTableStats toBinary(FieldStats[] stats) {
        int rowFieldCount = stats.length;
        GenericRowData minValues = new GenericRowData(rowFieldCount);
        GenericRowData maxValues = new GenericRowData(rowFieldCount);
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
        int rowFieldCount = converter.getArity();

        Object[] minValueObjects = converter.convert(array.min());
        Object[] maxValueObjects = converter.convert(array.max());

        FieldStats[] stats = new FieldStats[rowFieldCount];
        for (int i = 0; i < rowFieldCount; i++) {
            stats[i] =
                    new FieldStats(minValueObjects[i], maxValueObjects[i], array.nullCounts()[i]);
        }
        return stats;
    }

    public static RowType schema() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_MIN_VALUES", newBytesType(false)));
        fields.add(new RowType.RowField("_MAX_VALUES", newBytesType(false)));
        fields.add(new RowType.RowField("_NULL_COUNTS", new ArrayType(new BigIntType(false))));
        return new RowType(fields);
    }

    private static RowType toAllFieldsNullableRowType(RowType rowType) {
        // as stated in RollingFile.Writer#finish, field stats are not collected currently so
        // min/max values are all nulls
        return RowType.of(
                rowType.getFields().stream()
                        .map(f -> f.getType().copy(true))
                        .toArray(LogicalType[]::new),
                rowType.getFieldNames().toArray(new String[0]));
    }
}
