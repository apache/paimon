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

package org.apache.paimon.utils;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.InternalRowUtils.createNullCheckingFieldGetter;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.TypeUtils.castFromString;

/** PartitionComputer for {@link InternalRow}. */
public class InternalRowPartitionComputer {

    protected final String defaultPartValue;
    protected final String[] partitionColumns;
    protected final FieldGetter[] partitionFieldGetters;
    protected final CastExecutor[] partitionCastExecutors;
    protected final List<DataType> types;
    protected final boolean legacyPartitionName;

    public InternalRowPartitionComputer(
            String defaultPartValue,
            RowType rowType,
            String[] partitionColumns,
            boolean legacyPartitionName) {
        this.defaultPartValue = defaultPartValue;
        this.partitionColumns = partitionColumns;
        this.types = rowType.getFieldTypes();
        this.legacyPartitionName = legacyPartitionName;
        List<String> columnList = rowType.getFieldNames();
        this.partitionFieldGetters = new FieldGetter[partitionColumns.length];
        this.partitionCastExecutors = new CastExecutor[partitionColumns.length];
        for (String partitionColumn : partitionColumns) {
            int i = columnList.indexOf(partitionColumn);
            DataType type = rowType.getTypeAt(i);
            partitionFieldGetters[i] = createNullCheckingFieldGetter(type, i);
            partitionCastExecutors[i] = CastExecutors.resolve(type, VarCharType.STRING_TYPE);
        }
    }

    public LinkedHashMap<String, String> generatePartValues(InternalRow in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        for (int i = 0; i < partitionFieldGetters.length; i++) {
            Object field = partitionFieldGetters[i].getFieldOrNull(in);
            String partitionValue = null;
            if (field != null) {
                if (legacyPartitionName) {
                    partitionValue = field.toString();
                } else {
                    Object casted = partitionCastExecutors[i].cast(field);
                    if (casted != null) {
                        partitionValue = casted.toString();
                    }
                }
            }
            if (StringUtils.isNullOrWhitespaceOnly(partitionValue)) {
                partitionValue = defaultPartValue;
            }
            partSpec.put(partitionColumns[i], partitionValue);
        }
        return partSpec;
    }

    public static Map<String, Object> convertSpecToInternal(
            Map<String, String> spec, RowType partType, String defaultPartValue) {
        Map<String, Object> partValues = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            partValues.put(
                    entry.getKey(),
                    defaultPartValue.equals(entry.getValue())
                            ? null
                            : castFromString(
                                    entry.getValue(), partType.getField(entry.getKey()).type()));
        }
        return partValues;
    }

    public static GenericRow convertSpecToInternalRow(
            Map<String, String> spec, RowType partType, String defaultPartValue) {
        checkArgument(spec.size() == partType.getFieldCount());
        GenericRow partRow = new GenericRow(spec.size());
        List<String> fieldNames = partType.getFieldNames();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            Object value =
                    defaultPartValue.equals(entry.getValue())
                            ? null
                            : castFromString(
                                    entry.getValue(), partType.getField(entry.getKey()).type());
            partRow.setField(fieldNames.indexOf(entry.getKey()), value);
        }
        return partRow;
    }

    public static GenericRow convertSpecToInternalRow(Map<String, String> spec, RowType partType) {
        checkArgument(spec.size() == partType.getFieldCount());
        GenericRow partRow = new GenericRow(spec.size());
        List<String> fieldNames = partType.getFieldNames();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            Object value =
                    castFromString(entry.getValue(), partType.getField(entry.getKey()).type());
            partRow.setField(fieldNames.indexOf(entry.getKey()), value);
        }
        return partRow;
    }

    public static String partToSimpleString(
            RowType partitionType, BinaryRow partition, String delimiter, int maxLength) {
        FieldGetter[] getters = partitionType.fieldGetters();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < getters.length; i++) {
            Object part = getters[i].getFieldOrNull(partition);
            if (part != null) {
                builder.append(part);
            } else {
                builder.append("null");
            }
            if (i != getters.length - 1) {
                builder.append(delimiter);
            }
        }
        String result = builder.toString();
        return result.substring(0, Math.min(result.length(), maxLength));
    }
}
