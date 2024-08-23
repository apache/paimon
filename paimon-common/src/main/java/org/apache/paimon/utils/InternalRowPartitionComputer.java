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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.TypeUtils.castFromString;

/** PartitionComputer for {@link InternalRow}. */
public class InternalRowPartitionComputer {

    protected final String defaultPartValue;
    protected final String[] partitionColumns;
    protected final InternalRow.FieldGetter[] partitionFieldGetters;

    public InternalRowPartitionComputer(
            String defaultPartValue, RowType rowType, String[] partitionColumns) {
        this.defaultPartValue = defaultPartValue;
        this.partitionColumns = partitionColumns;
        List<String> columnList = rowType.getFieldNames();
        this.partitionFieldGetters =
                Arrays.stream(partitionColumns)
                        .mapToInt(columnList::indexOf)
                        .mapToObj(
                                i ->
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                rowType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    public LinkedHashMap<String, String> generatePartValues(InternalRow in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        for (int i = 0; i < partitionFieldGetters.length; i++) {
            Object field = partitionFieldGetters[i].getFieldOrNull(in);
            String partitionValue = field != null ? field.toString() : null;
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

    public static String partToSimpleString(
            RowType partitionType, BinaryRow partition, String delimiter, int maxLength) {
        InternalRow.FieldGetter[] getters = partitionType.fieldGetters();
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
