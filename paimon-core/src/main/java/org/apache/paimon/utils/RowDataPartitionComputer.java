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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;

/** PartitionComputer for {@link InternalRow}. */
public class RowDataPartitionComputer {

    protected final String defaultPartValue;
    protected final String[] partitionColumns;
    protected final InternalRow.FieldGetter[] partitionFieldGetters;

    public RowDataPartitionComputer(
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
            String partitionValue = null;
            if (field != null) {
                if (field instanceof byte[]) {
                    partitionValue =
                            StandardCharsets.ISO_8859_1
                                    .decode(
                                            Base64.getEncoder()
                                                    .encode(ByteBuffer.wrap((byte[]) field)))
                                    .toString();
                } else {
                    partitionValue = field.toString();
                }
            }
            if (StringUtils.isNullOrWhitespaceOnly(partitionValue)) {
                partitionValue = defaultPartValue;
            }
            partSpec.put(partitionColumns[i], partitionValue);
        }
        return partSpec;
    }
}
