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

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils to fetch partition map information from data schema and row type. */
public class PartitionUtils {

    public static Pair<Pair<int[], RowType>, List<DataField>> trimPartitionFields(
            TableSchema dataSchema, List<DataField> dataFields) {
        if (dataSchema.partitionKeys().isEmpty()) {
            return Pair.of(null, dataFields);
        }
        return getPartitionMapping2fieldsWithoutPartition(
                dataSchema.partitionKeys(),
                dataFields,
                dataSchema.projectedLogicalRowType(dataSchema.partitionKeys()));
    }

    public static Pair<int[], RowType> getPartitionMapping(
            List<String> partitionKeys, List<DataField> dataFields, RowType partitionType) {
        return getPartitionMapping2fieldsWithoutPartition(partitionKeys, dataFields, partitionType)
                .getLeft();
    }

    public static Pair<Pair<int[], RowType>, List<DataField>>
            getPartitionMapping2fieldsWithoutPartition(
                    List<String> partitionKeys, List<DataField> dataFields, RowType partitionType) {
        if (partitionKeys.isEmpty()) {
            return Pair.of(null, dataFields);
        }

        List<DataField> fieldsWithoutPartition = new ArrayList<>();
        int[] map = new int[dataFields.size() + 1];
        int partitionFieldCount = 0;

        for (int i = 0; i < dataFields.size(); i++) {
            DataField field = dataFields.get(i);
            if (partitionKeys.contains(field.name())) {
                // if the map[i] is minus, represent the related column is stored in partition row
                map[i] = -(partitionKeys.indexOf(field.name()) + 1);
                partitionFieldCount++;
            } else {
                // else if the map[i] is positive, the related column is stored in the file-read row
                map[i] = (i - partitionFieldCount) + 1;
                fieldsWithoutPartition.add(field);
            }
        }

        Pair<int[], RowType> partitionMapping =
                fieldsWithoutPartition.size() == dataFields.size()
                        ? null
                        : Pair.of(map, partitionType);
        return Pair.of(partitionMapping, fieldsWithoutPartition);
    }

    public static PartitionInfo create(@Nullable Pair<int[], RowType> pair, BinaryRow binaryRow) {
        return pair == null ? null : new PartitionInfo(pair.getLeft(), pair.getRight(), binaryRow);
    }

    public static String buildPartitionName(Map<String, String> partitionSpec) {
        if (partitionSpec.isEmpty()) {
            return "";
        }
        List<String> partitionName =
                partitionSpec.keySet().stream()
                        .map(key -> key + "=" + partitionSpec.get(key))
                        .collect(Collectors.toList());
        return String.join("/", partitionName);
    }
}
