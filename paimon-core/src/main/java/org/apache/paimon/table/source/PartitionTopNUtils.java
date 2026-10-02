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

package org.apache.paimon.table.source;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Internal utilities for selecting top partition prefixes. */
public final class PartitionTopNUtils {

    private PartitionTopNUtils() {}

    public static List<BinaryRow> topNFileStorePartitions(
            List<PartitionEntry> partitionEntries,
            RowType partitionType,
            int num,
            int partitionFieldCount) {
        if (partitionType.getFieldCount() == 0) {
            throw new UnsupportedOperationException(
                    "Cannot find top partitions for a non-partitioned table.");
        }
        validateParameters(partitionType, num, partitionFieldCount);
        List<List<BinaryRow>> groups =
                partitionGroupsDescending(
                        partitionEntries.stream()
                                .filter(entry -> entry.fileCount() > 0)
                                .collect(Collectors.toList()),
                        partitionType,
                        partitionFieldCount);
        List<BinaryRow> result = new ArrayList<>();
        for (int i = 0; i < Math.min(num, groups.size()); i++) {
            result.addAll(groups.get(i));
        }
        return result;
    }

    public static void validateParameters(RowType partitionType, int num, int partitionFieldCount) {
        Preconditions.checkArgument(num > 0, "Number of top partitions must be positive.");
        validatePartitionFieldCount(partitionType, partitionFieldCount);
    }

    public static List<List<BinaryRow>> partitionGroupsDescending(
            List<PartitionEntry> partitionEntries, RowType partitionType, int partitionFieldCount) {
        return partitionGroupsDescendingFromRows(
                partitionEntries.stream()
                        .map(PartitionEntry::partition)
                        .collect(Collectors.toList()),
                partitionType,
                partitionFieldCount);
    }

    public static List<BinaryRow> distinctPartitionsDescending(
            List<BinaryRow> partitions, RowType partitionType) {
        List<List<BinaryRow>> groups =
                partitionGroupsDescendingFromRows(
                        partitions, partitionType, partitionType.getFieldCount());
        List<BinaryRow> result = new ArrayList<>();
        groups.forEach(result::addAll);
        return result;
    }

    private static List<List<BinaryRow>> partitionGroupsDescendingFromRows(
            List<BinaryRow> partitions, RowType partitionType, int partitionFieldCount) {
        validatePartitionFieldCount(partitionType, partitionFieldCount);

        int[] prefixFields = new int[partitionFieldCount];
        for (int i = 0; i < partitionFieldCount; i++) {
            prefixFields[i] = i;
        }
        RecordComparator prefixComparator =
                CodeGenUtils.newRecordComparator(partitionType.getFieldTypes(), prefixFields);
        RecordComparator fullComparator =
                CodeGenUtils.newRecordComparator(partitionType.getFieldTypes());

        List<BinaryRow> sorted =
                partitions.stream().sorted(fullComparator.reversed()).collect(Collectors.toList());
        List<List<BinaryRow>> groups = new ArrayList<>();
        BinaryRow previousPartition = null;
        BinaryRow previousPrefix = null;
        for (BinaryRow partition : sorted) {
            if (previousPartition != null
                    && fullComparator.compare(previousPartition, partition) == 0) {
                continue;
            }
            if (previousPrefix == null
                    || prefixComparator.compare(previousPrefix, partition) != 0) {
                groups.add(new ArrayList<>());
                previousPrefix = partition;
            }
            groups.get(groups.size() - 1).add(partition);
            previousPartition = partition;
        }
        return groups;
    }

    private static void validatePartitionFieldCount(
            RowType partitionType, int partitionFieldCount) {
        Preconditions.checkArgument(
                partitionFieldCount > 0 && partitionFieldCount <= partitionType.getFieldCount(),
                "Partition field count must be between 1 and %s.",
                partitionType.getFieldCount());
    }
}
