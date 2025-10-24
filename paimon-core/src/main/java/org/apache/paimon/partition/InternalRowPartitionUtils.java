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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.ListUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils class for {@link InternalRow} partition. */
public class InternalRowPartitionUtils {

    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitions(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator partitionComparator) {
        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();
        int targetIndex = sortedTargetPartitions.size() - 1;
        for (BinaryRow sourceRow : sortedSourcePartitions) {
            BinaryRow firstSmaller = null;
            while (targetIndex >= 0) {
                BinaryRow targetRow = sortedTargetPartitions.get(targetIndex);
                if (partitionComparator.compare(targetRow, sourceRow) < 0) {
                    firstSmaller = targetRow;
                    break;
                }
                targetIndex--;
            }
            partitionMapping.put(sourceRow, firstSmaller);
        }
        return partitionMapping;
    }

    public static List<BinaryRow> getDeltaPartitions(
            BinaryRow beginPartition,
            BinaryRow endPartition,
            List<String> partitionColumns,
            RowType partType,
            CoreOptions options,
            RecordComparator partitionComparator,
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        boolean isDailyPartition = partitionColumns.size() == 1;
        List<String> startPartitionValues =
                partitionComputer.generateOrderPartValues(beginPartition);
        List<String> endPartitionValues = partitionComputer.generateOrderPartValues(endPartition);
        PartitionTimeExtractor timeExtractor =
                new PartitionTimeExtractor(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        PartitionValueGenerator valueGenerator =
                new PartitionValueGenerator(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        LocalDateTime stratPartitionTime =
                timeExtractor.extract(partitionColumns, startPartitionValues);
        LocalDateTime candidateTime = stratPartitionTime;
        LocalDateTime endPartitionTime =
                timeExtractor.extract(partitionColumns, endPartitionValues);
        while (candidateTime.compareTo(endPartitionTime) <= 0) {
            if (isDailyPartition) {
                if (candidateTime.compareTo(stratPartitionTime) > 0) {
                    deltaPartitions.add(
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    ListUtils.convertListsToMap(
                                                            partitionColumns,
                                                            valueGenerator.generatePartitionValues(
                                                                    candidateTime,
                                                                    partitionColumns)),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy());
                }
            } else {
                for (int hour = 0; hour <= 23; hour++) {
                    candidateTime = candidateTime.toLocalDate().atStartOfDay().plusHours(hour);
                    List<String> candidatePartitionValues =
                            valueGenerator.generatePartitionValues(candidateTime, partitionColumns);
                    BinaryRow candidatePartition =
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    ListUtils.convertListsToMap(
                                                            partitionColumns,
                                                            candidatePartitionValues),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy();
                    if (partitionComparator.compare(candidatePartition, beginPartition) > 0
                            && partitionComparator.compare(candidatePartition, endPartition) <= 0) {
                        deltaPartitions.add(candidatePartition);
                    }
                }
            }
            candidateTime = candidateTime.toLocalDate().plusDays(1).atStartOfDay();
        }
        return deltaPartitions;
    }
}
