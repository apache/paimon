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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.partition.PartitionTimeResolver;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/** Utils for chain table. */
public class ChainTableUtils {

    public static boolean isChainTable(Map<String, String> tblOptions) {
        return CoreOptions.fromMap(tblOptions).isChainTable();
    }

    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitions(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator partitionComparator) {
        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();
        int targetIndex = 0;
        for (BinaryRow sourceRow : sortedSourcePartitions) {
            BinaryRow firstSmaller;
            while (targetIndex < sortedTargetPartitions.size()
                    && partitionComparator.compare(
                                    sortedTargetPartitions.get(targetIndex), sourceRow)
                            < 0) {
                targetIndex++;
            }
            firstSmaller = (targetIndex > 0) ? sortedTargetPartitions.get(targetIndex - 1) : null;
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
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        List<String> startPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(beginPartition).values());
        List<String> endPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(endPartition).values());
        PartitionTimeResolver timeResolver =
                new PartitionTimeResolver(
                        partitionColumns,
                        options.partitionTimestampPattern(),
                        options.partitionTimestampFormatter());
        LocalDateTime stratPartitionTime = timeResolver.parsePartitionValues(startPartitionValues);
        LocalDateTime endPartitionTime = timeResolver.parsePartitionValues(endPartitionValues);
        TemporalAmount step = timeResolver.extractMinStep();
        LocalDateTime candidateTime = stratPartitionTime.plus(step);
        while (!candidateTime.isAfter(endPartitionTime)) {
            BinaryRow candidatePartition =
                    serializer
                            .toBinaryRow(
                                    InternalRowPartitionComputer.convertSpecToInternalRow(
                                            timeResolver.resolvePartitionValues(candidateTime),
                                            partType,
                                            options.partitionDefaultName()))
                            .copy();
            deltaPartitions.add(candidatePartition);
            candidateTime = candidateTime.plus(step);
        }
        return deltaPartitions;
    }

    public static Predicate createTriangularPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> innerFunc,
            BiFunction<Integer, Object, Predicate> outerFunc) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            List<Predicate> andConditions = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                Object o = partitionObjects[j];
                andConditions.add(innerFunc.apply(j, o));
            }
            Object currentValue = partitionObjects[i];
            andConditions.add(outerFunc.apply(i, currentValue));
            fieldPredicates.add(PredicateBuilder.and(andConditions));
        }
        return PredicateBuilder.or(fieldPredicates);
    }

    public static Predicate createLinearPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> func) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            fieldPredicates.add(func.apply(i, partitionObjects[i]));
        }
        return PredicateBuilder.and(fieldPredicates);
    }

    public static boolean isScanFallbackDeltaBranch(CoreOptions options) {
        return options.isChainTable()
                && options.scanFallbackDeltaBranch().equalsIgnoreCase(options.branch());
    }

    public static boolean isScanFallbackSnapshotBranch(CoreOptions options) {
        return options.isChainTable()
                && options.scanFallbackSnapshotBranch().equalsIgnoreCase(options.branch());
    }

    public static FileStoreTable resolveChainPrimaryTable(FileStoreTable table) {
        if (table.coreOptions().isChainTable() && table instanceof FallbackReadFileStoreTable) {
            return ((ChainGroupReadTable) ((FallbackReadFileStoreTable) table).other()).wrapped();
        }
        return table;
    }

    public static List<String> chainPartitionKeys(
            CoreOptions options, List<String> allPartitionKeys) {
        List<String> chainPartitionKeys = options.chainTableChainPartitionKeys();
        if (chainPartitionKeys == null) {
            return allPartitionKeys;
        }
        return chainPartitionKeys;
    }

    /**
     * Within the same group, find the nearest smaller partition in the target list for each source
     * partition, comparing only on the chain dimension.
     *
     * @param sortedSourcePartitions full partitions (sorted by chain dimension)
     * @param sortedTargetPartitions full partitions (sorted by chain dimension, same group)
     * @param chainComparator compares chain dimension only
     * @param projector partition projector
     * @return source → target mapping, target may be null
     */
    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitionsWithProjector(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator chainComparator,
            ChainPartitionProjector projector) {

        sortedTargetPartitions.sort(
                (a, b) ->
                        chainComparator.compare(
                                projector.extractChainPartition(b),
                                projector.extractChainPartition(a)));

        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();

        for (BinaryRow sourceRow : sortedSourcePartitions) {
            int targetIndex = 0;
            BinaryRow sourceChain = projector.extractChainPartition(sourceRow);
            while (targetIndex < sortedTargetPartitions.size()) {
                BinaryRow targetChain =
                        projector.extractChainPartition(sortedTargetPartitions.get(targetIndex));
                if (chainComparator.compare(targetChain, sourceChain) < 0) {
                    break;
                } else {
                    targetIndex++;
                }
            }
            BinaryRow firstSmaller =
                    (targetIndex < sortedTargetPartitions.size())
                            ? sortedTargetPartitions.get(targetIndex)
                            : null;
            partitionMapping.put(sourceRow, firstSmaller);
        }
        return partitionMapping;
    }

    /**
     * Generates the list of delta partitions in the range (beginPartition, endPartition].
     * Enumerates time range only on the chain dimension; the group dimension stays unchanged.
     */
    public static List<BinaryRow> getDeltaPartitionsWithProjector(
            BinaryRow beginPartition,
            BinaryRow endPartition,
            CoreOptions options,
            RecordComparator chainPartitionComparator,
            ChainPartitionProjector projector) {

        // Extract the chain parts from begin/end
        BinaryRow beginChain = projector.extractChainPartition(beginPartition);
        BinaryRow endChain = projector.extractChainPartition(endPartition);

        // Build chain-dimension RowType, column names, PartitionComputer
        RowType chainPartType = projector.chainPartitionType();
        List<String> chainPartitionColumns = chainPartType.getFieldNames();
        InternalRowPartitionComputer chainPartitionComputer =
                new InternalRowPartitionComputer(
                        options.partitionDefaultName(),
                        chainPartType,
                        chainPartitionColumns.toArray(new String[0]),
                        options.legacyPartitionName());

        // Reuse existing getDeltaPartitions to enumerate on chain dimension
        List<BinaryRow> chainOnlyDeltas =
                getDeltaPartitions(
                        beginChain,
                        endChain,
                        chainPartitionColumns,
                        chainPartType,
                        options,
                        chainPartitionComputer);

        // Combine each chain-only BinaryRow with the group part into a full partition
        BinaryRow groupPart = projector.extractGroupPartition(beginPartition);
        List<BinaryRow> fullDeltas = new ArrayList<>(chainOnlyDeltas.size());
        for (BinaryRow chainDelta : chainOnlyDeltas) {
            fullDeltas.add(projector.combinePartition(groupPart, chainDelta));
        }
        return fullDeltas;
    }

    /**
     * Builds a compound predicate: group fields exact match AND chain fields triangular range.
     *
     * @param fullPartition full partition row
     * @param converter converter for the full partition
     * @param groupFieldCount number of group fields
     * @param innerFunc equality function (field_index, value) → Predicate
     * @param outerFunc range function (field_index, value) → Predicate
     */
    public static Predicate createGroupChainPredicate(
            BinaryRow fullPartition,
            RowDataToObjectArrayConverter converter,
            int groupFieldCount,
            BiFunction<Integer, Object, Predicate> innerFunc,
            BiFunction<Integer, Object, Predicate> outerFunc) {

        Object[] allValues = converter.convert(fullPartition);
        List<Predicate> conditions = new ArrayList<>();

        for (int i = 0; i < groupFieldCount; i++) {
            conditions.add(innerFunc.apply(i, allValues[i]));
        }

        List<Predicate> chainFieldPredicates = new ArrayList<>();
        int totalFields = converter.getArity();
        for (int i = groupFieldCount; i < totalFields; i++) {
            List<Predicate> andConditions = new ArrayList<>();
            for (int j = groupFieldCount; j < i; j++) {
                andConditions.add(innerFunc.apply(j, allValues[j]));
            }
            andConditions.add(outerFunc.apply(i, allValues[i]));
            chainFieldPredicates.add(PredicateBuilder.and(andConditions));
        }

        if (!chainFieldPredicates.isEmpty()) {
            conditions.add(PredicateBuilder.or(chainFieldPredicates));
        }

        return PredicateBuilder.and(conditions);
    }

    /**
     * Validates that the chain table configuration is compatible with incremental read paths
     * (streaming read and lookup join). All validation rules for incremental reads should be
     * centralized here.
     */
    public static void validateChainTableForIncrementalRead(ChainGroupReadTable table) {
        CoreOptions.MergeEngine mergeEngine = table.other().coreOptions().mergeEngine();
        if (mergeEngine != CoreOptions.MergeEngine.DEDUPLICATE) {
            throw new IllegalArgumentException(
                    "Chain table does not support "
                            + mergeEngine
                            + " merge engine on the delta branch "
                            + "for streaming read or lookup join. "
                            + "Please use the DEDUPLICATE merge engine on the delta branch.");
        }
    }
}
