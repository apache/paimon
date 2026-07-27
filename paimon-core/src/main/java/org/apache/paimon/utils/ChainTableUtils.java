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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

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
            RecordComparator partitionComparator,
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        boolean isDailyPartition = partitionColumns.size() == 1;
        List<String> startPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(beginPartition).values());
        List<String> endPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(endPartition).values());
        PartitionTimeExtractor timeExtractor =
                new PartitionTimeExtractor(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        LocalDateTime stratPartitionTime =
                timeExtractor.extract(partitionColumns, startPartitionValues);
        LocalDateTime candidateTime = stratPartitionTime;
        LocalDateTime endPartitionTime =
                timeExtractor.extract(partitionColumns, endPartitionValues);
        while (!candidateTime.isAfter(endPartitionTime)) {
            if (isDailyPartition) {
                if (candidateTime.isAfter(stratPartitionTime)) {
                    deltaPartitions.add(
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy());
                }
            } else {
                for (int hour = 0; hour <= 23; hour++) {
                    candidateTime = candidateTime.toLocalDate().atStartOfDay().plusHours(hour);
                    BinaryRow candidatePartition =
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
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

    public static LinkedHashMap<String, String> calPartValues(
            LocalDateTime dateTime,
            List<String> partitionKeys,
            String timestampPattern,
            String timestampFormatter) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampFormatter);
        String formattedDateTime = dateTime.format(formatter);
        Pattern keyPattern = Pattern.compile("\\$(\\w+)");
        Matcher keyMatcher = keyPattern.matcher(timestampPattern);
        List<String> keyOrder = new ArrayList<>();
        StringBuilder regexBuilder = new StringBuilder();
        int lastPosition = 0;
        while (keyMatcher.find()) {
            regexBuilder.append(
                    Pattern.quote(timestampPattern.substring(lastPosition, keyMatcher.start())));
            regexBuilder.append("(.+)");
            keyOrder.add(keyMatcher.group(1));
            lastPosition = keyMatcher.end();
        }
        regexBuilder.append(Pattern.quote(timestampPattern.substring(lastPosition)));

        Matcher valueMatcher = Pattern.compile(regexBuilder.toString()).matcher(formattedDateTime);
        if (!valueMatcher.matches() || valueMatcher.groupCount() != keyOrder.size()) {
            throw new IllegalArgumentException(
                    "Formatted datetime does not match timestamp pattern");
        }

        Map<String, String> keyValues = new HashMap<>();
        for (int i = 0; i < keyOrder.size(); i++) {
            keyValues.put(keyOrder.get(i), valueMatcher.group(i + 1));
        }
        List<String> values =
                partitionKeys.stream()
                        .map(key -> keyValues.getOrDefault(key, ""))
                        .collect(Collectors.toList());
        LinkedHashMap<String, String> res = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            res.put(partitionKeys.get(i), values.get(i));
        }
        return res;
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
                        chainPartitionComparator,
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
     * Builds per-bucket {@link ChainSplit}s from the given snapshot and delta splits. Files that
     * originate from the snapshot splits are tagged with {@code snapshotBranch}; all other files
     * are tagged with {@code deltaBranch}.
     *
     * <p>This overload produces one {@link ChainSplit} per bucket (no key-range splitting). It is
     * used by streaming read, where splitting a bucket by key range is not desired.
     *
     * @param logicalPartition the logical partition for the resulting ChainSplits
     * @param snapshotSplits splits from the snapshot branch
     * @param deltaSplits splits from the delta branch
     * @param snapshotBranch name of the snapshot branch
     * @param deltaBranch name of the delta branch
     * @return one ChainSplit per bucket
     */
    public static List<ChainSplit> buildChainSplits(
            BinaryRow logicalPartition,
            List<DataSplit> snapshotSplits,
            List<DataSplit> deltaSplits,
            String snapshotBranch,
            String deltaBranch) {
        return buildChainSplits(
                logicalPartition,
                snapshotSplits,
                deltaSplits,
                snapshotBranch,
                deltaBranch,
                null,
                0L,
                0L);
    }

    /**
     * Builds {@link ChainSplit}s from the given snapshot and delta splits, optionally splitting
     * each bucket's files into multiple splits by key range to improve batch read parallelism.
     *
     * <p>When {@code keyComparator} is non-null, each bucket's snapshot and delta files are split
     * via interval partition: files with intersecting key ranges always go to the same split, so
     * that all versions of a key across the snapshot and delta branches are merged together within
     * one split. Key-disjoint sections are then packed into splits up to {@code targetSplitSize}.
     * This is the same invariant the ordinary primary-key table relies on (see {@link
     * org.apache.paimon.table.source.MergeTreeSplitGenerator}), except the chain path always goes
     * through the interval partition because snapshot and delta files from different branches may
     * have intersecting key ranges.
     *
     * <p>When {@code keyComparator} is null, each bucket produces a single {@link ChainSplit} (the
     * original behavior, used by streaming).
     *
     * @param keyComparator primary-key comparator used to order files by min/max key; null disables
     *     key-range splitting
     * @param targetSplitSize target size (in bytes) of each split; ignored when {@code
     *     keyComparator} is null
     * @param openFileCost per-file open cost accounted for in packing; ignored when {@code
     *     keyComparator} is null
     */
    public static List<ChainSplit> buildChainSplits(
            BinaryRow logicalPartition,
            List<DataSplit> snapshotSplits,
            List<DataSplit> deltaSplits,
            String snapshotBranch,
            String deltaBranch,
            @Nullable Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost) {
        Set<String> snapshotFileNames =
                snapshotSplits.stream()
                        .flatMap(s -> s.dataFiles().stream().map(DataFileMeta::fileName))
                        .collect(Collectors.toSet());

        Map<Integer, List<DataSplit>> bucketSplits = new LinkedHashMap<>();
        Integer bucketInAll = null;
        for (DataSplit ds : snapshotSplits) {
            bucketInAll = addToBucketMap(ds, bucketSplits, bucketInAll);
        }
        for (DataSplit ds : deltaSplits) {
            bucketInAll = addToBucketMap(ds, bucketSplits, bucketInAll);
        }

        List<ChainSplit> result = new ArrayList<>();
        for (Map.Entry<Integer, List<DataSplit>> entry : bucketSplits.entrySet()) {
            Map<String, String> fileBranchMapping = new HashMap<>();
            Map<String, String> fileBucketPathMapping = new HashMap<>();
            List<DataFileMeta> bucketFiles = new ArrayList<>();
            for (DataSplit ds : entry.getValue()) {
                for (DataFileMeta file : ds.dataFiles()) {
                    fileBucketPathMapping.put(file.fileName(), ds.bucketPath());
                    String branch =
                            snapshotFileNames.contains(file.fileName())
                                    ? snapshotBranch
                                    : deltaBranch;
                    fileBranchMapping.put(file.fileName(), branch);
                    bucketFiles.add(file);
                }
            }

            List<List<DataFileMeta>> fileGroups;
            if (keyComparator == null) {
                fileGroups = Collections.singletonList(bucketFiles);
            } else {
                fileGroups =
                        splitBucketByKeyRange(
                                bucketFiles, keyComparator, targetSplitSize, openFileCost);
            }

            for (List<DataFileMeta> groupFiles : fileGroups) {
                result.add(
                        new ChainSplit(
                                logicalPartition,
                                groupFiles,
                                subMapping(fileBranchMapping, groupFiles),
                                subMapping(fileBucketPathMapping, groupFiles)));
            }
        }
        return result;
    }

    /**
     * Splits a bucket's files (snapshot + delta combined) into key-range groups. The interval
     * partition first sorts files by (minKey, maxKey) and groups key-overlapping files into the
     * same section, guaranteeing all versions of a key stay in one split. Sections are key-disjoint
     * and key-ordered, so packing consecutive sections up to {@code targetSplitSize} preserves that
     * invariant.
     */
    private static List<List<DataFileMeta>> splitBucketByKeyRange(
            List<DataFileMeta> files,
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost) {
        List<List<DataFileMeta>> sections = new ArrayList<>();
        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            List<DataFileMeta> sectionFiles = new ArrayList<>();
            for (SortedRun run : section) {
                sectionFiles.addAll(run.files());
            }
            sections.add(sectionFiles);
        }

        Function<List<DataFileMeta>, Long> weightFunc =
                section -> Math.max(totalSize(section), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
                .map(ChainTableUtils::flatFiles)
                .collect(Collectors.toList());
    }

    private static long totalSize(List<DataFileMeta> files) {
        long size = 0L;
        for (DataFileMeta file : files) {
            size += file.fileSize();
        }
        return size;
    }

    private static List<DataFileMeta> flatFiles(List<List<DataFileMeta>> packedSections) {
        List<DataFileMeta> files = new ArrayList<>();
        packedSections.forEach(files::addAll);
        return files;
    }

    private static Map<String, String> subMapping(
            Map<String, String> mapping, List<DataFileMeta> files) {
        Map<String, String> sub = new HashMap<>();
        for (DataFileMeta file : files) {
            sub.put(file.fileName(), mapping.get(file.fileName()));
        }
        return sub;
    }

    private static Integer addToBucketMap(
            DataSplit ds, Map<Integer, List<DataSplit>> bucketSplits, Integer bucketInAll) {
        Integer totalBuckets = ds.totalBuckets();
        checkNotNull(totalBuckets, "totalBuckets should not be null");
        if (bucketInAll != null) {
            checkArgument(
                    totalBuckets.equals(bucketInAll), "Inconsistent bucket num " + ds.bucket());
        }
        bucketSplits.computeIfAbsent(ds.bucket(), k -> new ArrayList<>()).add(ds);
        return totalBuckets;
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
