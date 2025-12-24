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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact coordinator to compact data evolution table. */
public class DataEvolutionCompactCoordinator {

    private static final int FILES_BATCH = 100_000;

    private final CompactScanner scanner;
    private final CompactPlanner planner;

    public DataEvolutionCompactCoordinator(FileStoreTable table, boolean compactBlob) {
        this(table, null, compactBlob);
    }

    public DataEvolutionCompactCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean compactBlob) {
        CoreOptions options = table.coreOptions();
        long targetFileSize = options.targetFileSize(false);
        long openFileCost = options.splitOpenFileCost();
        long compactMinFileNum = options.compactionMinFileNum();

        this.scanner =
                new CompactScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate));
        this.planner =
                new CompactPlanner(compactBlob, targetFileSize, openFileCost, compactMinFileNum);
    }

    public List<DataEvolutionCompactTask> plan() {
        // scan files in snapshot
        List<ManifestEntry> entries = scanner.scan();
        if (!entries.isEmpty()) {
            // do plan compact tasks
            return planner.compactPlan(entries);
        }

        return Collections.emptyList();
    }

    /** Scanner to generate sorted ManifestEntries. */
    static class CompactScanner {

        private final SnapshotReader snapshotReader;
        private final Queue<List<ManifestFileMeta>> metas;

        private CompactScanner(SnapshotReader snapshotReader) {
            this.snapshotReader = snapshotReader;
            Snapshot snapshot = snapshotReader.snapshotManager().latestSnapshot();

            List<ManifestFileMeta> manifestFileMetas =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;
            RangeHelper<ManifestFileMeta> rangeHelper =
                    new RangeHelper<>(ManifestFileMeta::minRowId, ManifestFileMeta::maxRowId);
            this.metas = new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFileMetas));
        }

        List<ManifestEntry> scan() {
            List<ManifestEntry> result = new ArrayList<>();
            while (metas.peek() != null && result.size() < FILES_BATCH) {
                List<ManifestFileMeta> currentMetas = metas.poll();
                List<ManifestEntry> targetEntries =
                        currentMetas.stream()
                                .flatMap(meta -> snapshotReader.readManifest(meta).stream())
                                // we don't need stats for compaction
                                .map(ManifestEntry::copyWithoutStats)
                                .collect(Collectors.toList());
                result.addAll(targetEntries);
            }
            if (result.isEmpty()) {
                throw new EndOfScanException();
            }
            return result;
        }
    }

    /** Generate compaction tasks. */
    static class CompactPlanner {

        private final boolean compactBlob;
        private final long targetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;

        CompactPlanner(
                boolean compactBlob,
                long targetFileSize,
                long openFileCost,
                long compactMinFileNum) {
            this.compactBlob = compactBlob;
            this.targetFileSize = targetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
        }

        List<DataEvolutionCompactTask> compactPlan(List<ManifestEntry> input) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            Map<BinaryRow, List<DataFileMeta>> partitionedFiles = new LinkedHashMap<>();
            for (ManifestEntry entry : input) {
                partitionedFiles
                        .computeIfAbsent(entry.partition(), k -> new ArrayList<>())
                        .add(entry.file());
            }

            for (Map.Entry<BinaryRow, List<DataFileMeta>> partitionFiles :
                    partitionedFiles.entrySet()) {
                BinaryRow partition = partitionFiles.getKey();
                List<DataFileMeta> files = partitionFiles.getValue();
                RangeHelper<DataFileMeta> rangeHelper =
                        new RangeHelper<>(
                                DataFileMeta::nonNullFirstRowId,
                                // merge adjacent files
                                f -> f.nonNullFirstRowId() + f.rowCount());

                List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(files);

                for (List<DataFileMeta> group : ranges) {
                    List<DataFileMeta> dataFiles = new ArrayList<>();
                    List<DataFileMeta> blobFiles = new ArrayList<>();
                    TreeMap<Long, DataFileMeta> treeMap = new TreeMap<>();
                    Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles = new HashMap<>();
                    for (DataFileMeta f : group) {
                        if (!isBlobFile(f.fileName())) {
                            treeMap.put(f.nonNullFirstRowId(), f);
                            dataFiles.add(f);
                        } else {
                            blobFiles.add(f);
                        }
                    }

                    if (compactBlob) {
                        // associate blob files to data files
                        for (DataFileMeta blobFile : blobFiles) {
                            Long key = treeMap.floorKey(blobFile.nonNullFirstRowId());
                            if (key != null) {
                                DataFileMeta dataFile = treeMap.get(key);
                                if (blobFile.nonNullFirstRowId() >= dataFile.nonNullFirstRowId()
                                        && blobFile.nonNullFirstRowId()
                                                <= dataFile.nonNullFirstRowId()
                                                        + dataFile.rowCount()
                                                        - 1) {
                                    dataFileToBlobFiles
                                            .computeIfAbsent(dataFile, k -> new ArrayList<>())
                                            .add(blobFile);
                                }
                            }
                        }
                    }

                    RangeHelper<DataFileMeta> rangeHelper2 =
                            new RangeHelper<>(
                                    DataFileMeta::nonNullFirstRowId,
                                    // files group
                                    f -> f.nonNullFirstRowId() + f.rowCount() - 1);
                    List<List<DataFileMeta>> groupedFiles =
                            rangeHelper2.mergeOverlappingRanges(dataFiles);
                    List<DataFileMeta> waitCompactFiles = new ArrayList<>();

                    long weightSum = 0L;
                    for (List<DataFileMeta> fileGroup : groupedFiles) {
                        checkArgument(
                                rangeHelper.areAllRangesSame(fileGroup),
                                "Data files %s should be all row id ranges same.",
                                dataFiles);
                        long currentGroupWeight =
                                fileGroup.stream()
                                        .mapToLong(d -> Math.max(d.fileSize(), openFileCost))
                                        .sum();
                        if (currentGroupWeight > targetFileSize) {
                            // compact current file group to merge field files
                            tasks.addAll(triggerTask(fileGroup, partition, dataFileToBlobFiles));
                            // compact wait compact files
                            tasks.addAll(
                                    triggerTask(waitCompactFiles, partition, dataFileToBlobFiles));
                            waitCompactFiles = new ArrayList<>();
                            weightSum = 0;
                        } else {
                            weightSum += currentGroupWeight;
                            waitCompactFiles.addAll(fileGroup);
                            if (weightSum > targetFileSize) {
                                tasks.addAll(
                                        triggerTask(
                                                waitCompactFiles, partition, dataFileToBlobFiles));
                                waitCompactFiles = new ArrayList<>();
                                weightSum = 0L;
                            }
                        }
                    }
                    tasks.addAll(triggerTask(waitCompactFiles, partition, dataFileToBlobFiles));
                }
            }
            return tasks;
        }

        private List<DataEvolutionCompactTask> triggerTask(
                List<DataFileMeta> dataFiles,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            if (dataFiles.size() >= compactMinFileNum) {
                tasks.add(new DataEvolutionCompactTask(partition, dataFiles, false));
            }

            if (compactBlob) {
                List<DataFileMeta> blobFiles = new ArrayList<>();
                for (DataFileMeta dataFile : dataFiles) {
                    blobFiles.addAll(
                            dataFileToBlobFiles.getOrDefault(dataFile, Collections.emptyList()));
                }
                if (blobFiles.size() >= compactMinFileNum) {
                    tasks.add(new DataEvolutionCompactTask(partition, blobFiles, true));
                }
            }
            return tasks;
        }
    }
}
