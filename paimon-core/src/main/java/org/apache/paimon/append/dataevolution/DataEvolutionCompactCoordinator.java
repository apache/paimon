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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.types.DataTypeRoot.BLOB;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact coordinator to compact data evolution table. */
public class DataEvolutionCompactCoordinator {

    private static final int FILES_BATCH = 100_000;
    private static final int BLOB_COMPACT_MIN_FILE_NUM = 2;

    private final CompactScanner scanner;
    private final CompactPlanner planner;

    public DataEvolutionCompactCoordinator(
            FileStoreTable table, boolean compactBlob, boolean compactVector) {
        this(table, null, compactBlob, compactVector);
    }

    public DataEvolutionCompactCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean compactBlob,
            boolean compactVector) {
        CoreOptions options = table.coreOptions();
        long targetFileSize = options.targetFileSize(false);
        long openFileCost = options.splitOpenFileCost();
        long compactMinFileNum = options.compactionMinFileNum();
        Set<String> blobInlineFields = options.blobInlineField();

        this.scanner =
                new CompactScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate),
                        table.store().newScan());
        this.planner =
                new CompactPlanner(
                        compactBlob,
                        compactVector,
                        targetFileSize,
                        options.blobTargetFileSize(),
                        openFileCost,
                        compactMinFileNum,
                        schemaId -> table.schemaManager().schema(schemaId).logicalRowType(),
                        compactBlob
                                ? table.rowType().getFields().stream()
                                        .filter(
                                                field ->
                                                        field.type().is(BLOB)
                                                                && !blobInlineFields.contains(
                                                                        field.name()))
                                        .map(DataField::id)
                                        .collect(Collectors.toSet())
                                : null);
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

        private final FileStoreScan scan;
        private final Queue<List<ManifestFileMeta>> metas;

        private CompactScanner(SnapshotReader snapshotReader, FileStoreScan scan) {
            this.scan = scan;
            Snapshot snapshot = snapshotReader.snapshotManager().latestSnapshot();

            List<ManifestFileMeta> manifestFileMetas =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;

            if (allContainsRowId(manifestFileMetas)) {
                RangeHelper<ManifestFileMeta> rangeHelper =
                        new RangeHelper<>(
                                manifest -> new Range(manifest.minRowId(), manifest.maxRowId()));
                this.metas =
                        new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFileMetas));
            } else {
                this.metas = new ArrayDeque<>(Collections.singletonList(manifestFileMetas));
            }
        }

        List<ManifestEntry> scan() {
            List<ManifestEntry> result = new ArrayList<>();
            while (metas.peek() != null && result.size() < FILES_BATCH) {
                List<ManifestFileMeta> currentMetas = metas.poll();
                scan.readFileIterator(currentMetas)
                        .forEachRemaining(entry -> result.add(entry.copyWithoutStats()));
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
        private final boolean compactVector;
        private final long targetFileSize;
        private final long blobTargetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;
        private final LongFunction<RowType> schemaFetcher;
        @Nullable private final Set<Integer> currentBlobFieldIds;

        @VisibleForTesting
        CompactPlanner(
                boolean compactBlob,
                boolean compactVector,
                long targetFileSize,
                long openFileCost,
                long compactMinFileNum) {
            this(
                    compactBlob,
                    compactVector,
                    targetFileSize,
                    targetFileSize,
                    openFileCost,
                    compactMinFileNum,
                    schemaId -> {
                        throw new IllegalStateException(
                                "Schema fetcher is required for blob compaction.");
                    },
                    null);
        }

        CompactPlanner(
                boolean compactBlob,
                boolean compactVector,
                long targetFileSize,
                long blobTargetFileSize,
                long openFileCost,
                long compactMinFileNum,
                LongFunction<RowType> schemaFetcher,
                @Nullable Set<Integer> currentBlobFieldIds) {
            this.compactBlob = compactBlob;
            this.compactVector = compactVector;
            this.targetFileSize = targetFileSize;
            this.blobTargetFileSize = blobTargetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
            Map<Long, RowType> schemaCache = new HashMap<>();
            this.schemaFetcher =
                    schemaId -> schemaCache.computeIfAbsent(schemaId, schemaFetcher::apply);
            this.currentBlobFieldIds = currentBlobFieldIds;
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
                                f ->
                                        new Range(
                                                f.nonNullFirstRowId(),
                                                // merge adjacent files
                                                f.nonNullFirstRowId() + f.rowCount()));

                List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(files);

                for (List<DataFileMeta> group : ranges) {
                    List<DataFileMeta> dataFiles = new ArrayList<>();
                    List<DataFileMeta> blobFiles = new ArrayList<>();
                    List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
                    TreeMap<Long, DataFileMeta> treeMap = new TreeMap<>();
                    Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles = new HashMap<>();
                    Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles =
                            new HashMap<>();
                    for (DataFileMeta f : group) {
                        if (isBlobFile(f.fileName())) {
                            blobFiles.add(f);
                        } else if (isVectorStoreFile(f.fileName())) {
                            vectorStoreFiles.add(f);
                        } else {
                            treeMap.put(f.nonNullFirstRowId(), f);
                            dataFiles.add(f);
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
                    if (compactVector) {
                        // associate vector-store files to data files
                        for (DataFileMeta vectorStoreFile : vectorStoreFiles) {
                            Long key = treeMap.floorKey(vectorStoreFile.nonNullFirstRowId());
                            if (key != null) {
                                DataFileMeta dataFile = treeMap.get(key);
                                if (vectorStoreFile.nonNullFirstRowId()
                                                >= dataFile.nonNullFirstRowId()
                                        && vectorStoreFile.nonNullFirstRowId()
                                                <= dataFile.nonNullFirstRowId()
                                                        + dataFile.rowCount()
                                                        - 1) {
                                    dataFileToVectorStoreFiles
                                            .computeIfAbsent(dataFile, k -> new ArrayList<>())
                                            .add(vectorStoreFile);
                                }
                            }
                        }
                    }

                    RangeHelper<DataFileMeta> rangeHelper2 =
                            new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
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
                            tasks.addAll(
                                    triggerTask(
                                            fileGroup,
                                            partition,
                                            dataFileToBlobFiles,
                                            dataFileToVectorStoreFiles));
                            // compact wait compact files
                            tasks.addAll(
                                    triggerTask(
                                            waitCompactFiles,
                                            partition,
                                            dataFileToBlobFiles,
                                            dataFileToVectorStoreFiles));
                            waitCompactFiles = new ArrayList<>();
                            weightSum = 0;
                        } else {
                            weightSum += currentGroupWeight;
                            waitCompactFiles.addAll(fileGroup);
                            if (weightSum > targetFileSize) {
                                tasks.addAll(
                                        triggerTask(
                                                waitCompactFiles,
                                                partition,
                                                dataFileToBlobFiles,
                                                dataFileToVectorStoreFiles));
                                waitCompactFiles = new ArrayList<>();
                                weightSum = 0L;
                            }
                        }
                    }
                    tasks.addAll(
                            triggerTask(
                                    waitCompactFiles,
                                    partition,
                                    dataFileToBlobFiles,
                                    dataFileToVectorStoreFiles));
                }
            }
            return tasks;
        }

        private List<DataEvolutionCompactTask> triggerTask(
                List<DataFileMeta> dataFiles,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            boolean triggerNormalFile = dataFiles.size() >= compactMinFileNum;
            if (triggerNormalFile) {
                tasks.add(new DataEvolutionCompactTask(partition, dataFiles, false));
            }

            if (compactBlob) {
                if (triggerNormalFile) {
                    List<DataFileMeta> blobFiles = new ArrayList<>();
                    for (DataFileMeta dataFile : dataFiles) {
                        blobFiles.addAll(
                                dataFileToBlobFiles.getOrDefault(
                                        dataFile, Collections.emptyList()));
                    }
                    for (List<DataFileMeta> blobFilesToCompact :
                            blobFileGroupsToCompact(blobFiles)) {
                        tasks.add(
                                new DataEvolutionCompactTask(partition, blobFilesToCompact, true));
                    }
                } else {
                    for (DataFileMeta dataFile : dataFiles) {
                        for (List<DataFileMeta> blobFilesToCompact :
                                blobFileGroupsToCompact(
                                        dataFileToBlobFiles.getOrDefault(
                                                dataFile, Collections.emptyList()))) {
                            tasks.add(
                                    new DataEvolutionCompactTask(
                                            partition, blobFilesToCompact, true));
                        }
                    }
                }
            }

            if (compactVector) {
                if (triggerNormalFile) {
                    List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
                    for (DataFileMeta dataFile : dataFiles) {
                        vectorStoreFiles.addAll(
                                dataFileToVectorStoreFiles.getOrDefault(
                                        dataFile, Collections.emptyList()));
                    }
                    if (vectorStoreFiles.size() >= compactMinFileNum) {
                        tasks.add(new DataEvolutionCompactTask(partition, vectorStoreFiles, false));
                    }
                } else {
                    for (DataFileMeta dataFile : dataFiles) {
                        List<DataFileMeta> vectorStoreFiles =
                                dataFileToVectorStoreFiles.getOrDefault(
                                        dataFile, Collections.emptyList());
                        if (vectorStoreFiles.size() >= compactMinFileNum) {
                            tasks.add(
                                    new DataEvolutionCompactTask(
                                            partition, vectorStoreFiles, false));
                        }
                    }
                }
            }
            return tasks;
        }

        private List<List<DataFileMeta>> blobFileGroupsToCompact(List<DataFileMeta> blobFiles) {
            Map<Integer, List<DataFileMeta>> fieldIdToFiles = new LinkedHashMap<>();
            for (DataFileMeta blobFile : blobFiles) {
                int fieldId = blobFieldId(blobFile);
                if (currentBlobFieldIds == null || currentBlobFieldIds.contains(fieldId)) {
                    fieldIdToFiles.computeIfAbsent(fieldId, key -> new ArrayList<>()).add(blobFile);
                }
            }

            List<List<DataFileMeta>> result = new ArrayList<>();
            for (List<DataFileMeta> files : fieldIdToFiles.values()) {
                result.addAll(fileGroupsToCompact(files));
            }
            return result;
        }

        private List<List<DataFileMeta>> fileGroupsToCompact(List<DataFileMeta> files) {
            List<List<DataFileMeta>> result = new ArrayList<>();
            List<DataFileMeta> sortedFiles = new ArrayList<>(files);
            sortedFiles.sort(
                    comparingLong(DataFileMeta::nonNullFirstRowId)
                            .thenComparingLong(DataFileMeta::maxSequenceNumber));

            RangeHelper<DataFileMeta> rangeHelper =
                    new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
            List<DataFileMeta> smallFileCandidates = new ArrayList<>();
            for (List<DataFileMeta> rowRangeGroup :
                    rangeHelper.mergeOverlappingRanges(sortedFiles)) {
                if (rowRangeGroup.size() >= BLOB_COMPACT_MIN_FILE_NUM) {
                    rowRangeGroup.sort(
                            comparingLong(DataFileMeta::nonNullFirstRowId)
                                    .thenComparingLong(DataFileMeta::maxSequenceNumber));
                    result.add(rowRangeGroup);
                } else {
                    smallFileCandidates.add(rowRangeGroup.get(0));
                }
            }

            result.addAll(smallFileGroupsToCompact(smallFileCandidates));
            result.sort(comparingLong(group -> group.get(0).nonNullFirstRowId()));
            return result;
        }

        private List<List<DataFileMeta>> smallFileGroupsToCompact(List<DataFileMeta> files) {
            List<List<DataFileMeta>> result = new ArrayList<>();

            List<DataFileMeta> continuousFiles = new ArrayList<>();
            long expectedFirstRowId = -1;
            for (DataFileMeta file : files) {
                if (file.fileSize() >= blobTargetFileSize) {
                    addFileGroupsToCompact(result, continuousFiles);
                    continuousFiles.clear();
                    expectedFirstRowId = -1;
                    continue;
                }

                long firstRowId = file.nonNullFirstRowId();
                if (!continuousFiles.isEmpty() && firstRowId != expectedFirstRowId) {
                    addFileGroupsToCompact(result, continuousFiles);
                    continuousFiles.clear();
                }
                continuousFiles.add(file);
                expectedFirstRowId = firstRowId + file.rowCount();
            }
            addFileGroupsToCompact(result, continuousFiles);
            return result;
        }

        private void addFileGroupsToCompact(
                List<List<DataFileMeta>> result, List<DataFileMeta> continuousFiles) {
            if (continuousFiles.size() < BLOB_COMPACT_MIN_FILE_NUM) {
                return;
            }
            List<DataFileMeta> taskFiles = new ArrayList<>();
            long fileSize = 0L;
            for (DataFileMeta file : continuousFiles) {
                taskFiles.add(file);
                fileSize += file.fileSize();
                if (fileSize >= blobTargetFileSize
                        && taskFiles.size() >= BLOB_COMPACT_MIN_FILE_NUM) {
                    result.add(taskFiles);
                    taskFiles = new ArrayList<>();
                    fileSize = 0L;
                }
            }

            if (taskFiles.size() >= BLOB_COMPACT_MIN_FILE_NUM) {
                result.add(taskFiles);
            }
        }

        private int blobFieldId(DataFileMeta blobFile) {
            checkArgument(
                    blobFile.writeCols() != null && blobFile.writeCols().size() == 1,
                    "Blob file %s should contain exactly one write column.",
                    blobFile);
            RowType rowType = schemaFetcher.apply(blobFile.schemaId());
            return rowType.getField(blobFile.writeCols().get(0)).id();
        }
    }
}
