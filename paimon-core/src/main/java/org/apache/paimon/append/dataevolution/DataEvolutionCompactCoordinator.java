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
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DeletionFile;
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
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.DataTypeRoot.BLOB;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.DataEvolutionUtils.toDeletionFiles;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact coordinator to compact data evolution table. */
public class DataEvolutionCompactCoordinator {

    private static final int FILES_BATCH = 100_000;
    private static final int BLOB_COMPACT_MIN_FILE_NUM = 2;

    private final CompactScanner scanner;
    private final CompactPlanner planner;

    public DataEvolutionCompactCoordinator(
            FileStoreTable table, boolean compactBlob, boolean compactVector, Snapshot snapshot) {
        this(table, null, compactBlob, compactVector, snapshot);
    }

    public DataEvolutionCompactCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean compactBlob,
            boolean compactVector,
            Snapshot snapshot) {
        CoreOptions options = table.coreOptions();

        long targetFileSize = options.targetFileSize(false);
        long openFileCost = options.splitOpenFileCost();
        long compactMinFileNum = options.compactionMinFileNum();
        Set<String> blobInlineFields = options.blobInlineField();

        this.scanner =
                new CompactScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate),
                        table.store().newScan().withPartitionFilter(partitionPredicate),
                        snapshot);
        boolean reassignRowIdAndMaterializeDeletions =
                options.deletionVectorsEnabled()
                        && options.dataEvolutionCompactionReassignRowIdAndMaterializeDeletions();
        this.planner =
                new CompactPlanner(
                        compactBlob,
                        compactVector,
                        reassignRowIdAndMaterializeDeletions,
                        table.store().newIndexFileHandler(),
                        scanner.snapshot(),
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

    public Snapshot snapshot() {
        return scanner.snapshot();
    }

    /** Scanner to generate sorted ManifestEntries. */
    static class CompactScanner {

        private final FileStoreScan scan;
        private final Snapshot snapshot;
        private final Queue<List<ManifestFileMeta>> metas;

        private CompactScanner(
                SnapshotReader snapshotReader, FileStoreScan scan, Snapshot snapshot) {
            this.scan = scan;
            this.snapshot = snapshot;

            List<ManifestFileMeta> manifestFileMetas =
                    snapshotReader
                            .manifestsReader()
                            .read(this.snapshot, ScanMode.ALL)
                            .filteredManifests;

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

        Snapshot snapshot() {
            return snapshot;
        }
    }

    /** Generate compaction tasks. */
    static class CompactPlanner {

        private final boolean compactBlob;
        private final boolean compactVector;
        private final boolean materializeDeletions;
        @Nullable private final IndexFileHandler indexFileHandler;
        @Nullable private final Snapshot snapshot;
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
                    false,
                    null,
                    null,
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
                boolean materializeDeletions,
                @Nullable IndexFileHandler indexFileHandler,
                @Nullable Snapshot snapshot,
                long targetFileSize,
                long blobTargetFileSize,
                long openFileCost,
                long compactMinFileNum,
                LongFunction<RowType> schemaFetcher,
                @Nullable Set<Integer> currentBlobFieldIds) {
            this.compactBlob = compactBlob;
            this.compactVector = compactVector;
            this.materializeDeletions = materializeDeletions;
            this.indexFileHandler = indexFileHandler;
            this.snapshot = snapshot;
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
                Map<String, DeletionFile> deletionFiles = deletionFiles(partition);
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

                    if (compactBlob || materializeDeletions) {
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
                    if (compactVector || materializeDeletions) {
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

                    CompactBin compactBin = new CompactBin(targetFileSize);
                    for (List<DataFileMeta> fileGroup : groupedFiles) {
                        checkArgument(
                                rangeHelper.areAllRangesSame(fileGroup),
                                "Data files %s should be all row id ranges same.",
                                dataFiles);
                        DataFileMeta anchor = retrieveAnchorFile(fileGroup, file -> file);
                        DeletionFile anchorDeletionFile =
                                materializeDeletions ? deletionFiles.get(anchor.fileName()) : null;

                        // use remaining records num ratio to estimate real file size
                        // of each normal file
                        long groupWeight = groupWeight(fileGroup, anchor, anchorDeletionFile);
                        if (groupWeight > targetFileSize) {
                            tasks.addAll(
                                    triggerTask(
                                            compactBin.drain(),
                                            partition,
                                            dataFileToBlobFiles,
                                            dataFileToVectorStoreFiles));
                            // compact current file group to merge field files
                            tasks.addAll(
                                    triggerTask(
                                            compactBin(
                                                    fileGroup,
                                                    anchor.fileName(),
                                                    anchorDeletionFile,
                                                    groupWeight),
                                            partition,
                                            dataFileToBlobFiles,
                                            dataFileToVectorStoreFiles));
                        } else {
                            compactBin.add(
                                    fileGroup, anchor.fileName(), anchorDeletionFile, groupWeight);
                            if (compactBin.enoughContent()) {
                                tasks.addAll(
                                        triggerTask(
                                                compactBin.drain(),
                                                partition,
                                                dataFileToBlobFiles,
                                                dataFileToVectorStoreFiles));
                            }
                        }
                    }
                    tasks.addAll(
                            triggerTask(
                                    compactBin.drain(),
                                    partition,
                                    dataFileToBlobFiles,
                                    dataFileToVectorStoreFiles));
                }
            }
            return tasks;
        }

        private List<DataEvolutionCompactTask> triggerTask(
                CompactBin compactBin,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles) {
            if (compactBin.isEmpty()) {
                return Collections.emptyList();
            }

            List<DataFileMeta> dataFiles = compactBin.files();
            if (compactBin.materializeDeletion()) {
                return triggerMaterializeTask(
                        dataFiles,
                        compactBin.anchorDeletionFiles(),
                        partition,
                        dataFileToBlobFiles,
                        dataFileToVectorStoreFiles);
            }
            return triggerNonMaterializeTask(
                    dataFiles, partition, dataFileToBlobFiles, dataFileToVectorStoreFiles);
        }

        private List<DataEvolutionCompactTask> triggerNonMaterializeTask(
                List<DataFileMeta> dataFiles,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles) {
            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            boolean triggerNormalFile = dataFiles.size() >= compactMinFileNum;
            if (triggerNormalFile) {
                tasks.add(new DataEvolutionNormalCompactTask(partition, dataFiles));
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
                        tasks.add(new DataEvolutionBlobCompactTask(partition, blobFilesToCompact));
                    }
                } else {
                    for (DataFileMeta dataFile : dataFiles) {
                        for (List<DataFileMeta> blobFilesToCompact :
                                blobFileGroupsToCompact(
                                        dataFileToBlobFiles.getOrDefault(
                                                dataFile, Collections.emptyList()))) {
                            tasks.add(
                                    new DataEvolutionBlobCompactTask(
                                            partition, blobFilesToCompact));
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
                        tasks.add(new DataEvolutionNormalCompactTask(partition, vectorStoreFiles));
                    }
                } else {
                    for (DataFileMeta dataFile : dataFiles) {
                        List<DataFileMeta> vectorStoreFiles =
                                dataFileToVectorStoreFiles.getOrDefault(
                                        dataFile, Collections.emptyList());
                        if (vectorStoreFiles.size() >= compactMinFileNum) {
                            tasks.add(
                                    new DataEvolutionNormalCompactTask(
                                            partition, vectorStoreFiles));
                        }
                    }
                }
            }
            return tasks;
        }

        private List<DataEvolutionCompactTask> triggerMaterializeTask(
                List<DataFileMeta> dataFiles,
                Map<String, DeletionFile> deletionFiles,
                BinaryRow partition,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles,
                Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles) {
            List<DataFileMeta> taskFiles = new ArrayList<>(dataFiles);
            for (DataFileMeta dataFile : dataFiles) {
                taskFiles.addAll(
                        dataFileToBlobFiles.getOrDefault(dataFile, Collections.emptyList()));
                List<DataFileMeta> vectorStoreFiles =
                        dataFileToVectorStoreFiles.getOrDefault(dataFile, Collections.emptyList());
                checkArgument(
                        vectorStoreFiles.isEmpty(),
                        "Materializing deletion vectors for vector-store files is not supported.");
            }

            List<DeletionFile> taskDeletionFiles = new ArrayList<>(taskFiles.size());
            for (DataFileMeta taskFile : taskFiles) {
                taskDeletionFiles.add(deletionFiles.get(taskFile.fileName()));
            }
            return Collections.singletonList(
                    new DataEvolutionMaterializeDeletionCompactTask(
                            partition, taskFiles, taskDeletionFiles));
        }

        private CompactBin compactBin(
                List<DataFileMeta> files,
                String anchorFileName,
                @Nullable DeletionFile anchorDeletionFile,
                long groupWeight) {
            CompactBin bin = new CompactBin(targetFileSize);
            bin.add(files, anchorFileName, anchorDeletionFile, groupWeight);
            return bin;
        }

        private long groupWeight(
                List<DataFileMeta> files,
                DataFileMeta anchor,
                @Nullable DeletionFile anchorDeletionFile) {
            double remainingRatio = remainingRatio(anchor, anchorDeletionFile);
            long weight = 0L;
            for (DataFileMeta file : files) {
                weight +=
                        Math.max((long) Math.ceil(file.fileSize() * remainingRatio), openFileCost);
            }
            return weight;
        }

        private double remainingRatio(DataFileMeta file, DeletionFile deletionFile) {
            Long cardinality = deletionFile == null ? null : deletionFile.cardinality();
            if (cardinality == null || file.rowCount() <= 0) {
                return 1D;
            }
            long visibleRows = Math.max(0L, file.rowCount() - cardinality);
            return ((double) visibleRows) / file.rowCount();
        }

        private Map<String, DeletionFile> deletionFiles(BinaryRow partition) {
            if (!materializeDeletions || snapshot == null || indexFileHandler == null) {
                return Collections.emptyMap();
            }
            List<IndexFileMeta> indexFiles =
                    indexFileHandler.scan(
                            snapshot, DELETION_VECTORS_INDEX, partition, UNAWARE_BUCKET);
            return toDeletionFiles(indexFileHandler, partition, UNAWARE_BUCKET, indexFiles);
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

    private static class CompactBin {

        private final List<DataFileMeta> files = new ArrayList<>();
        private final Map<String, DeletionFile> anchorDeletionFiles = new HashMap<>();
        private final long targetFileSize;

        private long weight = 0L;

        CompactBin(long targetFileSize) {
            this.targetFileSize = targetFileSize;
        }

        private void add(
                List<DataFileMeta> files,
                String anchorFileName,
                @Nullable DeletionFile anchorDeletionFile,
                long weight) {
            this.files.addAll(files);
            this.weight += weight;
            if (anchorDeletionFile != null) {
                this.anchorDeletionFiles.put(anchorFileName, anchorDeletionFile);
            }
        }

        private boolean isEmpty() {
            return files.isEmpty();
        }

        private boolean materializeDeletion() {
            return !anchorDeletionFiles.isEmpty();
        }

        private List<DataFileMeta> files() {
            return files;
        }

        private Map<String, DeletionFile> anchorDeletionFiles() {
            return anchorDeletionFiles;
        }

        private boolean enoughContent() {
            return weight > targetFileSize;
        }

        private CompactBin drain() {
            CompactBin result = new CompactBin(targetFileSize);
            result.files.addAll(files);
            result.anchorDeletionFiles.putAll(anchorDeletionFiles);
            result.weight = weight;
            files.clear();
            anchorDeletionFiles.clear();
            weight = 0L;
            return result;
        }
    }
}
