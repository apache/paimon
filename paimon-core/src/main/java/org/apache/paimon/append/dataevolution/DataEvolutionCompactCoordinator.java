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
import org.apache.paimon.append.dataevolution.DataEvolutionCompactRangePlanner.RangeBatch;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.ManifestsReader;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.BlobType.isBlobFileField;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact coordinator to compact data evolution table. */
public class DataEvolutionCompactCoordinator {

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
        this(
                table,
                partitionPredicate,
                compactBlob,
                compactVector,
                snapshot,
                DataEvolutionCompactRangePlanner.CANDIDATE_FILES_PER_BATCH);
    }

    @VisibleForTesting
    public DataEvolutionCompactCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean compactBlob,
            boolean compactVector,
            Snapshot snapshot,
            int candidateFilesPerBatch) {
        CoreOptions options = table.coreOptions();
        validateOptions(options);

        long targetFileSize = options.targetFileSize(false);
        long openFileCost = options.splitOpenFileCost();
        long compactMinFileNum = options.compactionMinFileNum();
        Set<String> blobInlineFields = options.blobInlineField();
        LongFunction<RowType> schemaFetcher =
                schemaId -> table.schemaManager().schema(schemaId).logicalRowType();
        Set<Integer> currentBlobFieldIds =
                compactBlob
                        ? table.rowType().getFields().stream()
                                .filter(
                                        field ->
                                                isBlobFileField(field.type())
                                                        && !blobInlineFields.contains(field.name()))
                                .map(DataField::id)
                                .collect(Collectors.toSet())
                        : null;
        DataEvolutionCompactRangePlanner.CandidateOptions candidateOptions =
                new DataEvolutionCompactRangePlanner.CandidateOptions(
                        compactBlob,
                        compactVector,
                        targetFileSize,
                        options.blobTargetFileSize(),
                        openFileCost,
                        compactMinFileNum,
                        schemaFetcher,
                        currentBlobFieldIds);

        this.scanner =
                new CompactScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate),
                        table.store().newScan().withPartitionFilter(partitionPredicate).dropStats(),
                        table.store().manifestFileFactory(),
                        snapshot,
                        candidateOptions,
                        candidateFilesPerBatch);
        this.planner =
                new CompactPlanner(
                        compactBlob,
                        compactVector,
                        targetFileSize,
                        options.blobTargetFileSize(),
                        openFileCost,
                        compactMinFileNum,
                        schemaFetcher,
                        currentBlobFieldIds);
    }

    public static void validateOptions(CoreOptions options) {
        checkArgument(
                !options.dataEvolutionCompactionRewriteRowIds(),
                "Option '%s=true' is no longer supported. Data evolution compaction preserves "
                        + "row IDs and logical deletions. Use the 'materialize_deletion_vectors' "
                        + "procedure to apply deletion vectors to the latest table state and "
                        + "assign new row IDs.",
                CoreOptions.DATA_EVOLUTION_COMPACTION_REWRITE_ROW_IDS.key());
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
        private final Iterator<RangeBatch> rangeBatches;
        private boolean nonEmptyResultReturned;
        private boolean emptyResultReturned;

        private CompactScanner(
                SnapshotReader snapshotReader,
                FileStoreScan scan,
                ManifestFile.Factory manifestFileFactory,
                Snapshot snapshot,
                DataEvolutionCompactRangePlanner.CandidateOptions candidateOptions,
                int candidateFilesPerBatch) {
            this.scan = scan;
            this.snapshot = snapshot;

            ManifestsReader manifestsReader = snapshotReader.manifestsReader();
            List<ManifestFileMeta> manifestFileMetas =
                    manifestsReader.read(this.snapshot, ScanMode.ALL).filteredManifests;
            this.rangeBatches =
                    new DataEvolutionCompactRangePlanner(
                                    manifestFileFactory.create(),
                                    manifestsReader.partitionFilter(),
                                    candidateFilesPerBatch,
                                    candidateOptions)
                            .plan(manifestFileMetas);
        }

        List<ManifestEntry> scan() {
            RangeBatch rangeBatch = rangeBatches.hasNext() ? rangeBatches.next() : null;
            if (rangeBatch == null) {
                if (!nonEmptyResultReturned && !emptyResultReturned) {
                    emptyResultReturned = true;
                    return Collections.emptyList();
                }
                throw new EndOfScanException();
            }

            List<ManifestEntry> result = new ArrayList<>(rangeBatch.fileCount());
            scan.withRowRanges(rangeBatch.toRanges());
            scan.readFileIterator(rangeBatch.manifestFiles()).forEachRemaining(result::add);
            if (result.isEmpty()) {
                if (!nonEmptyResultReturned && !emptyResultReturned) {
                    emptyResultReturned = true;
                    return result;
                }
                throw new EndOfScanException();
            }
            nonEmptyResultReturned = true;
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
                List<DataFileMeta> dataFiles = new ArrayList<>();
                List<DataFileMeta> blobFiles = new ArrayList<>();
                List<DataFileMeta> vectorStoreFiles = new ArrayList<>();
                TreeMap<Long, DataFileMeta> treeMap = new TreeMap<>();
                Map<DataFileMeta, List<DataFileMeta>> dataFileToBlobFiles = new HashMap<>();
                Map<DataFileMeta, List<DataFileMeta>> dataFileToVectorStoreFiles = new HashMap<>();
                for (DataFileMeta file : files) {
                    if (isBlobFile(file.fileName())) {
                        blobFiles.add(file);
                    } else if (isVectorStoreFile(file.fileName())) {
                        vectorStoreFiles.add(file);
                    } else {
                        treeMap.put(file.nonNullFirstRowId(), file);
                        dataFiles.add(file);
                    }
                }

                if (compactBlob) {
                    associateDedicatedFiles(blobFiles, treeMap, dataFileToBlobFiles);
                }
                if (compactVector) {
                    associateDedicatedFiles(vectorStoreFiles, treeMap, dataFileToVectorStoreFiles);
                }

                RangeHelper<DataFileMeta> continuousDataRangeHelper =
                        new RangeHelper<>(
                                f ->
                                        new Range(
                                                f.nonNullFirstRowId(),
                                                // merge adjacent files
                                                f.nonNullFirstRowId() + f.rowCount()));

                for (List<DataFileMeta> continuousDataFiles :
                        continuousDataRangeHelper.mergeOverlappingRanges(dataFiles)) {
                    RangeHelper<DataFileMeta> logicalRangeHelper =
                            new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
                    List<List<DataFileMeta>> groupedFiles =
                            logicalRangeHelper.mergeOverlappingRanges(continuousDataFiles);

                    CompactBin compactBin = new CompactBin(targetFileSize);
                    for (List<DataFileMeta> fileGroup : groupedFiles) {
                        checkArgument(
                                logicalRangeHelper.areAllRangesSame(fileGroup),
                                "Data files %s should be all row id ranges same.",
                                continuousDataFiles);
                        long groupWeight = groupWeight(fileGroup);
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
                                            compactBin(fileGroup, groupWeight),
                                            partition,
                                            dataFileToBlobFiles,
                                            dataFileToVectorStoreFiles));
                        } else {
                            compactBin.add(fileGroup, groupWeight);
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

        private void associateDedicatedFiles(
                List<DataFileMeta> dedicatedFiles,
                TreeMap<Long, DataFileMeta> dataFilesByFirstRowId,
                Map<DataFileMeta, List<DataFileMeta>> association) {
            for (DataFileMeta dedicatedFile : dedicatedFiles) {
                Long key = dataFilesByFirstRowId.floorKey(dedicatedFile.nonNullFirstRowId());
                if (key == null) {
                    continue;
                }
                DataFileMeta dataFile = dataFilesByFirstRowId.get(key);
                if (dedicatedFile.nonNullFirstRowId() <= dataFile.nonNullRowIdRange().to) {
                    association
                            .computeIfAbsent(dataFile, ignored -> new ArrayList<>())
                            .add(dedicatedFile);
                }
            }
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

        private CompactBin compactBin(List<DataFileMeta> files, long groupWeight) {
            CompactBin bin = new CompactBin(targetFileSize);
            bin.add(files, groupWeight);
            return bin;
        }

        private long groupWeight(List<DataFileMeta> files) {
            long weight = 0L;
            for (DataFileMeta file : files) {
                weight += Math.max(file.fileSize(), openFileCost);
            }
            return weight;
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
            List<List<DataFileMeta>> continuousOrOverlapFiles = new ArrayList<>();
            long expectedFirstRowId = -1L;
            for (List<DataFileMeta> rowRangeGroup :
                    rangeHelper.mergeOverlappingRanges(sortedFiles)) {
                List<Range> rowRanges =
                        rowRangeGroup.stream()
                                .map(DataFileMeta::nonNullRowIdRange)
                                .collect(Collectors.toList());
                Range rowRange = Range.sortAndMergeOverlap(rowRanges).get(0);
                long firstRowId = rowRange.from;
                if (!continuousOrOverlapFiles.isEmpty() && firstRowId != expectedFirstRowId) {
                    addFileGroupsToCompact(result, continuousOrOverlapFiles);
                    continuousOrOverlapFiles.clear();
                }

                continuousOrOverlapFiles.add(rowRangeGroup);
                expectedFirstRowId = rowRange.to + 1;
            }
            addFileGroupsToCompact(result, continuousOrOverlapFiles);
            result.sort(comparingLong(group -> group.get(0).nonNullFirstRowId()));
            return result;
        }

        private void addFileGroupsToCompact(
                List<List<DataFileMeta>> result,
                List<List<DataFileMeta>> continuousOrOverlapFiles) {
            int compactFileCount = continuousOrOverlapFiles.stream().mapToInt(List::size).sum();
            if (compactFileCount < BLOB_COMPACT_MIN_FILE_NUM) {
                return;
            }

            List<DataFileMeta> taskFiles = new ArrayList<>();
            long taskFileSize = 0L;
            for (List<DataFileMeta> fileGroup : continuousOrOverlapFiles) {
                if (fileGroup.size() == 1 && fileGroup.get(0).fileSize() >= blobTargetFileSize) {
                    if (taskFiles.size() >= BLOB_COMPACT_MIN_FILE_NUM) {
                        result.add(taskFiles);
                    }
                    taskFiles = new ArrayList<>();
                    taskFileSize = 0L;
                    continue;
                }

                taskFiles.addAll(fileGroup);
                taskFileSize += fileGroup.stream().mapToLong(DataFileMeta::fileSize).sum();
                if (taskFileSize >= blobTargetFileSize
                        && taskFiles.size() >= BLOB_COMPACT_MIN_FILE_NUM) {
                    result.add(taskFiles);
                    taskFiles = new ArrayList<>();
                    taskFileSize = 0L;
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
        private final long targetFileSize;

        private long weight = 0L;

        CompactBin(long targetFileSize) {
            this.targetFileSize = targetFileSize;
        }

        private void add(List<DataFileMeta> files, long weight) {
            this.files.addAll(files);
            this.weight += weight;
        }

        private boolean isEmpty() {
            return files.isEmpty();
        }

        private List<DataFileMeta> files() {
            return files;
        }

        private boolean enoughContent() {
            return weight > targetFileSize;
        }

        private CompactBin drain() {
            CompactBin result = new CompactBin(targetFileSize);
            result.files.addAll(files);
            result.weight = weight;
            files.clear();
            weight = 0L;
            return result;
        }
    }
}
