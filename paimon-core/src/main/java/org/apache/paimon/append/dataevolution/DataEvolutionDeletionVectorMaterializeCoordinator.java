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
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryIndexManifestEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.PrimitiveRowRanges;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Plans tasks which apply deletion vectors to the latest table state and assign new row IDs. */
public class DataEvolutionDeletionVectorMaterializeCoordinator {

    // Soft target. One overlapping row-id component can exceed it.
    private static final int DELETION_FILES_PER_BATCH = 100_000;

    private final MaterializeScanner scanner;
    private final MaterializePlanner planner;

    public DataEvolutionDeletionVectorMaterializeCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            Snapshot snapshot) {
        this(table, partitionPredicate, snapshot, DELETION_FILES_PER_BATCH);
    }

    @VisibleForTesting
    public DataEvolutionDeletionVectorMaterializeCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            Snapshot snapshot,
            int deletionFilesPerBatch) {
        CoreOptions options = table.coreOptions();
        checkArgument(
                options.dataEvolutionEnabled(),
                "Materializing deletion vectors requires a data evolution table.");
        checkArgument(
                options.deletionVectorsEnabled(),
                "Materializing deletion vectors requires deletion vectors to be enabled.");

        this.scanner =
                new MaterializeScanner(table, partitionPredicate, snapshot, deletionFilesPerBatch);
        this.planner =
                new MaterializePlanner(options.targetFileSize(false), options.splitOpenFileCost());
    }

    public List<DataEvolutionCompactTask> plan() {
        return planner.plan(scanner.scan());
    }

    public Snapshot snapshot() {
        return scanner.snapshot;
    }

    private static class MaterializeScanner {

        private final FileStoreTable table;
        @Nullable private final PartitionPredicate partitionPredicate;
        private final IndexFileHandler indexFileHandler;
        private final Snapshot snapshot;
        private final int deletionFilesPerBatch;
        private boolean scanned;

        private MaterializeScanner(
                FileStoreTable table,
                @Nullable PartitionPredicate partitionPredicate,
                Snapshot snapshot,
                int deletionFilesPerBatch) {
            this.table = table;
            this.partitionPredicate = partitionPredicate;
            this.indexFileHandler = table.store().newIndexFileHandler();
            this.snapshot = snapshot;
            checkArgument(deletionFilesPerBatch > 0, "Deletion files per batch must be positive.");
            this.deletionFilesPerBatch = deletionFilesPerBatch;
        }

        private MaterializeScanBatch scan() {
            if (scanned) {
                throw new EndOfScanException();
            }
            scanned = true;

            Map<BinaryRow, Map<String, DeletionFile>> candidates = scanCandidates();
            if (candidates.isEmpty()) {
                throw new EndOfScanException();
            }
            Set<String> candidateFileNames = new HashSet<>();
            candidates.values().forEach(files -> candidateFileNames.addAll(files.keySet()));

            SnapshotReader snapshotReader =
                    table.newSnapshotReader().withPartitionFilter(partitionPredicate);
            List<ManifestFileMeta> manifests =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;
            FileStoreScan anchorScan =
                    table.store().newScan().withPartitionFilter(partitionPredicate).dropStats();
            FileStoreScan rangeScan =
                    table.store().newScan().withPartitionFilter(partitionPredicate).dropStats();
            anchorScan.withDataFileNameFilter(candidateFileNames::contains);
            List<ManifestEntry> anchors = new ArrayList<>(candidateFileNames.size());
            anchorScan.readFileIterator(manifests).forEachRemaining(anchors::add);

            Map<BinaryRow, Set<String>> missing = new LinkedHashMap<>();
            for (Map.Entry<BinaryRow, Map<String, DeletionFile>> candidate :
                    candidates.entrySet()) {
                missing.put(candidate.getKey(), new HashSet<>(candidate.getValue().keySet()));
            }
            List<Range> ranges = new ArrayList<>(anchors.size());
            for (ManifestEntry anchor : anchors) {
                Set<String> partitionMissing = missing.get(anchor.partition());
                if (partitionMissing == null
                        || !partitionMissing.remove(anchor.file().fileName())) {
                    continue;
                }
                checkArgument(
                        !isBlobFile(anchor.file().fileName())
                                && !isVectorStoreFile(anchor.file().fileName()),
                        "Deletion vector anchor '%s' must be a normal data file.",
                        anchor.file().fileName());
                ranges.add(anchor.file().nonNullRowIdRange());
            }
            List<String> missingFiles =
                    missing.values().stream().flatMap(Set::stream).collect(Collectors.toList());
            checkState(
                    missingFiles.isEmpty(),
                    "Cannot find live data files for deletion vectors: %s",
                    missingFiles);

            ranges = Range.sortAndMergeOverlap(ranges);
            List<ManifestEntry> entries = scanOverlappingRangeClosure(rangeScan, manifests, ranges);

            Map<BinaryRow, Map<String, DeletionFile>> batchDeletionFiles = scanForEntries(entries);
            checkState(
                    containsAll(batchDeletionFiles, candidates),
                    "Deletion-vector row ranges do not contain all selected anchor files.");
            return new MaterializeScanBatch(entries, batchDeletionFiles);
        }

        private List<ManifestEntry> scanOverlappingRangeClosure(
                FileStoreScan rangeScan,
                List<ManifestFileMeta> manifests,
                List<Range> initialRanges) {
            List<Range> coveredRanges = initialRanges;
            List<Range> frontier = initialRanges;
            Map<FileEntry.Identifier, ManifestEntry> result = new LinkedHashMap<>();
            while (!frontier.isEmpty()) {
                rangeScan.withRowRanges(frontier);
                List<ManifestEntry> scannedEntries = new ArrayList<>();
                rangeScan
                        .readFileIterator(manifestsForRanges(manifests, frontier))
                        .forEachRemaining(scannedEntries::add);

                List<Range> expandedRanges = new ArrayList<>(coveredRanges);
                for (ManifestEntry entry : scannedEntries) {
                    result.putIfAbsent(entry.identifier(), entry);
                    // A dedicated file can transitively bridge multiple normal-file ranges.
                    expandedRanges.add(entry.file().nonNullRowIdRange());
                }
                expandedRanges = Range.sortAndMergeOverlap(expandedRanges);
                frontier = subtractRanges(expandedRanges, coveredRanges);
                coveredRanges = expandedRanges;
            }
            return new ArrayList<>(result.values());
        }

        private Map<BinaryRow, Map<String, DeletionFile>> scanCandidates() {
            Map<BinaryRow, Map<String, DeletionFile>> candidates = new LinkedHashMap<>();
            int count = 0;
            try (CloseableIterator<BinaryIndexManifestEntry> entries =
                    indexFileHandler.scan(snapshot, BinaryIndexManifestEntry.FULL_PROJECTION)) {
                while (entries.hasNext() && count < deletionFilesPerBatch) {
                    IndexManifestEntry entry = entries.next().copy();
                    if (!isDeletionVectorEntry(entry)) {
                        continue;
                    }
                    LinkedHashMap<String, DeletionVectorMeta> deletionVectors =
                            entry.indexFile().dvRanges();
                    if (deletionVectors == null) {
                        continue;
                    }
                    Map<String, DeletionFile> partitionCandidates =
                            candidates.computeIfAbsent(
                                    entry.partition(), ignored -> new LinkedHashMap<>());
                    String path = indexFileHandler.filePath(entry).toString();
                    for (DeletionVectorMeta deletionVector : deletionVectors.values()) {
                        checkState(
                                partitionCandidates.put(
                                                deletionVector.dataFileName(),
                                                toDeletionFile(path, deletionVector))
                                        == null,
                                "Duplicate deletion vector for data file '%s'.",
                                deletionVector.dataFileName());
                        count++;
                        if (count >= deletionFilesPerBatch) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to scan deletion vectors.", e);
            }
            return candidates;
        }

        private List<ManifestFileMeta> manifestsForRanges(
                List<ManifestFileMeta> manifests, List<Range> ranges) {
            if (!allContainsRowId(manifests)) {
                return manifests;
            }
            PrimitiveRowRanges rangeIndex = new PrimitiveRowRanges(ranges.size());
            ranges.forEach(range -> rangeIndex.add(range.from, range.to));
            return manifests.stream()
                    .filter(
                            manifest ->
                                    rangeIndex.overlaps(manifest.minRowId(), manifest.maxRowId()))
                    .collect(Collectors.toList());
        }

        private Map<BinaryRow, Map<String, DeletionFile>> scanForEntries(
                List<ManifestEntry> dataEntries) {
            Map<BinaryRow, Set<String>> selectedFiles = new LinkedHashMap<>();
            for (ManifestEntry entry : dataEntries) {
                selectedFiles
                        .computeIfAbsent(entry.partition(), ignored -> new HashSet<>())
                        .add(entry.file().fileName());
            }
            Map<BinaryRow, Map<String, DeletionFile>> result = new LinkedHashMap<>();
            try (CloseableIterator<BinaryIndexManifestEntry> entries =
                    indexFileHandler.scan(snapshot, BinaryIndexManifestEntry.FULL_PROJECTION)) {
                while (entries.hasNext()) {
                    IndexManifestEntry entry = entries.next().copy();
                    if (!isDeletionVectorEntry(entry)) {
                        continue;
                    }
                    Set<String> selectedPartitionFiles = selectedFiles.get(entry.partition());
                    LinkedHashMap<String, DeletionVectorMeta> deletionVectors =
                            entry.indexFile().dvRanges();
                    if (selectedPartitionFiles == null || deletionVectors == null) {
                        continue;
                    }
                    String path = indexFileHandler.filePath(entry).toString();
                    for (DeletionVectorMeta deletionVector : deletionVectors.values()) {
                        if (!selectedPartitionFiles.contains(deletionVector.dataFileName())) {
                            continue;
                        }
                        Map<String, DeletionFile> partitionResult =
                                result.computeIfAbsent(
                                        entry.partition(), ignored -> new LinkedHashMap<>());
                        checkState(
                                partitionResult.put(
                                                deletionVector.dataFileName(),
                                                toDeletionFile(path, deletionVector))
                                        == null,
                                "Duplicate deletion vector for data file '%s'.",
                                deletionVector.dataFileName());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to scan deletion vectors for data files.", e);
            }
            return result;
        }

        private boolean isDeletionVectorEntry(IndexManifestEntry entry) {
            if (entry.kind() != FileKind.ADD
                    || !DELETION_VECTORS_INDEX.equals(entry.indexFile().indexType())) {
                return false;
            }
            if (partitionPredicate != null && !partitionPredicate.test(entry.partition())) {
                return false;
            }
            checkArgument(
                    entry.bucket() == UNAWARE_BUCKET,
                    "Materializing deletion vectors only supports unaware-bucket tables.");
            return true;
        }

        private static boolean containsAll(
                Map<BinaryRow, Map<String, DeletionFile>> actual,
                Map<BinaryRow, Map<String, DeletionFile>> expected) {
            for (Map.Entry<BinaryRow, Map<String, DeletionFile>> partition : expected.entrySet()) {
                Map<String, DeletionFile> actualPartition = actual.get(partition.getKey());
                if (actualPartition == null
                        || !actualPartition.keySet().containsAll(partition.getValue().keySet())) {
                    return false;
                }
            }
            return true;
        }

        private static DeletionFile toDeletionFile(String path, DeletionVectorMeta deletionVector) {
            return new DeletionFile(
                    path,
                    deletionVector.offset(),
                    deletionVector.length(),
                    deletionVector.cardinality());
        }

        private static List<Range> subtractRanges(
                List<Range> expandedRanges, List<Range> coveredRanges) {
            List<Range> result = new ArrayList<>();
            int coveredIndex = 0;
            for (Range expanded : expandedRanges) {
                long cursor = expanded.from;
                boolean exhausted = false;
                while (coveredIndex < coveredRanges.size()
                        && coveredRanges.get(coveredIndex).to < cursor) {
                    coveredIndex++;
                }
                int currentCoveredIndex = coveredIndex;
                while (currentCoveredIndex < coveredRanges.size()) {
                    Range covered = coveredRanges.get(currentCoveredIndex);
                    if (covered.from > expanded.to) {
                        break;
                    }
                    if (covered.from > cursor) {
                        result.add(new Range(cursor, covered.from - 1));
                    }
                    if (covered.to >= expanded.to) {
                        exhausted = true;
                        break;
                    }
                    if (covered.to == Long.MAX_VALUE) {
                        exhausted = true;
                        break;
                    }
                    cursor = Math.max(cursor, covered.to + 1);
                    currentCoveredIndex++;
                }
                if (!exhausted && cursor <= expanded.to) {
                    result.add(new Range(cursor, expanded.to));
                }
            }
            return result;
        }
    }

    private static class MaterializeScanBatch {

        private final List<ManifestEntry> entries;
        private final Map<BinaryRow, Map<String, DeletionFile>> deletionFiles;

        private MaterializeScanBatch(
                List<ManifestEntry> entries,
                Map<BinaryRow, Map<String, DeletionFile>> deletionFiles) {
            this.entries = entries;
            this.deletionFiles = deletionFiles;
        }
    }

    private static class MaterializePlanner {

        private final long targetFileSize;
        private final long openFileCost;

        private MaterializePlanner(long targetFileSize, long openFileCost) {
            this.targetFileSize = targetFileSize;
            this.openFileCost = openFileCost;
        }

        private List<DataEvolutionCompactTask> plan(MaterializeScanBatch batch) {
            Map<BinaryRow, List<DataFileMeta>> partitionedFiles = new LinkedHashMap<>();
            for (ManifestEntry entry : batch.entries) {
                partitionedFiles
                        .computeIfAbsent(entry.partition(), ignored -> new ArrayList<>())
                        .add(entry.file());
            }

            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            for (Map.Entry<BinaryRow, List<DataFileMeta>> partitionFiles :
                    partitionedFiles.entrySet()) {
                BinaryRow partition = partitionFiles.getKey();
                Map<String, DeletionFile> deletionFiles =
                        batch.deletionFiles.getOrDefault(partition, Collections.emptyMap());
                if (deletionFiles.isEmpty()) {
                    continue;
                }

                RangeHelper<DataFileMeta> rangeHelper =
                        new RangeHelper<>(DataFileMeta::nonNullRowIdRange);
                List<List<DataFileMeta>> components =
                        rangeHelper.mergeOverlappingRanges(partitionFiles.getValue());
                MaterializeBin bin = new MaterializeBin(targetFileSize);
                long previousEnd = Long.MIN_VALUE;
                for (List<DataFileMeta> component : components) {
                    Range componentRange = componentRange(component);
                    if (!bin.isEmpty()
                            && previousEnd != Long.MAX_VALUE
                            && componentRange.from > previousEnd + 1) {
                        addTask(tasks, bin.drain(), partition, deletionFiles);
                    }

                    long weight = componentWeight(component, deletionFiles);
                    if (weight > targetFileSize) {
                        addTask(tasks, bin.drain(), partition, deletionFiles);
                        bin.add(
                                component,
                                weight,
                                containsDeletionVector(component, deletionFiles));
                        addTask(tasks, bin.drain(), partition, deletionFiles);
                    } else {
                        bin.add(
                                component,
                                weight,
                                containsDeletionVector(component, deletionFiles));
                        if (bin.enoughContent()) {
                            addTask(tasks, bin.drain(), partition, deletionFiles);
                        }
                    }
                    previousEnd = componentRange.to;
                }
                addTask(tasks, bin.drain(), partition, deletionFiles);
            }
            return tasks;
        }

        private void addTask(
                List<DataEvolutionCompactTask> tasks,
                MaterializeBin bin,
                BinaryRow partition,
                Map<String, DeletionFile> deletionFiles) {
            if (bin.isEmpty() || !bin.containsDeletionVector()) {
                return;
            }

            List<DataFileMeta> files = bin.files();
            checkArgument(
                    files.stream().noneMatch(file -> isVectorStoreFile(file.fileName())),
                    "Materializing deletion vectors for vector-store files is not supported.");
            List<DeletionFile> alignedDeletionFiles =
                    files.stream()
                            .map(file -> deletionFiles.get(file.fileName()))
                            .collect(Collectors.toList());
            tasks.add(
                    new DataEvolutionMaterializeDeletionCompactTask(
                            partition, files, alignedDeletionFiles));
        }

        private long componentWeight(
                List<DataFileMeta> component, Map<String, DeletionFile> deletionFiles) {
            List<List<DataFileMeta>> normalGroups = normalGroups(component);
            long rowCount = 0L;
            long deletedRows = 0L;
            for (List<DataFileMeta> group : normalGroups) {
                DataFileMeta anchor = retrieveAnchorFile(group, file -> file);
                rowCount += anchor.rowCount();
                DeletionFile deletionFile = deletionFiles.get(anchor.fileName());
                if (deletionFile != null && deletionFile.cardinality() != null) {
                    deletedRows += Math.min(anchor.rowCount(), deletionFile.cardinality());
                }
            }
            double remainingRatio =
                    rowCount == 0L
                            ? 1D
                            : ((double) Math.max(0L, rowCount - deletedRows)) / rowCount;
            long weight = 0L;
            for (DataFileMeta file : component) {
                weight +=
                        Math.max((long) Math.ceil(file.fileSize() * remainingRatio), openFileCost);
            }
            return weight;
        }

        private boolean containsDeletionVector(
                List<DataFileMeta> component, Map<String, DeletionFile> deletionFiles) {
            for (List<DataFileMeta> group : normalGroups(component)) {
                if (deletionFiles.containsKey(retrieveAnchorFile(group, file -> file).fileName())) {
                    return true;
                }
            }
            return false;
        }

        private List<List<DataFileMeta>> normalGroups(List<DataFileMeta> component) {
            List<DataFileMeta> normalFiles =
                    component.stream()
                            .filter(
                                    file ->
                                            !isBlobFile(file.fileName())
                                                    && !isVectorStoreFile(file.fileName()))
                            .collect(Collectors.toList());
            return new RangeHelper<DataFileMeta>(DataFileMeta::nonNullRowIdRange)
                    .mergeOverlappingRanges(normalFiles);
        }

        private Range componentRange(List<DataFileMeta> component) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (DataFileMeta file : component) {
                Range range = file.nonNullRowIdRange();
                min = Math.min(min, range.from);
                max = Math.max(max, range.to);
            }
            return new Range(min, max);
        }
    }

    private static class MaterializeBin {

        private final long targetFileSize;
        private final List<DataFileMeta> files = new ArrayList<>();
        private long weight;
        private boolean containsDeletionVector;

        private MaterializeBin(long targetFileSize) {
            this.targetFileSize = targetFileSize;
        }

        private void add(
                List<DataFileMeta> component, long componentWeight, boolean componentContainsDv) {
            files.addAll(component);
            weight += componentWeight;
            containsDeletionVector |= componentContainsDv;
        }

        private boolean isEmpty() {
            return files.isEmpty();
        }

        private boolean containsDeletionVector() {
            return containsDeletionVector;
        }

        private boolean enoughContent() {
            return weight > targetFileSize;
        }

        private List<DataFileMeta> files() {
            return files;
        }

        private MaterializeBin drain() {
            MaterializeBin result = new MaterializeBin(targetFileSize);
            result.files.addAll(files);
            result.weight = weight;
            result.containsDeletionVector = containsDeletionVector;
            files.clear();
            weight = 0L;
            containsDeletionVector = false;
            return result;
        }
    }
}
