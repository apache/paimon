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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Plans tasks which physically apply deletion vectors and assign new row IDs. */
public class DataEvolutionDeletionVectorMaterializeCoordinator {

    // Soft target. One overlapping manifest group can exceed it.
    private static final int FILES_PER_BATCH = 100_000;

    private final MaterializeScanner scanner;
    private final MaterializePlanner planner;

    public DataEvolutionDeletionVectorMaterializeCoordinator(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            Snapshot snapshot) {
        CoreOptions options = table.coreOptions();
        checkArgument(
                options.dataEvolutionEnabled(),
                "Materializing deletion vectors requires a data evolution table.");
        checkArgument(
                options.deletionVectorsEnabled(),
                "Materializing deletion vectors requires deletion vectors to be enabled.");

        this.scanner =
                new MaterializeScanner(
                        table.newSnapshotReader().withPartitionFilter(partitionPredicate),
                        table.store().newScan().withPartitionFilter(partitionPredicate).dropStats(),
                        snapshot);
        this.planner =
                new MaterializePlanner(
                        table.store().newIndexFileHandler(),
                        snapshot,
                        options.targetFileSize(false),
                        options.splitOpenFileCost());
    }

    public List<DataEvolutionCompactTask> plan() {
        List<ManifestEntry> entries = scanner.scan();
        return entries.isEmpty() ? Collections.emptyList() : planner.plan(entries);
    }

    public Snapshot snapshot() {
        return scanner.snapshot;
    }

    private static class MaterializeScanner {

        private final FileStoreScan scan;
        private final Snapshot snapshot;
        private final Queue<List<ManifestFileMeta>> manifestGroups;

        private MaterializeScanner(
                SnapshotReader snapshotReader, FileStoreScan scan, Snapshot snapshot) {
            this.scan = scan;
            this.snapshot = snapshot;

            List<ManifestFileMeta> manifests =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;
            if (allContainsRowId(manifests)) {
                RangeHelper<ManifestFileMeta> rangeHelper =
                        new RangeHelper<>(
                                manifest ->
                                        new Range(
                                                manifest.minRowId(),
                                                manifest.maxRowId() < Long.MAX_VALUE
                                                        ? manifest.maxRowId() + 1L
                                                        : manifest.maxRowId()));
                this.manifestGroups =
                        new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifests));
            } else {
                this.manifestGroups = new ArrayDeque<>(Collections.singletonList(manifests));
            }
        }

        private List<ManifestEntry> scan() {
            List<ManifestEntry> result = new ArrayList<>();
            while (!manifestGroups.isEmpty() && result.size() < FILES_PER_BATCH) {
                scan.readFileIterator(manifestGroups.poll()).forEachRemaining(result::add);
            }
            if (result.isEmpty()) {
                throw new EndOfScanException();
            }
            return result;
        }
    }

    private static class MaterializePlanner {

        private final IndexFileHandler indexFileHandler;
        private final Snapshot snapshot;
        private final long targetFileSize;
        private final long openFileCost;

        private MaterializePlanner(
                IndexFileHandler indexFileHandler,
                Snapshot snapshot,
                long targetFileSize,
                long openFileCost) {
            this.indexFileHandler = indexFileHandler;
            this.snapshot = snapshot;
            this.targetFileSize = targetFileSize;
            this.openFileCost = openFileCost;
        }

        private List<DataEvolutionCompactTask> plan(List<ManifestEntry> entries) {
            Map<BinaryRow, List<DataFileMeta>> partitionedFiles = new LinkedHashMap<>();
            for (ManifestEntry entry : entries) {
                partitionedFiles
                        .computeIfAbsent(entry.partition(), ignored -> new ArrayList<>())
                        .add(entry.file());
            }

            List<DataEvolutionCompactTask> tasks = new ArrayList<>();
            for (Map.Entry<BinaryRow, List<DataFileMeta>> partitionFiles :
                    partitionedFiles.entrySet()) {
                BinaryRow partition = partitionFiles.getKey();
                Map<String, DeletionFile> deletionFiles = deletionFiles(partition);
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

        private Map<String, DeletionFile> deletionFiles(BinaryRow partition) {
            List<IndexFileMeta> indexFiles =
                    indexFileHandler.scan(
                            snapshot, DELETION_VECTORS_INDEX, partition, UNAWARE_BUCKET);
            return indexFileHandler.dvIndex(partition, UNAWARE_BUCKET).toDeletionFiles(indexFiles);
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
