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
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact coordinator to cmpact data evolution table. */
public class DataEvolutionCompactCoordinator {

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
                new CompactPlanner(
                        scanner::fetchResult,
                        compactBlob,
                        targetFileSize,
                        openFileCost,
                        compactMinFileNum);
    }

    public List<DataEvolutionCompactTask> plan() {
        // scan files in snapshot
        if (scanner.scan()) {
            // do plan compact tasks
            return planner.compactPlan();
        }

        return Collections.emptyList();
    }

    /** Scanner to generate sorted ManifestEntries. */
    static class CompactScanner {

        private final SnapshotReader snapshotReader;
        private final Queue<List<ManifestFileMeta>> metas;

        private List<ManifestEntry> result;

        private CompactScanner(SnapshotReader snapshotReader) {
            this.snapshotReader = snapshotReader;
            Snapshot snapshot = snapshotReader.snapshotManager().latestSnapshot();

            List<ManifestFileMeta> manifestFileMetas =
                    snapshotReader.manifestsReader().read(snapshot, ScanMode.ALL).filteredManifests;
            RangeHelper<ManifestFileMeta> rangeHelper =
                    new RangeHelper<>(ManifestFileMeta::minRowId, ManifestFileMeta::maxRowId);
            this.metas = new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFileMetas));
            this.result = new ArrayList<>();
        }

        boolean scan() {
            boolean scanResult = false;
            while (metas.peek() != null && result.size() < 1000) {
                scanResult = true;
                List<ManifestFileMeta> currentMetas = metas.poll();
                List<ManifestEntry> targetEntries =
                        currentMetas.stream()
                                .flatMap(meta -> snapshotReader.readManifest(meta).stream())
                                .collect(Collectors.toList());
                Comparator<ManifestEntry> comparator =
                        Comparator.comparingLong((ManifestEntry a) -> a.file().nonNullFirstRowId())
                                .thenComparingInt(
                                        a -> BlobFileFormat.isBlobFile(a.fileName()) ? 1 : 0);
                targetEntries.sort(comparator);

                result.addAll(targetEntries);
            }
            return scanResult;
        }

        List<ManifestEntry> fetchResult() {
            List<ManifestEntry> result = new ArrayList<>(this.result);
            this.result = new ArrayList<>();
            return result;
        }
    }

    /** Generate compaction tasks. */
    static class CompactPlanner {

        private final Supplier<List<ManifestEntry>> supplier;
        private final boolean compactBlob;
        private final long targetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;
        private long lastRowIdStart = -1;
        private long nextRowIdExpected = -1;
        private long weightSum = 0L;
        private BinaryRow lastPartition = null;
        private boolean skipFile = false;
        private List<DataEvolutionCompactTask> tasks = new ArrayList<>();
        private List<DataFileMeta> groupFiles = new ArrayList<>();
        private List<DataFileMeta> blobFiles = new ArrayList<>();

        CompactPlanner(
                Supplier<List<ManifestEntry>> supplier,
                boolean compactBlob,
                long targetFileSize,
                long openFileCost,
                long compactMinFileNum) {
            this.supplier = supplier;
            this.compactBlob = compactBlob;
            this.targetFileSize = targetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
        }

        List<DataEvolutionCompactTask> compactPlan() {
            for (ManifestEntry entry : supplier.get()) {
                long rowId = entry.file().nonNullFirstRowId();
                if (rowId < lastRowIdStart) {
                    throw new IllegalStateException(
                            "Files are not in order by rowId. Current file rowId: "
                                    + rowId
                                    + ", last file rowId: "
                                    + lastRowIdStart);
                } else if (rowId == lastRowIdStart) {
                    checkArgument(
                            lastPartition.equals(entry.partition()),
                            "Inconsistent partition for the same rowId: " + rowId);
                    if (!skipFile) {
                        if (BlobFileFormat.isBlobFile(entry.fileName())) {
                            blobFiles.add(entry.file());
                        } else {
                            groupFiles.add(entry.file());
                            weightSum += Math.max(entry.file().fileSize(), openFileCost);
                        }
                    }
                } else if (rowId < nextRowIdExpected) {
                    checkArgument(
                            lastPartition.equals(entry.partition()),
                            "Inconsistent partition for the same rowId: " + rowId);
                    checkArgument(
                            BlobFileFormat.isBlobFile(entry.fileName()),
                            "Data file found in the middle of blob files for rowId: " + rowId);
                    if (!skipFile) {
                        blobFiles.add(entry.file());
                    }
                } else {
                    BinaryRow currentPartition = entry.partition();
                    long currentWeight = Math.max(entry.file().fileSize(), openFileCost);
                    // skip big file
                    skipFile = currentWeight > targetFileSize;

                    // If compaction condition meets, do compaction
                    if (weightSum > targetFileSize
                            || rowId > nextRowIdExpected
                            || !currentPartition.equals(lastPartition)
                            || skipFile) {
                        flushAll();
                    }

                    if (!skipFile) {
                        weightSum += currentWeight;
                        groupFiles.add(entry.file());
                    }
                    lastRowIdStart = rowId;
                    nextRowIdExpected = rowId + entry.file().rowCount();
                    lastPartition = currentPartition;
                }
            }
            // do compaction for the last group
            flushAll();

            List<DataEvolutionCompactTask> result = new ArrayList<>(tasks);
            tasks = new ArrayList<>();
            return result;
        }

        private void flushAll() {
            if (!groupFiles.isEmpty()) {
                if (groupFiles.size() >= compactMinFileNum) {
                    tasks.add(
                            new DataEvolutionCompactTask(
                                    lastPartition, new ArrayList<>(groupFiles), false));

                    if (compactBlob && blobFiles.size() > 1) {
                        tasks.add(
                                new DataEvolutionCompactTask(
                                        lastPartition, new ArrayList<>(blobFiles), true));
                    }
                }

                weightSum = 0L;
                groupFiles = new ArrayList<>();
                blobFiles = new ArrayList<>();
            }

            checkArgument(weightSum == 0L, "Weight sum should be zero after compaction.");
            checkArgument(groupFiles.isEmpty(), "Group files should be empty.");
            checkArgument(blobFiles.isEmpty(), "Blob files should be empty.");
        }
    }
}
