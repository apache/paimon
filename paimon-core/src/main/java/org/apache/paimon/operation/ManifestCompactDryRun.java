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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** Dry run for manifest compaction. Reads only existing metadata, never writes files. */
public class ManifestCompactDryRun {

    public static String execute(FileStoreTable table) {
        CoreOptions options = new CoreOptions(table.options());
        Snapshot latestSnapshot = table.store().snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            return appendEmptyManifestSortLevels("Dry run: no snapshot exists.", options);
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifests = manifestList.readDataManifests(latestSnapshot);

        if (manifests.isEmpty()) {
            return appendEmptyManifestSortLevels("Dry run: 0 manifest files.", options);
        }

        long suggestedMetaSize = options.manifestTargetSize().getBytes();

        long totalFiles = manifests.size();
        long totalSize = 0;
        long totalDeletedEntries = 0;
        long filesWithDeletedEntries = 0;
        long smallFiles = 0;

        for (ManifestFileMeta file : manifests) {
            totalSize += file.fileSize();
            totalDeletedEntries += file.numDeletedFiles();
            if (file.numDeletedFiles() > 0) {
                filesWithDeletedEntries++;
            }
            if (file.fileSize() < suggestedMetaSize) {
                smallFiles++;
            }
        }

        String summary =
                String.format(
                        "Dry run: %d manifest files (%s), "
                                + "%d deleted entries in %d files, "
                                + "%d undersized files (< %s).",
                        totalFiles,
                        MemorySize.ofBytes(totalSize),
                        totalDeletedEntries,
                        filesWithDeletedEntries,
                        smallFiles,
                        MemorySize.ofBytes(suggestedMetaSize));

        if (!options.manifestSortEnabled()) {
            return summary;
        }

        RowType partitionType = table.schema().logicalPartitionType();
        if (partitionType.getFieldCount() == 0
                && !(options.dataEvolutionEnabled()
                        && ManifestFileMeta.allContainsRowId(manifests))) {
            return summary + " Manifest sort level files: unavailable (no sortable field).";
        }

        long[] levelFileCounts = new long[ManifestPickStrategy.MAX_LEVEL + 1];
        List<ManifestAdjacentSortedRun> levelRuns =
                buildLevelSortedRunsForDryRun(
                        manifests,
                        table.store().manifestFileFactory().create(),
                        partitionType,
                        options);
        for (ManifestAdjacentSortedRun run : levelRuns) {
            levelFileCounts[run.level()] += run.files().size();
        }

        return appendManifestSortLevels(summary, levelFileCounts);
    }

    private static List<ManifestAdjacentSortedRun> buildLevelSortedRunsForDryRun(
            List<ManifestFileMeta> manifests,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options) {
        long suggestedMetaSize = options.manifestTargetSize().getBytes();
        boolean fullCompaction =
                ManifestFileSorter.reachesFullCompactionThreshold(
                        manifests,
                        suggestedMetaSize,
                        options.manifestFullCompactionThresholdSize().getBytes());
        ManifestFileSorter.ManifestSortKey sortKey =
                ManifestFileSorter.createSortKey(
                        options.dataEvolutionEnabled(),
                        manifests,
                        options.manifestSortPartitionField(),
                        partitionType);
        ManifestFileSorter.ClassifyResult classifyResult =
                ManifestFileSorter.classifyManifests(
                        manifests,
                        fullCompaction,
                        manifestFile,
                        partitionType,
                        suggestedMetaSize,
                        options.scanManifestParallelism());
        List<ManifestAdjacentSortedRun> levelRuns = buildLevelSortedRuns(classifyResult, sortKey);

        // A full compaction with no work falls through to the minor path. Mirror that fallback so
        // the reported levels describe the path which a real compaction would use.
        if (fullCompaction
                && classifyResult.compactWithoutSort.isEmpty()
                && new ManifestPickStrategy(
                                options.maxSizeAmplificationPercent(), options.sortedRunSizeRatio())
                        .pick(levelRuns)
                        .isEmpty()) {
            classifyResult =
                    ManifestFileSorter.classifyManifests(
                            manifests,
                            false,
                            manifestFile,
                            partitionType,
                            suggestedMetaSize,
                            options.scanManifestParallelism());
            levelRuns = buildLevelSortedRuns(classifyResult, sortKey);
        }
        return levelRuns;
    }

    private static List<ManifestAdjacentSortedRun> buildLevelSortedRuns(
            ManifestFileSorter.ClassifyResult classifyResult,
            ManifestFileSorter.ManifestSortKey sortKey) {
        return classifyResult.lsmFiles.isEmpty()
                ? new ArrayList<>()
                : ManifestFileSorter.buildLevelSortedRuns(classifyResult.lsmFiles, sortKey);
    }

    private static String appendEmptyManifestSortLevels(String summary, CoreOptions options) {
        return options.manifestSortEnabled()
                ? appendManifestSortLevels(summary, new long[ManifestPickStrategy.MAX_LEVEL + 1])
                : summary;
    }

    private static String appendManifestSortLevels(String summary, long[] levelFileCounts) {
        return summary
                + String.format(
                        " Manifest sort level files: L0=%d, L1=%d, L2=%d, L3=%d, L4=%d.",
                        levelFileCounts[0],
                        levelFileCounts[1],
                        levelFileCounts[2],
                        levelFileCounts[3],
                        levelFileCounts[4]);
    }
}
