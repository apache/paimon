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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Scans and aggregates partition statistics from manifest entries.
 *
 * <p>It uses a narrow, streaming projection when the manifest cannot benefit from the cache and
 * falls back to the caller's complete entry reader when other filters need the full schema.
 */
final class PartitionEntryScanner {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionEntryScanner.class);
    private static final ProjectedManifestEntry.Projection PARTITION_ENTRY_PROJECTION =
            createPartitionEntryProjection();

    private final ManifestFile.Factory manifestFileFactory;
    private final Function<ManifestFileMeta, List<PartitionEntry>> fullEntryReader;
    @Nullable private final PartitionPredicate partitionFilter;
    @Nullable private final BucketFilter bucketFilter;
    @Nullable private final Integer specifiedLevel;
    @Nullable private final Filter<Integer> levelFilter;
    @Nullable private final Filter<String> fileNameFilter;
    private final boolean requiresFullManifestEntry;
    @Nullable private final Integer parallelism;

    PartitionEntryScanner(
            ManifestFile.Factory manifestFileFactory,
            Function<ManifestFileMeta, List<PartitionEntry>> fullEntryReader,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            @Nullable Integer specifiedLevel,
            @Nullable Filter<Integer> levelFilter,
            @Nullable Filter<String> fileNameFilter,
            boolean requiresFullManifestEntry,
            @Nullable Integer parallelism) {
        this.manifestFileFactory = manifestFileFactory;
        this.fullEntryReader = fullEntryReader;
        this.partitionFilter = partitionFilter;
        this.bucketFilter = bucketFilter;
        this.specifiedLevel = specifiedLevel;
        this.levelFilter = levelFilter;
        this.fileNameFilter = fileNameFilter;
        this.requiresFullManifestEntry = requiresFullManifestEntry;
        this.parallelism = parallelism;
    }

    List<PartitionEntry> scan(List<ManifestFileMeta> manifests) {
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                manifest -> scanManifest(manifest, partitions),
                manifests);
        return partitions.values().stream()
                .filter(partition -> partition.fileCount() > 0)
                .collect(Collectors.toList());
    }

    private void scanManifest(
            ManifestFileMeta manifest, Map<BinaryRow, PartitionEntry> partitions) {
        // Projected scans read the file directly, so preserve the normal path for cached manifests
        // and filters which require fields outside the partition projection.
        if (requiresFullManifestEntry || manifestFileFactory.isCacheable(manifest.fileSize())) {
            PartitionEntry.merge(fullEntryReader.apply(manifest), partitions);
            return;
        }

        long count = 0;
        try (CloseableIterator<ProjectedManifestEntry> entries =
                manifestFileFactory
                        .create()
                        .scan(
                                manifest.fileName(),
                                PARTITION_ENTRY_PROJECTION,
                                partitionFilter,
                                bucketFilter)) {
            while (entries.hasNext()) {
                ProjectedManifestEntry entry = entries.next();
                if (!filter(entry)) {
                    continue;
                }

                PartitionEntry partitionEntry = PartitionEntry.fromManifestEntry(entry);
                partitions.compute(
                        partitionEntry.partition(),
                        (partition, previous) ->
                                previous == null ? partitionEntry : previous.merge(partitionEntry));
                count++;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to scan manifest " + manifest.fileName(), e);
        }
        LOG.info("Read {} projected manifest entries from {}", count, manifest.fileName());
    }

    private boolean filter(ProjectedManifestEntry entry) {
        int level = entry.level();
        if (specifiedLevel != null && level != specifiedLevel) {
            return false;
        }
        if (levelFilter != null && !levelFilter.test(level)) {
            return false;
        }
        return fileNameFilter == null || fileNameFilter.test(entry.fileName());
    }

    /**
     * Keeps the fields required to aggregate {@link PartitionEntry}: kind controls the sign of
     * added/deleted files, partition is the grouping key, total buckets is part of the result, and
     * file size, row count and creation time form its statistics. File name and level are also kept
     * to preserve the corresponding structural filters.
     */
    private static ProjectedManifestEntry.Projection createPartitionEntryProjection() {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        return ProjectedManifestEntry.Projection.create(
                new RowType(
                        false,
                        Arrays.asList(
                                manifestType.getField(ManifestEntry.KIND),
                                manifestType.getField(ManifestEntry.PARTITION),
                                manifestType.getField(ManifestEntry.TOTAL_BUCKETS),
                                manifestType
                                        .getField(ManifestEntry.FILE)
                                        .newType(
                                                DataFileMeta.SCHEMA.project(
                                                        DataFileMeta.FILE_NAME,
                                                        DataFileMeta.FILE_SIZE,
                                                        DataFileMeta.ROW_COUNT,
                                                        DataFileMeta.LEVEL,
                                                        DataFileMeta.CREATION_TIME)))));
    }
}
