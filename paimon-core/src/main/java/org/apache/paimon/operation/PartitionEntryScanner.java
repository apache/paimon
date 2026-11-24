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
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Reads partition statistics directly from a snapshot's data manifests.
 *
 * <p>This scanner has no data, level or bucket filters. It is intended for partition maintenance,
 * which needs all active files independently of the table's data scan configuration.
 */
public final class PartitionEntryScanner {

    private static final ProjectedManifestEntry.Projection PARTITION_PROJECTION =
            createProjection(false);
    private static final ProjectedManifestEntry.Projection PARTITION_WITH_IDENTITY_PROJECTION =
            createProjection(true);

    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestsReader manifestsReader;
    @Nullable private final Integer parallelism;

    public PartitionEntryScanner(
            ManifestsReader manifestsReader,
            ManifestFile.Factory manifestFileFactory,
            @Nullable Integer parallelism) {
        this.manifestsReader = manifestsReader;
        this.manifestFileFactory = manifestFileFactory;
        this.parallelism = parallelism;
    }

    public PartitionEntryScanner withPartitionFilter(@Nullable PartitionPredicate filter) {
        manifestsReader.withPartitionFilter(filter);
        return this;
    }

    public List<PartitionEntry> scan() {
        return scan(manifestsReader.read(null, ScanMode.ALL).filteredManifests);
    }

    public List<PartitionEntry> scan(List<ManifestFileMeta> manifests) {
        Set<Identifier> deletedEntries = ConcurrentHashMap.newKeySet();
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                manifest ->
                        readManifest(
                                manifest,
                                ProjectedManifestEntry.DELETE_ENTRY_PROJECTION,
                                entry -> {
                                    if (entry.kind() == FileKind.DELETE) {
                                        deletedEntries.add(entry.identifier());
                                    }
                                }),
                manifests.stream()
                        .filter(manifest -> manifest.numDeletedFiles() > 0)
                        .collect(Collectors.toList()));

        ProjectedManifestEntry.Projection projection =
                deletedEntries.isEmpty()
                        ? PARTITION_PROJECTION
                        : PARTITION_WITH_IDENTITY_PROJECTION;
        Map<BinaryRow, PartitionStats> partitions = new ConcurrentHashMap<>();
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                manifest -> {
                    Map<BinaryRow, PartitionStats> local = new HashMap<>();
                    readManifest(
                            manifest,
                            projection,
                            entry -> {
                                if (entry.kind() != FileKind.ADD
                                        || (!deletedEntries.isEmpty()
                                                && deletedEntries.contains(entry.identifier()))) {
                                    return;
                                }
                                BinaryRow partition = entry.partition();
                                local.computeIfAbsent(partition, key -> new PartitionStats())
                                        .add(entry);
                            });
                    local.forEach(
                            (partition, stats) ->
                                    partitions.compute(
                                            partition,
                                            (key, previous) ->
                                                    previous == null
                                                            ? stats
                                                            : previous.merge(stats)));
                },
                manifests.stream()
                        .filter(manifest -> manifest.numAddedFiles() > 0)
                        .collect(Collectors.toList()));
        return partitions.entrySet().stream()
                .map(entry -> entry.getValue().toEntry(entry.getKey()))
                .collect(Collectors.toList());
    }

    private void readManifest(
            ManifestFileMeta manifest,
            ProjectedManifestEntry.Projection projection,
            Consumer<ManifestEntry> consumer) {
        manifestFileFactory
                .create()
                .scan(manifest, projection, manifestsReader.partitionFilter(), consumer);
    }

    private static ProjectedManifestEntry.Projection createProjection(boolean withIdentity) {
        RowType fileType =
                withIdentity
                        ? DataFileMeta.SCHEMA.project(
                                DataFileMeta.FILE_NAME,
                                DataFileMeta.FILE_SIZE,
                                DataFileMeta.ROW_COUNT,
                                DataFileMeta.LEVEL,
                                DataFileMeta.CREATION_TIME,
                                DataFileMeta.EXTRA_FILES,
                                DataFileMeta.EMBEDDED_FILE_INDEX,
                                DataFileMeta.EXTERNAL_PATH)
                        : DataFileMeta.SCHEMA.project(
                                DataFileMeta.FILE_SIZE,
                                DataFileMeta.ROW_COUNT,
                                DataFileMeta.CREATION_TIME);
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        List<DataField> fields =
                new ArrayList<>(
                        Arrays.asList(
                                manifestType.getField(ManifestEntry.KIND),
                                manifestType.getField(ManifestEntry.PARTITION),
                                manifestType.getField(ManifestEntry.TOTAL_BUCKETS),
                                manifestType.getField(ManifestEntry.FILE).newType(fileType)));
        if (withIdentity) {
            fields.add(manifestType.getField(ManifestEntry.BUCKET));
        }
        return ProjectedManifestEntry.Projection.create(new RowType(false, fields));
    }

    private static class PartitionStats {
        private long recordCount;
        private long fileSize;
        private long fileCount;
        private long lastFileCreationTime = Long.MIN_VALUE;
        private int totalBuckets;

        private void add(ManifestEntry entry) {
            DataFileMeta file = entry.file();
            recordCount += file.rowCount();
            fileSize += file.fileSize();
            fileCount++;
            lastFileCreationTime = Math.max(lastFileCreationTime, file.creationTimeEpochMillis());
            totalBuckets = entry.totalBuckets();
        }

        private PartitionStats merge(PartitionStats other) {
            recordCount += other.recordCount;
            fileSize += other.fileSize;
            fileCount += other.fileCount;
            lastFileCreationTime = Math.max(lastFileCreationTime, other.lastFileCreationTime);
            totalBuckets = other.totalBuckets;
            return this;
        }

        private PartitionEntry toEntry(BinaryRow partition) {
            return new PartitionEntry(
                    partition,
                    recordCount,
                    fileSize,
                    fileCount,
                    lastFileCreationTime,
                    totalBuckets);
        }
    }
}
