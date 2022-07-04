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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.utils.RowDataToObjectArrayConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default implementation of {@link FileStoreScan}. */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private final FieldStatsArraySerializer partitionStatsConverter;
    private final RowDataToObjectArrayConverter partitionConverter;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestList manifestList;

    private Predicate partitionFilter;

    private Long specifiedSnapshotId = null;
    private Integer specifiedBucket = null;
    private List<ManifestFileMeta> specifiedManifests = null;
    private boolean isIncremental = false;

    public AbstractFileStoreScan(
            RowType partitionType,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory) {
        this.partitionStatsConverter = new FieldStatsArraySerializer(partitionType);
        this.partitionConverter = new RowDataToObjectArrayConverter(partitionType);
        this.snapshotManager = snapshotManager;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRowData> partitions) {
        PredicateBuilder builder = new PredicateBuilder(partitionConverter.rowType());
        Function<BinaryRowData, Predicate> partitionToPredicate =
                p -> {
                    List<Predicate> fieldPredicates = new ArrayList<>();
                    Object[] partitionObjects = partitionConverter.convert(p);
                    for (int i = 0; i < partitionConverter.getArity(); i++) {
                        Object partition = partitionObjects[i];
                        fieldPredicates.add(builder.equal(i, partition));
                    }
                    return PredicateBuilder.and(fieldPredicates);
                };
        List<Predicate> predicates =
                partitions.stream()
                        .filter(p -> p.getArity() > 0)
                        .map(partitionToPredicate)
                        .collect(Collectors.toList());
        if (predicates.isEmpty()) {
            return this;
        } else {
            return withPartitionFilter(PredicateBuilder.or(predicates));
        }
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.specifiedSnapshotId = snapshotId;
        if (specifiedManifests != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        this.specifiedManifests = manifests;
        if (specifiedSnapshotId != null) {
            throw new IllegalStateException("Cannot set both snapshot id and manifests.");
        }
        return this;
    }

    @Override
    public FileStoreScan withIncremental(boolean isIncremental) {
        this.isIncremental = isIncremental;
        return this;
    }

    @Override
    public Plan plan() {
        List<ManifestFileMeta> manifests = specifiedManifests;
        Long snapshotId = specifiedSnapshotId;
        if (manifests == null) {
            if (snapshotId == null) {
                snapshotId = snapshotManager.latestSnapshotId();
            }
            if (snapshotId == null) {
                manifests = Collections.emptyList();
            } else {
                Snapshot snapshot = snapshotManager.snapshot(snapshotId);
                manifests =
                        isIncremental
                                ? manifestList.read(snapshot.deltaManifestList())
                                : snapshot.readAllManifests(manifestList);
            }
        }

        final Long readSnapshot = snapshotId;
        final List<ManifestFileMeta> readManifests = manifests;

        List<ManifestEntry> entries;
        try {
            entries =
                    FileUtils.COMMON_IO_FORK_JOIN_POOL
                            .submit(
                                    () ->
                                            readManifests
                                                    .parallelStream()
                                                    .filter(this::filterManifestFileMeta)
                                                    .flatMap(m -> readManifestFileMeta(m).stream())
                                                    .filter(this::filterManifestEntry)
                                                    .collect(Collectors.toList()))
                            .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to read ManifestEntry list concurrently", e);
        }

        LinkedHashMap<ManifestEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
            ManifestEntry.Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    Preconditions.checkState(
                            !map.containsKey(identifier),
                            "Trying to add file %s which is already added. "
                                    + "Manifest might be corrupted.",
                            identifier);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    Preconditions.checkState(
                            map.containsKey(identifier),
                            "Trying to delete file %s which is not previously added. "
                                    + "Manifest might be corrupted.",
                            identifier);
                    map.remove(identifier);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
        List<ManifestEntry> files = new ArrayList<>(map.values());

        return new Plan() {
            @Nullable
            @Override
            public Long snapshotId() {
                return readSnapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        return partitionFilter == null
                || partitionFilter.test(
                        manifest.numAddedFiles() + manifest.numDeletedFiles(),
                        manifest.partitionStats().fields(partitionStatsConverter));
    }

    private boolean filterManifestEntry(ManifestEntry entry) {
        return filterByPartitionAndBucket(entry) && filterByStats(entry);
    }

    private boolean filterByPartitionAndBucket(ManifestEntry entry) {
        return (partitionFilter == null
                        || partitionFilter.test(partitionConverter.convert(entry.partition())))
                && (specifiedBucket == null || entry.bucket() == specifiedBucket);
    }

    protected abstract boolean filterByStats(ManifestEntry entry);

    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory.create().read(manifest.fileName());
    }
}
