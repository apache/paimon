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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.BucketSelector;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.predicate.PredicateBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.utils.RowDataToObjectArrayConverter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default implementation of {@link FileStoreScan}. */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private final FieldStatsArraySerializer partitionStatsConverter;
    private final RowDataToObjectArrayConverter partitionConverter;
    protected final RowType bucketKeyType;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestList manifestList;
    private final int numOfBuckets;
    private final boolean checkNumOfBuckets;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;
    private final long schemaId;

    private Predicate partitionFilter;
    private BucketSelector bucketSelector;

    private Long specifiedSnapshotId = null;
    private Integer specifiedBucket = null;
    private List<ManifestFileMeta> specifiedManifests = null;
    private ScanKind scanKind = ScanKind.ALL;
    private Integer specifiedLevel = null;

    public AbstractFileStoreScan(
            RowType partitionType,
            RowType bucketKeyType,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            long schemaId,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets) {
        this.partitionStatsConverter = new FieldStatsArraySerializer(partitionType);
        this.partitionConverter = new RowDataToObjectArrayConverter(partitionType);
        Preconditions.checkArgument(
                bucketKeyType.getFieldCount() > 0, "The bucket keys should not be empty.");
        this.bucketKeyType = bucketKeyType;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
        this.numOfBuckets = numOfBuckets;
        this.checkNumOfBuckets = checkNumOfBuckets;
        this.tableSchemas = new ConcurrentHashMap<>();
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    protected FileStoreScan withBucketKeyFilter(Predicate predicate) {
        this.bucketSelector = BucketSelector.create(predicate, bucketKeyType).orElse(null);
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
    public FileStoreScan withKind(ScanKind scanKind) {
        this.scanKind = scanKind;
        return this;
    }

    @Override
    public FileStoreScan withLevel(int level) {
        this.specifiedLevel = level;
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
                manifests = readManiests(snapshot);
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

        List<ManifestEntry> files = new ArrayList<>();
        for (ManifestEntry file : ManifestEntry.mergeEntries(entries)) {
            if (checkNumOfBuckets && file.totalBuckets() != numOfBuckets) {
                String partInfo =
                        partitionConverter.getArity() > 0
                                ? "partition "
                                        + FileStorePathFactory.getPartitionComputer(
                                                        partitionConverter.rowType(),
                                                        FileStorePathFactory.PARTITION_DEFAULT_NAME
                                                                .defaultValue())
                                                .generatePartValues(file.partition())
                                : "table";
                throw new TableException(
                        String.format(
                                "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                partInfo, numOfBuckets, file.totalBuckets()));
            }

            // bucket filter should not be applied along with partition filter
            // because the specifiedBucket is computed against the current numOfBuckets
            // however entry.bucket() was computed against the old numOfBuckets
            // and thus the filtered manifest entries might be empty
            // which renders the bucket check invalid
            if (filterByBucket(file) && filterByBucketSelector(file) && filterByLevel(file)) {
                files.add(file);
            }
        }

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

    private List<ManifestFileMeta> readManiests(Snapshot snapshot) {
        switch (scanKind) {
            case ALL:
                return snapshot.readAllDataManifests(manifestList);
            case DELTA:
                return manifestList.read(snapshot.deltaManifestList());
            case CHANGELOG:
                if (snapshot.version() >= 2) {
                    if (snapshot.changelogManifestList() == null) {
                        return Collections.emptyList();
                    } else {
                        return manifestList.read(snapshot.changelogManifestList());
                    }
                }

                // compatible with Table Store 0.2, we'll read extraFiles in DataFileMeta
                // see comments on DataFileMeta#extraFiles
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return manifestList.read(snapshot.deltaManifestList());
                }
                throw new IllegalStateException(
                        String.format(
                                "Incremental scan does not accept %s snapshot",
                                snapshot.commitKind()));
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanKind.name());
        }
    }

    // ------------------------------------------------------------------------
    // Start Thread Safe Methods: The following methods need to be thread safe because they will be
    // called by multiple threads
    // ------------------------------------------------------------------------

    /** Note: Keep this thread-safe. */
    protected TableSchema scanTableSchema() {
        return scanTableSchema(this.schemaId);
    }

    /** Note: Keep this thread-safe. */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(id, key -> schemaManager.schema(id));
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        return partitionFilter == null
                || partitionFilter.test(
                        manifest.numAddedFiles() + manifest.numDeletedFiles(),
                        manifest.partitionStats().fields(partitionStatsConverter));
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestEntry(ManifestEntry entry) {
        return filterByPartition(entry) && filterByStats(entry);
    }

    /** Note: Keep this thread-safe. */
    private boolean filterByPartition(ManifestEntry entry) {
        return (partitionFilter == null
                || partitionFilter.test(partitionConverter.convert(entry.partition())));
    }

    /** Note: Keep this thread-safe. */
    private boolean filterByBucket(ManifestEntry entry) {
        return (specifiedBucket == null || entry.bucket() == specifiedBucket);
    }

    /** Note: Keep this thread-safe. */
    private boolean filterByBucketSelector(ManifestEntry entry) {
        return (bucketSelector == null
                || bucketSelector.select(entry.bucket(), entry.totalBuckets()));
    }

    /** Note: Keep this thread-safe. */
    private boolean filterByLevel(ManifestEntry entry) {
        return (specifiedLevel == null || entry.file().level() == specifiedLevel);
    }

    /** Note: Keep this thread-safe. */
    protected abstract boolean filterByStats(ManifestEntry entry);

    /** Note: Keep this thread-safe. */
    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory.create().read(manifest.fileName());
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}
