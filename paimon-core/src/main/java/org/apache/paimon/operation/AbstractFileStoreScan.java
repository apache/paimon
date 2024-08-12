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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Default implementation of {@link FileStoreScan}. */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private final RowType partitionType;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final ManifestList manifestList;
    private final int numOfBuckets;
    private final boolean checkNumOfBuckets;
    private final Integer scanManifestParallelism;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    protected final ScanBucketFilter bucketKeyFilter;

    private PartitionPredicate partitionFilter;
    private Snapshot specifiedSnapshot = null;
    private Filter<Integer> bucketFilter = null;
    private List<ManifestFileMeta> specifiedManifests = null;
    private ScanMode scanMode = ScanMode.ALL;
    private Filter<Integer> levelFilter = null;
    private Long dataFileTimeMills = null;
    private Filter<String> fileNameFilter = null;

    private ManifestCacheFilter manifestCacheFilter = null;
    private ScanMetrics scanMetrics = null;

    public AbstractFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketKeyFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism) {
        this.partitionType = partitionType;
        this.bucketKeyFilter = bucketKeyFilter;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.manifestList = manifestListFactory.create();
        this.numOfBuckets = numOfBuckets;
        this.checkNumOfBuckets = checkNumOfBuckets;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.scanManifestParallelism = scanManifestParallelism;
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        this.bucketFilter = i -> i == bucket;
        return this;
    }

    @Override
    public FileStoreScan withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
        if (manifestCacheFilter != null && manifestFileFactory.isCacheEnabled()) {
            checkArgument(
                    manifestCacheFilter.test(partition, bucket),
                    String.format(
                            "This is a bug! The partition %s and bucket %s is filtered!",
                            partition, bucket));
        }
        withPartitionFilter(Collections.singletonList(partition));
        withBucket(bucket);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        checkState(specifiedManifests == null, "Cannot set both snapshot and manifests.");
        this.specifiedSnapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(Snapshot snapshot) {
        checkState(specifiedManifests == null, "Cannot set both snapshot and manifests.");
        this.specifiedSnapshot = snapshot;
        return this;
    }

    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        checkState(specifiedSnapshot == null, "Cannot set both snapshot and manifests.");
        this.specifiedManifests = manifests;
        return this;
    }

    @Override
    public FileStoreScan withKind(ScanMode scanMode) {
        this.scanMode = scanMode;
        return this;
    }

    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    @Override
    public FileStoreScan withDataFileTimeMills(long dataFileTimeMills) {
        this.dataFileTimeMills = dataFileTimeMills;
        return this;
    }

    @Override
    public FileStoreScan withManifestCacheFilter(ManifestCacheFilter manifestFilter) {
        this.manifestCacheFilter = manifestFilter;
        return this;
    }

    @Override
    public FileStoreScan withDataFileNameFilter(Filter<String> fileNameFilter) {
        this.fileNameFilter = fileNameFilter;
        return this;
    }

    @Override
    public FileStoreScan withMetrics(ScanMetrics metrics) {
        this.scanMetrics = metrics;
        return this;
    }

    @Override
    public Plan plan() {

        Pair<Snapshot, List<ManifestEntry>> planResult = doPlan();

        final Snapshot readSnapshot = planResult.getLeft();
        final List<ManifestEntry> files = planResult.getRight();

        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return readSnapshot == null ? null : readSnapshot.watermark();
            }

            @Nullable
            @Override
            public Long snapshotId() {
                return readSnapshot == null ? null : readSnapshot.id();
            }

            @Override
            public ScanMode scanMode() {
                return scanMode;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    @Override
    public List<SimpleFileEntry> readSimpleEntries() {
        List<ManifestFileMeta> manifests = readManifests().getRight();
        Collection<SimpleFileEntry> mergedEntries =
                readAndMergeFileEntries(manifests, this::readSimpleEntries, Filter.alwaysTrue());
        return new ArrayList<>(mergedEntries);
    }

    @Override
    public List<PartitionEntry> readPartitionEntries() {
        List<ManifestFileMeta> manifests = readManifests().getRight();
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        // Can be executed in disorder
        ThreadPoolExecutor executor = getExecutorService(scanManifestParallelism);
        Consumer<ManifestFileMeta> processor =
                m ->
                        PartitionEntry.merge(
                                PartitionEntry.merge(readManifestFileMeta(m)), partitions);
        randomlyOnlyExecute(executor, processor, manifests);
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public List<BucketEntry> readBucketEntries() {
        List<ManifestFileMeta> manifests = readManifests().getRight();
        Map<Integer, BucketEntry> buckets = new ConcurrentHashMap<>();
        // Can be executed in disorder
        ThreadPoolExecutor executor = getExecutorService(scanManifestParallelism);
        Consumer<ManifestFileMeta> processor =
                m ->
                        BucketEntry.merge(
                                BucketEntry.merge(
                                        readManifestFileMeta(m),
                                        CoreOptions.fromMap(schema.options())
                                                .numSortedRunCompactionTrigger()),
                                buckets);
        randomlyOnlyExecute(executor, processor, manifests);
        return buckets.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    private Pair<Snapshot, List<ManifestEntry>> doPlan() {
        long started = System.nanoTime();
        Pair<Snapshot, List<ManifestFileMeta>> snapshotListPair = readManifests();
        Snapshot snapshot = snapshotListPair.getLeft();
        List<ManifestFileMeta> manifests = snapshotListPair.getRight();

        long startDataFiles =
                manifests.stream().mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles()).sum();

        Collection<ManifestEntry> mergedEntries =
                readAndMergeFileEntries(
                        manifests, this::readManifestFileMeta, this::filterUnmergedManifestEntry);

        List<ManifestEntry> files = new ArrayList<>();
        long skippedByPartitionAndStats = startDataFiles - mergedEntries.size();
        for (ManifestEntry file : mergedEntries) {
            if (checkNumOfBuckets && file.totalBuckets() != numOfBuckets) {
                String partInfo =
                        partitionType.getFieldCount() > 0
                                ? "partition "
                                        + FileStorePathFactory.getPartitionComputer(
                                                        partitionType,
                                                        CoreOptions.PARTITION_DEFAULT_NAME
                                                                .defaultValue())
                                                .generatePartValues(file.partition())
                                : "table";
                throw new RuntimeException(
                        String.format(
                                "Try to write %s with a new bucket num %d, but the previous bucket num is %d. "
                                        + "Please switch to batch mode, and perform INSERT OVERWRITE to rescale current data layout first.",
                                partInfo, numOfBuckets, file.totalBuckets()));
            }

            // bucket filter should not be applied along with partition filter
            // because the specifiedBucket is computed against the current
            // numOfBuckets
            // however entry.bucket() was computed against the old numOfBuckets
            // and thus the filtered manifest entries might be empty
            // which renders the bucket check invalid
            if (filterMergedManifestEntry(file)) {
                files.add(file);
            }
        }

        long afterBucketFilter = files.size();
        long skippedByBucketAndLevelFilter = mergedEntries.size() - files.size();
        // We group files by bucket here, and filter them by the whole bucket filter.
        // Why do this: because in primary key table, we can't just filter the value
        // by the stat in files (see `PrimaryKeyFileStoreTable.nonPartitionFilterConsumer`),
        // but we can do this by filter the whole bucket files
        files =
                files.stream()
                        .collect(
                                Collectors.groupingBy(
                                        // we use LinkedHashMap to avoid disorder
                                        file -> Pair.of(file.partition(), file.bucket()),
                                        LinkedHashMap::new,
                                        Collectors.toList()))
                        .values()
                        .stream()
                        .map(this::filterWholeBucketByStats)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        long skippedByWholeBucketFiles = afterBucketFilter - files.size();
        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        checkState(
                startDataFiles
                                - skippedByPartitionAndStats
                                - skippedByBucketAndLevelFilter
                                - skippedByWholeBucketFiles
                        == files.size());
        if (scanMetrics != null) {
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration,
                            manifests.size(),
                            skippedByPartitionAndStats,
                            skippedByBucketAndLevelFilter,
                            skippedByWholeBucketFiles,
                            files.size()));
        }
        return Pair.of(snapshot, files);
    }

    public <T extends FileEntry> Collection<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<ManifestFileMeta, List<T>> manifestReader,
            @Nullable Filter<T> filterUnmergedEntry) {
        // in memory filter, do it first
        manifests =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<T>> reader =
                file -> {
                    List<T> entries = manifestReader.apply(file);
                    if (filterUnmergedEntry != null) {
                        entries =
                                entries.stream()
                                        .filter(filterUnmergedEntry::test)
                                        .collect(Collectors.toList());
                    }
                    return entries;
                };
        return FileEntry.mergeEntries(
                sequentialBatchedExecute(reader, manifests, scanManifestParallelism));
    }

    private Pair<Snapshot, List<ManifestFileMeta>> readManifests() {
        List<ManifestFileMeta> manifests = specifiedManifests;
        Snapshot snapshot = null;
        if (manifests == null) {
            snapshot =
                    specifiedSnapshot == null
                            ? snapshotManager.latestSnapshot()
                            : specifiedSnapshot;
            if (snapshot == null) {
                manifests = Collections.emptyList();
            } else {
                manifests = readManifests(snapshot);
            }
        }
        return Pair.of(snapshot, manifests);
    }

    private List<ManifestFileMeta> readManifests(Snapshot snapshot) {
        switch (scanMode) {
            case ALL:
                return snapshot.dataManifests(manifestList);
            case DELTA:
                return snapshot.deltaManifests(manifestList);
            case CHANGELOG:
                if (snapshot.version() > Snapshot.TABLE_STORE_02_VERSION) {
                    return snapshot.changelogManifests(manifestList);
                }

                // compatible with Paimon 0.2, we'll read extraFiles in DataFileMeta
                // see comments on DataFileMeta#extraFiles
                if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
                    return snapshot.deltaManifests(manifestList);
                }
                throw new IllegalStateException(
                        String.format(
                                "Incremental scan does not accept %s snapshot",
                                snapshot.commitKind()));
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
    }

    // ------------------------------------------------------------------------
    // Start Thread Safe Methods: The following methods need to be thread safe because they will be
    // called by multiple threads
    // ------------------------------------------------------------------------

    /** Note: Keep this thread-safe. */
    protected TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        if (partitionFilter == null) {
            return true;
        }

        SimpleStats stats = manifest.partitionStats();
        return partitionFilter == null
                || partitionFilter.test(
                        manifest.numAddedFiles() + manifest.numDeletedFiles(),
                        stats.minValues(),
                        stats.maxValues(),
                        stats.nullCounts());
    }

    /** Note: Keep this thread-safe. */
    private boolean filterUnmergedManifestEntry(ManifestEntry entry) {
        if (dataFileTimeMills != null
                && entry.file().creationTimeEpochMillis() < dataFileTimeMills) {
            return false;
        }

        return filterByStats(entry);
    }

    /** Note: Keep this thread-safe. */
    protected abstract boolean filterByStats(ManifestEntry entry);

    /** Note: Keep this thread-safe. */
    private boolean filterMergedManifestEntry(ManifestEntry entry) {
        return (bucketFilter == null || bucketFilter.test(entry.bucket()))
                && bucketKeyFilter.select(entry.bucket(), entry.totalBuckets())
                && (levelFilter == null || levelFilter.test(entry.file().level()));
    }

    /** Note: Keep this thread-safe. */
    protected abstract List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries);

    /** Note: Keep this thread-safe. */
    private List<ManifestEntry> readManifestFileMeta(ManifestFileMeta manifest) {
        return manifestFileFactory
                .create()
                .read(
                        manifest.fileName(),
                        manifest.fileSize(),
                        ManifestEntry.createCacheRowFilter(manifestCacheFilter, numOfBuckets),
                        ManifestEntry.createEntryRowFilter(
                                partitionFilter, bucketFilter, fileNameFilter, numOfBuckets));
    }

    /** Note: Keep this thread-safe. */
    private List<SimpleFileEntry> readSimpleEntries(ManifestFileMeta manifest) {
        return manifestFileFactory
                .createSimpleFileEntryReader()
                .read(
                        manifest.fileName(),
                        manifest.fileSize(),
                        // use filter for ManifestEntry
                        // currently, projection is not pushed down to file format
                        // see SimpleFileEntrySerializer
                        ManifestEntry.createCacheRowFilter(manifestCacheFilter, numOfBuckets),
                        ManifestEntry.createEntryRowFilter(
                                partitionFilter, bucketFilter, fileNameFilter, numOfBuckets));
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}
