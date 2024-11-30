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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Default implementation of {@link FileStoreScan}. */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private final ManifestsReader manifestsReader;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final Integer parallelism;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;
    private final TableSchema schema;

    private Snapshot specifiedSnapshot = null;
    private Filter<Integer> bucketFilter = null;
    private BiFilter<Integer, Integer> totalAwareBucketFilter = null;
    protected ScanMode scanMode = ScanMode.ALL;
    private Filter<Integer> levelFilter = null;
    private Filter<ManifestEntry> manifestEntryFilter = null;
    private Filter<String> fileNameFilter = null;

    private ManifestCacheFilter manifestCacheFilter = null;
    private ScanMetrics scanMetrics = null;
    private boolean dropStats;

    public AbstractFileStoreScan(
            ManifestsReader manifestsReader,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            @Nullable Integer parallelism) {
        this.manifestsReader = manifestsReader;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.parallelism = parallelism;
        this.dropStats = false;
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(List<BinaryRow> partitions) {
        manifestsReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
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
    public FileStoreScan withTotalAwareBucketFilter(
            BiFilter<Integer, Integer> totalAwareBucketFilter) {
        this.totalAwareBucketFilter = totalAwareBucketFilter;
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
        this.specifiedSnapshot = snapshotManager.snapshot(snapshotId);
        return this;
    }

    @Override
    public FileStoreScan withSnapshot(Snapshot snapshot) {
        this.specifiedSnapshot = snapshot;
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
    public FileStoreScan enableValueFilter() {
        return this;
    }

    @Override
    public FileStoreScan withManifestEntryFilter(Filter<ManifestEntry> filter) {
        this.manifestEntryFilter = filter;
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
    public FileStoreScan dropStats() {
        this.dropStats = true;
        return this;
    }

    @Nullable
    @Override
    public Integer parallelism() {
        return parallelism;
    }

    @Override
    public ManifestsReader manifestsReader() {
        return manifestsReader;
    }

    @Override
    public Plan plan() {
        long started = System.nanoTime();
        ManifestsReader.Result manifestsResult = readManifests();
        Snapshot snapshot = manifestsResult.snapshot;
        List<ManifestFileMeta> manifests = manifestsResult.filteredManifests;

        Iterator<ManifestEntry> iterator = readManifestEntries(manifests, false);
        List<ManifestEntry> files = new ArrayList<>();
        while (iterator.hasNext()) {
            files.add(iterator.next());
        }

        if (wholeBucketFilterEnabled()) {
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
        }

        List<ManifestEntry> result = files;

        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        if (scanMetrics != null) {
            long allDataFiles =
                    manifestsResult.allManifests.stream()
                            .mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles())
                            .sum();
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration,
                            manifests.size(),
                            allDataFiles - result.size(),
                            result.size()));
        }

        return new Plan() {
            @Nullable
            @Override
            public Long watermark() {
                return snapshot == null ? null : snapshot.watermark();
            }

            @Nullable
            @Override
            public Snapshot snapshot() {
                return snapshot;
            }

            @Override
            public List<ManifestEntry> files() {
                return result;
            }
        };
    }

    @Override
    public List<SimpleFileEntry> readSimpleEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Iterator<SimpleFileEntry> iterator =
                scanMode == ScanMode.ALL
                        ? readAndMergeFileEntries(manifests, SimpleFileEntry::from, false)
                        : readAndNoMergeFileEntries(manifests, SimpleFileEntry::from, false);
        List<SimpleFileEntry> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    @Override
    public List<PartitionEntry> readPartitionEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m -> PartitionEntry.merge(PartitionEntry.merge(readManifest(m)), partitions);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public List<BucketEntry> readBucketEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m -> BucketEntry.merge(BucketEntry.merge(readManifest(m)), buckets);
        randomlyOnlyExecute(getExecutorService(parallelism), processor, manifests);
        return buckets.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public Iterator<ManifestEntry> readFileIterator() {
        // useSequential: reduce memory and iterator can be stopping
        return readManifestEntries(readManifests().filteredManifests, true);
    }

    private Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifests, boolean useSequential) {
        return scanMode == ScanMode.ALL
                ? readAndMergeFileEntries(manifests, Function.identity(), useSequential)
                : readAndNoMergeFileEntries(manifests, Function.identity(), useSequential);
    }

    private <T extends FileEntry> Iterator<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<List<ManifestEntry>, List<T>> converter,
            boolean useSequential) {
        Set<Identifier> deletedEntries =
                FileEntry.readDeletedEntries(
                        manifest -> readManifest(manifest, FileEntry.deletedFilter(), null),
                        manifests,
                        parallelism);

        manifests =
                manifests.stream()
                        .filter(file -> file.numAddedFiles() > 0)
                        .collect(Collectors.toList());

        Function<ManifestFileMeta, List<T>> processor =
                manifest ->
                        converter.apply(
                                readManifest(
                                        manifest,
                                        FileEntry.addFilter(),
                                        entry -> !deletedEntries.contains(entry.identifier())));
        if (useSequential) {
            return sequentialBatchedExecute(processor, manifests, parallelism).iterator();
        } else {
            return randomlyExecuteSequentialReturn(processor, manifests, parallelism);
        }
    }

    private <T extends FileEntry> Iterator<T> readAndNoMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<List<ManifestEntry>, List<T>> converter,
            boolean useSequential) {
        Function<ManifestFileMeta, List<T>> reader =
                manifest -> converter.apply(readManifest(manifest));
        if (useSequential) {
            return sequentialBatchedExecute(reader, manifests, parallelism).iterator();
        } else {
            return randomlyExecuteSequentialReturn(reader, manifests, parallelism);
        }
    }

    private ManifestsReader.Result readManifests() {
        return manifestsReader.read(specifiedSnapshot, scanMode);
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
    protected abstract boolean filterByStats(ManifestEntry entry);

    protected boolean wholeBucketFilterEnabled() {
        return false;
    }

    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        return entries;
    }

    /** Note: Keep this thread-safe. */
    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return readManifest(manifest, null, null);
    }

    private List<ManifestEntry> readManifest(
            ManifestFileMeta manifest,
            @Nullable Filter<InternalRow> additionalFilter,
            @Nullable Filter<ManifestEntry> additionalTFilter) {
        List<ManifestEntry> entries =
                manifestFileFactory
                        .create()
                        .read(
                                manifest.fileName(),
                                manifest.fileSize(),
                                createCacheRowFilter(),
                                createEntryRowFilter().and(additionalFilter),
                                entry ->
                                        (additionalTFilter == null || additionalTFilter.test(entry))
                                                && (manifestEntryFilter == null
                                                        || manifestEntryFilter.test(entry))
                                                && filterByStats(entry));
        if (dropStats) {
            List<ManifestEntry> copied = new ArrayList<>(entries.size());
            for (ManifestEntry entry : entries) {
                copied.add(dropStats(entry));
            }
            entries = copied;
        }
        return entries;
    }

    protected ManifestEntry dropStats(ManifestEntry entry) {
        return entry.copyWithoutStats();
    }

    /**
     * According to the {@link ManifestCacheFilter}, entry that needs to be cached will be retained,
     * so the entry that will not be accessed in the future will not be cached.
     *
     * <p>Implemented to {@link InternalRow} is for performance (No deserialization).
     */
    private Filter<InternalRow> createCacheRowFilter() {
        if (manifestCacheFilter == null) {
            return Filter.alwaysTrue();
        }

        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        return row -> manifestCacheFilter.test(partitionGetter.apply(row), bucketGetter.apply(row));
    }

    /**
     * Read the corresponding entries based on the current required partition and bucket.
     *
     * <p>Implemented to {@link InternalRow} is for performance (No deserialization).
     */
    private Filter<InternalRow> createEntryRowFilter() {
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        Function<InternalRow, String> fileNameGetter = ManifestEntrySerializer.fileNameGetter();
        PartitionPredicate partitionFilter = manifestsReader.partitionFilter();
        Function<InternalRow, Integer> levelGetter = ManifestEntrySerializer.levelGetter();
        return row -> {
            if ((partitionFilter != null && !partitionFilter.test(partitionGetter.apply(row)))) {
                return false;
            }

            int bucket = bucketGetter.apply(row);
            if (bucketFilter != null && !bucketFilter.test(bucket)) {
                return false;
            }

            if (totalAwareBucketFilter != null
                    && !totalAwareBucketFilter.test(bucket, totalBucketGetter.apply(row))) {
                return false;
            }

            if (levelFilter != null && !levelFilter.test(levelGetter.apply(row))) {
                return false;
            }

            return fileNameFilter == null || fileNameFilter.test((fileNameGetter.apply(row)));
        };
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}
