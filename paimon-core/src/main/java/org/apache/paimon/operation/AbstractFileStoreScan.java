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
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestSidecar;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.ScanMetrics;
import org.apache.paimon.operation.metrics.ScanStats;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ListUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TriFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.getExecutorService;
import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/** Default implementation of {@link FileStoreScan}. */
public abstract class AbstractFileStoreScan implements FileStoreScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreScan.class);

    private final ManifestsReader manifestsReader;
    private final SnapshotManager snapshotManager;
    private final ManifestFile.Factory manifestFileFactory;
    private final Integer parallelism;
    private final String tableName;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final SchemaManager schemaManager;
    protected final TableSchema schema;

    private Snapshot specifiedSnapshot = null;
    private boolean onlyReadRealBuckets = false;
    private Integer specifiedBucket = null;
    private Filter<Integer> bucketFilter = null;
    private TriFilter<BinaryRow, Integer, Integer> totalAwareBucketFilter = null;
    protected ScanMode scanMode = ScanMode.ALL;
    private Integer specifiedLevel = null;
    private Filter<Integer> levelFilter = null;
    private Filter<ManifestEntry> manifestEntryFilter = null;
    private Filter<String> fileNameFilter = null;

    private ScanMetrics scanMetrics = null;
    private boolean dropStats;
    @Nullable protected RowRangeIndex rowRangeIndex = null;
    @Nullable protected Long limit;

    public AbstractFileStoreScan(
            ManifestsReader manifestsReader,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            @Nullable Integer parallelism,
            String tableName) {
        this.manifestsReader = manifestsReader;
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.manifestFileFactory = manifestFileFactory;
        this.tableSchemas = new ConcurrentHashMap<>();
        this.parallelism = parallelism;
        this.tableName = tableName;
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
    public FileStoreScan withPartitionsFilter(List<Map<String, String>> partitions) {
        manifestsReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public FileStoreScan withPartitionFilter(PartitionPredicate predicate) {
        manifestsReader.withPartitionFilter(predicate);
        return this;
    }

    @Override
    public FileStoreScan onlyReadRealBuckets() {
        manifestsReader.onlyReadRealBuckets();
        this.onlyReadRealBuckets = true;
        return this;
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        manifestsReader.withBucket(bucket);
        specifiedBucket = bucket;
        return this;
    }

    @Override
    public FileStoreScan withBucketFilter(Filter<Integer> bucketFilter) {
        manifestsReader.withBucketFilter(bucketFilter);
        this.bucketFilter = bucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withTotalAwareBucketFilter(
            TriFilter<BinaryRow, Integer, Integer> totalAwareBucketFilter) {
        manifestsReader.withTotalAwareBucketFilter(totalAwareBucketFilter);
        this.totalAwareBucketFilter = totalAwareBucketFilter;
        return this;
    }

    @Override
    public FileStoreScan withPartitionBucket(BinaryRow partition, int bucket) {
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
    public FileStoreScan withLevel(int level) {
        manifestsReader.withLevel(level);
        this.specifiedLevel = level;
        return this;
    }

    @Override
    public FileStoreScan withLevelFilter(Filter<Integer> levelFilter) {
        this.levelFilter = levelFilter;
        return this;
    }

    @Override
    public FileStoreScan withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        manifestsReader.withLevelMinMaxFilter(minMaxFilter);
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

    @Override
    public FileStoreScan keepStats() {
        this.dropStats = false;
        return this;
    }

    @Override
    public FileStoreScan withRowRanges(List<Range> rowRanges) {
        if (rowRanges == null) {
            this.rowRangeIndex = null;
            manifestsReader.withRowRangeIndex(null);
            return this;
        }
        this.rowRangeIndex = RowRangeIndex.create(rowRanges);
        manifestsReader.withRowRangeIndex(this.rowRangeIndex);
        return this;
    }

    @Override
    public FileStoreScan withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        if (rowRangeIndex == null) {
            this.rowRangeIndex = null;
            manifestsReader.withRowRangeIndex(null);
            return this;
        }
        this.rowRangeIndex = rowRangeIndex;
        manifestsReader.withRowRangeIndex(rowRangeIndex);
        return this;
    }

    @Override
    public FileStoreScan withReadType(RowType readType) {
        return this;
    }

    @Override
    public FileStoreScan withLimit(long limit) {
        this.limit = limit;
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

        RoaringNavigableMap64 candidates = manifestsReader.manifestGroupCandidates();
        boolean pruneGroups = scanMode == ScanMode.ALL && candidates != null && limit == null;
        Map<ManifestFileMeta, List<ManifestEntry>> mixedEntries =
                pruneGroups ? readMixedManifests(manifests) : Collections.emptyMap();
        RowRangeIndex retainedGroups = null;
        if (!mixedEntries.isEmpty()) {
            retainedGroups =
                    ManifestsReader.selectManifestGroupRanges(manifests, candidates, mixedEntries);
            manifests = ManifestsReader.filterManifestsByRowRanges(manifests, retainedGroups);
        }
        // Refine after the mixed manifest pass so we only open sidecars for surviving ADDs.
        Map<ManifestFileMeta, Optional<ManifestSidecar.Selection>> addBlocks =
                pruneGroups ? readAddBlockIndexes(manifests) : Collections.emptyMap();
        if (addBlocks.values().stream().anyMatch(Optional::isPresent)) {
            RowRangeIndex blockGroups =
                    ManifestsReader.selectManifestGroupRanges(
                            manifests, candidates, mixedEntries, addBlocks);
            if (blockGroups != null) {
                retainedGroups = blockGroups;
                manifests = ManifestsReader.filterManifestsByRowRanges(manifests, retainedGroups);
            }
        }
        Iterator<ManifestEntry> iterator =
                mixedEntries.isEmpty() && addBlocks.isEmpty()
                        ? readManifestEntries(manifests, false)
                        : readAndMergeFileEntries(
                                manifests,
                                Function.identity(),
                                false,
                                mixedEntries,
                                retainedGroups,
                                addBlocks);

        List<ManifestEntry> files = ListUtils.toList(iterator);
        if (postFilterManifestEntriesEnabled()) {
            files = postFilterManifestEntries(files);
        }

        List<ManifestEntry> result = files;

        long scanDuration = (System.nanoTime() - started) / 1_000_000;
        LOG.info("File store scan plan completed in {} ms for table {}", scanDuration, tableName);
        if (scanMetrics != null) {
            long allDataFiles =
                    manifestsResult.allManifests.stream()
                            .mapToLong(f -> f.numAddedFiles() - f.numDeletedFiles())
                            .sum();
            long scannedManifests =
                    mixedEntries.isEmpty()
                            ? manifests.size()
                            : mixedEntries.size()
                                    + manifests.stream()
                                            .filter(manifest -> !mixedEntries.containsKey(manifest))
                                            .count();
            scanMetrics.reportScan(
                    new ScanStats(
                            scanDuration,
                            snapshot == null ? 0 : snapshot.id(),
                            scannedManifests,
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
        Set<Identifier> deletedEntries =
                FileEntry.readDeletedEntries(
                        manifest ->
                                readManifest(
                                        manifest,
                                        SimpleFileEntry::from,
                                        FileEntry.deletedFilter(),
                                        null),
                        manifests,
                        parallelism);

        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                manifest -> {
                    Map<BinaryRow, PartitionEntry> entries = new HashMap<>();
                    for (ManifestEntry manifestEntry :
                            readManifest(
                                    manifest,
                                    Function.identity(),
                                    FileEntry.addFilter(),
                                    entry -> !deletedEntries.contains(entry.identifier()))) {
                        PartitionEntry entry = PartitionEntry.fromManifestEntry(manifestEntry);
                        entries.compute(
                                entry.partition(),
                                (partition, old) -> old == null ? entry : old.merge(entry));
                    }
                    PartitionEntry.merge(entries.values(), partitions);
                };
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                processor,
                manifests.stream()
                        .filter(manifest -> manifest.numAddedFiles() > 0)
                        .collect(Collectors.toList()));
        return partitions.values().stream()
                .filter(p -> p.fileCount() > 0)
                .collect(Collectors.toList());
    }

    @Override
    public List<BucketEntry> readBucketEntries() {
        List<ManifestFileMeta> manifests = readManifests().filteredManifests;
        Map<Pair<BinaryRow, Integer>, BucketEntry> buckets = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m ->
                        BucketEntry.merge(
                                readManifest(m, BucketEntry::fromManifestEntry, null, null),
                                buckets);
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

    @Override
    public Iterator<ManifestEntry> readFileIterator(List<ManifestFileMeta> manifestFileMetas) {
        // useSequential: reduce memory and iterator can be stopping
        return readManifestEntries(manifestFileMetas, true);
    }

    public Iterator<ManifestEntry> readManifestEntries(
            List<ManifestFileMeta> manifests, boolean useSequential) {
        return scanMode == ScanMode.ALL
                ? readAndMergeFileEntries(manifests, Function.identity(), useSequential)
                : readAndNoMergeFileEntries(manifests, Function.identity(), useSequential);
    }

    private Map<ManifestFileMeta, List<ManifestEntry>> readMixedManifests(
            List<ManifestFileMeta> manifests) {
        // The coarse filter already selected these manifests. Load both kinds in one pass using
        // the normal scan filters, so refinement and the final merge can share the entries.
        List<ManifestFileMeta> mixed =
                manifests.stream()
                        .filter(
                                manifest ->
                                        manifest.numAddedFiles() > 0
                                                && manifest.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        if (mixed.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<ManifestFileMeta, List<ManifestEntry>> entries = new ConcurrentHashMap<>();
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                manifest -> entries.put(manifest, readManifest(manifest)),
                mixed);
        return entries;
    }

    private Map<ManifestFileMeta, Optional<ManifestSidecar.Selection>> readAddBlockIndexes(
            List<ManifestFileMeta> manifests) {
        // A v1 sidecar covers both entry kinds. Only ADD-only blocks can define file groups.
        List<ManifestFileMeta> indexed =
                manifests.stream()
                        .filter(
                                manifest ->
                                        manifest.numAddedFiles() > 1
                                                && manifest.numDeletedFiles() == 0
                                                && ManifestSidecar.fileName(manifest) != null)
                        .collect(Collectors.toList());
        if (indexed.isEmpty()) {
            return Collections.emptyMap();
        }
        PartitionPredicate partitionFilter = manifestsReader.partitionFilter();
        BucketFilter bucketFilter = createBucketFilter();
        Map<ManifestFileMeta, Optional<ManifestSidecar.Selection>> selections =
                new ConcurrentHashMap<>();
        randomlyOnlyExecute(
                getExecutorService(parallelism),
                manifest -> {
                    ManifestSidecar.Selection selection =
                            manifestFileFactory
                                    .create()
                                    .readBlockIndex(
                                            manifest, rowRangeIndex, partitionFilter, bucketFilter);
                    selections.put(manifest, Optional.ofNullable(selection));
                },
                indexed);
        return selections;
    }

    private <T extends FileEntry> Iterator<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<ManifestEntry, T> converter,
            boolean useSequential) {
        return readAndMergeFileEntries(
                manifests,
                converter,
                useSequential,
                Collections.emptyMap(),
                null,
                Collections.emptyMap());
    }

    private <T extends FileEntry> Iterator<T> readAndMergeFileEntries(
            List<ManifestFileMeta> manifests,
            Function<ManifestEntry, T> converter,
            boolean useSequential,
            Map<ManifestFileMeta, List<ManifestEntry>> mixedEntries,
            @Nullable RowRangeIndex retainedGroups,
            Map<ManifestFileMeta, Optional<ManifestSidecar.Selection>> addBlocks) {
        Set<Identifier> deletedEntries =
                FileEntry.readDeletedEntries(
                        manifest -> {
                            List<ManifestEntry> entries = mixedEntries.get(manifest);
                            return entries == null
                                    ? readManifest(
                                            manifest,
                                            SimpleFileEntry::from,
                                            FileEntry.deletedFilter(),
                                            null)
                                    : entries.stream()
                                            .filter(entry -> entry.kind() == FileKind.DELETE)
                                            .map(SimpleFileEntry::from)
                                            .collect(Collectors.toList());
                        },
                        manifests,
                        parallelism);

        manifests =
                manifests.stream()
                        .filter(file -> file.numAddedFiles() > 0)
                        .collect(Collectors.toList());

        Filter<ManifestEntry> notDeleted = entry -> !deletedEntries.contains(entry.identifier());
        Function<ManifestFileMeta, List<T>> processor =
                manifest -> {
                    List<ManifestEntry> entries = mixedEntries.get(manifest);
                    Optional<ManifestSidecar.Selection> blocks = addBlocks.get(manifest);
                    if (blocks != null) {
                        return readManifest(
                                manifest,
                                converter,
                                FileEntry.addFilter(),
                                notDeleted,
                                blocks.map(selection -> selection.withRowGroups(retainedGroups)));
                    }
                    // A mixed manifest retained for its DELETEs can still have unrelated ADDs.
                    return entries == null
                            ? readManifest(manifest, converter, FileEntry.addFilter(), notDeleted)
                            : entries.stream()
                                    .filter(
                                            entry ->
                                                    entry.kind() == FileKind.ADD
                                                            && notDeleted.test(entry)
                                                            && (retainedGroups == null
                                                                    || retainedGroups.contains(
                                                                            entry.file()
                                                                                    .nonNullRowIdRange())))
                                    .map(converter)
                                    .collect(Collectors.toList());
                };
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

    protected boolean postFilterManifestEntriesEnabled() {
        return false;
    }

    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        throw new UnsupportedOperationException();
    }

    /** Note: Keep this thread-safe. */
    @Override
    public List<ManifestEntry> readManifest(ManifestFileMeta manifest) {
        return readManifest(manifest, Function.identity(), null, null);
    }

    private <T> List<T> readManifest(
            ManifestFileMeta manifest,
            Function<ManifestEntry, T> converter,
            @Nullable Filter<InternalRow> additionalFilter,
            @Nullable Filter<ManifestEntry> additionalTFilter) {
        return readManifest(manifest, converter, additionalFilter, additionalTFilter, null);
    }

    private <T> List<T> readManifest(
            ManifestFileMeta manifest,
            Function<ManifestEntry, T> converter,
            @Nullable Filter<InternalRow> additionalFilter,
            @Nullable Filter<ManifestEntry> additionalTFilter,
            @Nullable Optional<ManifestSidecar.Selection> prefetchedBlocks) {
        ManifestFile manifestFile = manifestFileFactory.create();
        BucketFilter bucketFilter = createBucketFilter();
        // An empty prefetched result already selected the ordinary manifest read.
        ManifestSidecar.Selection selected =
                prefetchedBlocks == null
                        ? manifestFile.selectBlocks(
                                manifest,
                                rowRangeIndex,
                                manifestsReader.partitionFilter(),
                                bucketFilter)
                        : prefetchedBlocks.orElse(null);
        if (selected != null && selected.blocks().isEmpty()) {
            return Collections.emptyList();
        }
        Filter<InternalRow> entryRowFilter = createEntryRowFilter();
        Function<ManifestEntry, T> finalConverter =
                dropStats ? e -> converter.apply(dropStats(e)) : converter;

        List<T> entries =
                manifestFile
                        .withCacheMetrics(
                                scanMetrics != null ? scanMetrics.getCacheMetrics() : null)
                        .read(
                                manifest.fileName(),
                                manifest.fileSize(),
                                manifestsReader.partitionFilter(),
                                bucketFilter,
                                entryRowFilter.and(additionalFilter),
                                entry ->
                                        (additionalTFilter == null || additionalTFilter.test(entry))
                                                && (manifestEntryFilter == null
                                                        || manifestEntryFilter.test(entry))
                                                && filterByStats(entry),
                                finalConverter,
                                selected);
        LOG.info("Read {} manifest entries from {}", entries.size(), manifest.fileName());
        return entries;
    }

    protected ManifestEntry dropStats(ManifestEntry entry) {
        return entry.copyWithoutStats();
    }

    private BucketFilter createBucketFilter() {
        return BucketFilter.create(
                onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
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
        BucketFilter bucketFilter = createBucketFilter();
        return row -> {
            BinaryRow partition = null;
            if (partitionFilter != null) {
                partition = partitionGetter.apply(row);
                if (!partitionFilter.test(partition)) {
                    return false;
                }
            }

            if (bucketFilter != null) {
                int bucket = bucketGetter.apply(row);
                int totalBucket = totalBucketGetter.apply(row);
                if (partition == null) {
                    partition = partitionGetter.apply(row);
                }
                if (!bucketFilter.test(partition, bucket, totalBucket)) {
                    return false;
                }
            }

            int level = levelGetter.apply(row);
            if (specifiedLevel != null && level != specifiedLevel) {
                return false;
            }

            if (levelFilter != null && !levelFilter.test(level)) {
                return false;
            }

            return fileNameFilter == null || fileNameFilter.test((fileNameGetter.apply(row)));
        };
    }

    // ------------------------------------------------------------------------
    // End Thread Safe Methods
    // ------------------------------------------------------------------------
}
