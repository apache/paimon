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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BinaryManifestEntry;
import org.apache.paimon.manifest.BinaryManifestEntry.Projection;
import org.apache.paimon.manifest.BinaryManifestEntry.ReusableIdentifier;
import org.apache.paimon.manifest.DeletedIdentifierSet;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.PrimitiveRowRanges;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Manifest entries scanner for commit. */
public class CommitScanner {

    private static final Projection GLOBAL_INDEX_ROW_ID_PROJECTION =
            manifestProjection(
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.LEVEL,
                    DataFileMeta.EXTRA_FILES,
                    DataFileMeta.EMBEDDED_FILE_INDEX,
                    DataFileMeta.EXTERNAL_PATH,
                    DataFileMeta.FIRST_ROW_ID);

    private final FileStoreScan scan;
    private final Supplier<FileStoreScan> scanSupplier;
    private final SnapshotManager snapshotManager;
    private final IndexManifestFile indexManifestFile;
    private final @Nullable ManifestFile manifestFile;
    private final @Nullable ManifestList manifestList;
    private final @Nullable RowType partitionType;
    private final boolean dropStats;

    public CommitScanner(
            Supplier<FileStoreScan> scanSupplier,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            CoreOptions options) {
        this(scanSupplier, snapshotManager, indexManifestFile, null, null, null, options);
    }

    public CommitScanner(
            Supplier<FileStoreScan> scanSupplier,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            @Nullable ManifestFile manifestFile,
            @Nullable ManifestList manifestList,
            @Nullable RowType partitionType,
            CoreOptions options) {
        this.scanSupplier = scanSupplier;
        this.scan = scanSupplier.get();
        this.snapshotManager = snapshotManager;
        this.indexManifestFile = indexManifestFile;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.partitionType = partitionType;
        // Stats in DELETE Manifest Entries is useless
        this.dropStats = options.manifestDeleteFileDropStats();
        if (dropStats) {
            this.scan.dropStats();
        }
    }

    public List<SimpleFileEntry> readIncrementalChanges(
            Snapshot from, Snapshot to, List<BinaryRow> changedPartitions) {
        List<SimpleFileEntry> entries = new ArrayList<>();
        for (long i = from.id() + 1; i <= to.id(); i++) {
            List<SimpleFileEntry> delta =
                    scan.withSnapshot(i)
                            .withKind(ScanMode.DELTA)
                            .withPartitionFilter(changedPartitions)
                            .readSimpleEntries();
            entries.addAll(delta);
        }
        return entries;
    }

    public List<ManifestEntry> readIncrementalEntries(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        return scan.withSnapshot(snapshot)
                .withKind(ScanMode.DELTA)
                .withPartitionFilter(changedPartitions)
                .plan()
                .files();
    }

    public List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        try {
            return scan.withSnapshot(snapshot)
                    .withKind(ScanMode.ALL)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    /**
     * Checks global-index row ranges by streaming projected binary manifest entries.
     *
     * <p>Only live data ranges which intersect a requested index range are retained. This avoids
     * materializing all data files in the changed partitions as {@link SimpleFileEntry}s.
     *
     * @return the first index entry whose row range is not covered by live data files
     */
    public Optional<IndexManifestEntry> firstGlobalIndexWithMissingRowIds(
            Snapshot snapshot, List<IndexManifestEntry> indexesToCheck) {
        if (indexesToCheck.isEmpty()) {
            return Optional.empty();
        }
        if (manifestFile == null || manifestList == null || partitionType == null) {
            throw new IllegalStateException(
                    "Projected manifest scanning is not configured for this commit scanner.");
        }

        try {
            GlobalIndexCoveragePlan coveragePlan = createGlobalIndexCoverage(indexesToCheck);
            RowRangeIndex requestedRanges = requestedRanges(indexesToCheck);
            List<ManifestFileMeta> candidateManifests =
                    candidateManifests(snapshot, coveragePlan.targetPartitions(), requestedRanges);

            DeletedIdentifierSet deletedIdentifiers = new DeletedIdentifierSet();
            try {
                collectDeletedIdentifiers(
                        candidateManifests, coveragePlan.byPartitionBytes, deletedIdentifiers);
                collectLiveRowIdRanges(
                        candidateManifests, coveragePlan.byPartitionBytes, deletedIdentifiers);
            } finally {
                deletedIdentifiers.release();
            }

            for (IndexManifestEntry indexEntry : indexesToCheck) {
                GlobalIndexMeta index = indexEntry.indexFile().globalIndexMeta();
                GlobalIndexCoverage coverage = coveragePlan.forIndex(indexEntry);
                if (!coverage.liveDataRanges.covers(index.rowRangeStart(), index.rowRangeEnd())) {
                    return Optional.of(indexEntry);
                }
            }
            return Optional.empty();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot check global-index row IDs from data manifests.", e);
        }
    }

    private static GlobalIndexCoveragePlan createGlobalIndexCoverage(
            List<IndexManifestEntry> indexesToCheck) {
        Map<BinaryRow, Map<Integer, GlobalIndexCoverage>> byPartitionRows = new HashMap<>();
        int nextCoverageId = 0;
        for (IndexManifestEntry indexEntry : indexesToCheck) {
            GlobalIndexMeta index = indexEntry.indexFile().globalIndexMeta();
            BinaryRow partition = indexEntry.partition();
            Map<Integer, GlobalIndexCoverage> byBucket = byPartitionRows.get(partition);
            if (byBucket == null) {
                byBucket = new HashMap<>();
                byPartitionRows.put(partition.copy(), byBucket);
            }
            GlobalIndexCoverage coverage = byBucket.get(indexEntry.bucket());
            if (coverage == null) {
                coverage = new GlobalIndexCoverage(nextCoverageId++);
                byBucket.put(indexEntry.bucket(), coverage);
            }
            coverage.requestedRanges.add(index.rowRange());
        }
        Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> byPartitionBytes = new HashMap<>();
        for (Map.Entry<BinaryRow, Map<Integer, GlobalIndexCoverage>> partition :
                byPartitionRows.entrySet()) {
            byPartitionBytes.put(
                    new ByteArrayKey(serializeBinaryRow(partition.getKey())), partition.getValue());
        }
        for (Map<Integer, GlobalIndexCoverage> byBucket : byPartitionRows.values()) {
            for (GlobalIndexCoverage coverage : byBucket.values()) {
                coverage.prepare();
            }
        }
        return new GlobalIndexCoveragePlan(byPartitionRows, byPartitionBytes);
    }

    private static RowRangeIndex requestedRanges(List<IndexManifestEntry> indexesToCheck) {
        List<Range> ranges = new ArrayList<>(indexesToCheck.size());
        for (IndexManifestEntry entry : indexesToCheck) {
            ranges.add(entry.indexFile().globalIndexMeta().rowRange());
        }
        return RowRangeIndex.create(ranges);
    }

    private List<ManifestFileMeta> candidateManifests(
            Snapshot snapshot, Set<BinaryRow> targetPartitions, RowRangeIndex requestedRanges) {
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromMultiple(partitionType, targetPartitions);
        List<ManifestFileMeta> result = new ArrayList<>();
        for (ManifestFileMeta meta : manifestList.readDataManifests(snapshot)) {
            Long minRowId = meta.minRowId();
            Long maxRowId = meta.maxRowId();
            if (minRowId != null
                    && maxRowId != null
                    && !requestedRanges.intersects(minRowId, maxRowId)) {
                continue;
            }
            if (partitionPredicate != null
                    && !partitionPredicate.test(
                            meta.numAddedFiles() + meta.numDeletedFiles(),
                            meta.partitionStats().minValues(),
                            meta.partitionStats().maxValues(),
                            meta.partitionStats().nullCounts())) {
                continue;
            }
            result.add(meta);
        }
        return result;
    }

    private void collectDeletedIdentifiers(
            List<ManifestFileMeta> manifests,
            Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> coverageByPartition,
            DeletedIdentifierSet deletedIdentifiers)
            throws Exception {
        ReusableIdentifier identifier = new ReusableIdentifier();
        ByteArrayLookupKey partitionLookup = new ByteArrayLookupKey();
        try {
            for (ManifestFileMeta meta : manifests) {
                if (meta.numDeletedFiles() == 0) {
                    continue;
                }
                try (CloseableIterator<BinaryManifestEntry> entries =
                        manifestFile.scan(
                                meta.fileName(),
                                meta.fileSize(),
                                BinaryManifestEntry.DELETE_ENTRY_PROJECTION)) {
                    while (entries.hasNext()) {
                        BinaryManifestEntry entry = entries.next();
                        if (!entry.isDelete()) {
                            continue;
                        }
                        GlobalIndexCoverage coverage =
                                requestedBucket(entry, coverageByPartition, partitionLookup);
                        if (coverage != null) {
                            deletedIdentifiers.add(coverage.id, identifier.replace(entry));
                        }
                    }
                }
            }
        } finally {
            identifier.release();
            partitionLookup.clear();
        }
    }

    private void collectLiveRowIdRanges(
            List<ManifestFileMeta> manifests,
            Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> coverageByPartition,
            DeletedIdentifierSet deletedIdentifiers)
            throws Exception {
        ReusableIdentifier identifier = new ReusableIdentifier();
        ByteArrayLookupKey partitionLookup = new ByteArrayLookupKey();
        try {
            for (ManifestFileMeta meta : manifests) {
                if (meta.numAddedFiles() == 0) {
                    continue;
                }
                try (CloseableIterator<BinaryManifestEntry> entries =
                        manifestFile.scan(
                                meta.fileName(), meta.fileSize(), GLOBAL_INDEX_ROW_ID_PROJECTION)) {
                    while (entries.hasNext()) {
                        BinaryManifestEntry entry = entries.next();
                        if (!entry.isAdd()) {
                            continue;
                        }
                        GlobalIndexCoverage coverage =
                                requestedBucket(entry, coverageByPartition, partitionLookup);
                        if (coverage == null
                                || (!deletedIdentifiers.isEmpty()
                                        && deletedIdentifiers.contains(
                                                coverage.id, identifier.replace(entry)))) {
                            continue;
                        }
                        BinaryDataFileMeta file = entry.file();
                        if (!file.hasFirstRowId()) {
                            continue;
                        }
                        long start = file.firstRowId();
                        long end = Math.addExact(start, file.rowCount() - 1L);
                        if (coverage.requestedRangeIndex.intersects(start, end)) {
                            coverage.liveDataRanges.add(start, end);
                        }
                    }
                }
            }
        } finally {
            identifier.release();
            partitionLookup.clear();
        }
    }

    @Nullable
    private static GlobalIndexCoverage requestedBucket(
            BinaryManifestEntry entry,
            Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> coverageByPartition,
            ByteArrayLookupKey partitionLookup) {
        partitionLookup.reset(entry.partitionBytes());
        Map<Integer, GlobalIndexCoverage> byBucket = coverageByPartition.get(partitionLookup);
        return byBucket == null ? null : byBucket.get(entry.bucket());
    }

    private static Projection manifestProjection(String... projectedFileFields) {
        List<DataField> fields =
                new ArrayList<>(
                        Arrays.asList(
                                ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.KIND),
                                ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.PARTITION),
                                ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET),
                                ManifestEntry.MANIFEST_ROW_TYPE
                                        .getField(ManifestEntry.FILE)
                                        .newType(
                                                DataFileMeta.SCHEMA.project(projectedFileFields))));
        return Projection.create(new RowType(false, fields));
    }

    private static class GlobalIndexCoverage {

        private final int id;
        private final List<Range> requestedRanges = new ArrayList<>();
        private final PrimitiveRowRanges liveDataRanges = new PrimitiveRowRanges(16);
        private RowRangeIndex requestedRangeIndex;

        private GlobalIndexCoverage(int id) {
            this.id = id;
        }

        private void prepare() {
            requestedRangeIndex = RowRangeIndex.create(requestedRanges);
        }
    }

    private static class GlobalIndexCoveragePlan {

        private final Map<BinaryRow, Map<Integer, GlobalIndexCoverage>> byPartitionRows;
        private final Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> byPartitionBytes;

        private GlobalIndexCoveragePlan(
                Map<BinaryRow, Map<Integer, GlobalIndexCoverage>> byPartitionRows,
                Map<ByteArrayKey, Map<Integer, GlobalIndexCoverage>> byPartitionBytes) {
            this.byPartitionRows = byPartitionRows;
            this.byPartitionBytes = byPartitionBytes;
        }

        private Set<BinaryRow> targetPartitions() {
            return byPartitionRows.keySet();
        }

        private GlobalIndexCoverage forIndex(IndexManifestEntry entry) {
            return byPartitionRows.get(entry.partition()).get(entry.bucket());
        }
    }

    public Map<BinaryRow, Integer> readTotalBuckets(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        try {
            Set<BinaryRow> remainingPartitions = new HashSet<>(changedPartitions);
            Map<BinaryRow, Integer> totalBuckets = new HashMap<>();
            FileStoreScan freshScan = scanSupplier.get();
            if (dropStats) {
                freshScan.dropStats();
            }
            Iterator<ManifestEntry> iterator =
                    freshScan
                            .withSnapshot(snapshot)
                            .withKind(ScanMode.ALL)
                            .withPartitionFilter(changedPartitions)
                            .readFileIterator();
            while (iterator.hasNext() && !remainingPartitions.isEmpty()) {
                ManifestEntry entry = iterator.next();
                int totalBucket = entry.totalBuckets();
                if (totalBucket > 0 && remainingPartitions.remove(entry.partition())) {
                    totalBuckets.put(entry.partition(), totalBucket);
                }
            }
            return totalBuckets;
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read total buckets from changed partitions.", e);
        }
    }

    /**
     * Returns a stateful {@link CommitChangesProvider} for overwrite operations. The returned
     * provider caches the current files of the target partitions across retries and only walks
     * delta manifests when the latest snapshot advances, avoiding repeated full scans on every
     * commit retry.
     */
    public CommitChangesProvider overwriteChangesProvider(
            int numBucket,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            @Nullable PartitionPredicate partitionFilter) {
        return new OverwriteChangesProvider(
                scanSupplier,
                snapshotManager,
                indexManifestFile,
                dropStats,
                numBucket,
                changes,
                indexFiles,
                partitionFilter);
    }
}
