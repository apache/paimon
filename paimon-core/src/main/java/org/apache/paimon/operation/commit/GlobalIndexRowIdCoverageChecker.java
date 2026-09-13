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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.ProjectedDataFileMeta;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.manifest.ProjectedManifestEntry.Projection;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ByteArrayKey;
import org.apache.paimon.utils.ByteArrayLookupKey;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.PrimitiveRowRanges;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Checks global-index RowID coverage without materializing complete data-file entries. */
public final class GlobalIndexRowIdCoverageChecker {

    private static final Projection LIVE_FILE_PROJECTION =
            manifestProjection(
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.LEVEL,
                    DataFileMeta.EXTRA_FILES,
                    DataFileMeta.EMBEDDED_FILE_INDEX,
                    DataFileMeta.EXTERNAL_PATH,
                    DataFileMeta.FIRST_ROW_ID);

    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final RowType partitionType;

    public GlobalIndexRowIdCoverageChecker(
            ManifestFile manifestFile, ManifestList manifestList, RowType partitionType) {
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;
        this.partitionType = partitionType;
    }

    public static boolean canCheck(List<IndexManifestEntry> indexChanges) {
        boolean hasGlobalIndexAddition = false;
        for (IndexManifestEntry entry : indexChanges) {
            if (DELETION_VECTORS_INDEX.equals(entry.indexFile().indexType())) {
                return false;
            }
            hasGlobalIndexAddition |=
                    entry.kind() == FileKind.ADD && entry.indexFile().globalIndexMeta() != null;
        }
        return hasGlobalIndexAddition;
    }

    public Optional<RuntimeException> check(
            Snapshot snapshot, List<IndexManifestEntry> indexChanges) {
        List<IndexManifestEntry> indexes = new ArrayList<>();
        for (IndexManifestEntry entry : indexChanges) {
            if (entry.kind() == FileKind.ADD && entry.indexFile().globalIndexMeta() != null) {
                indexes.add(entry);
            }
        }

        CoveragePlan coveragePlan = new CoveragePlan(indexes);
        List<ManifestFileMeta> manifests = candidateManifests(snapshot, coveragePlan);
        CompactFileIdentifierSet deletes = new CompactFileIdentifierSet();
        ReusableIdentifier identifier = new ReusableIdentifier();
        ByteArrayLookupKey partitionLookup = new ByteArrayLookupKey();
        try {
            collectDeletes(manifests, coveragePlan, deletes, partitionLookup);
            collectLiveRanges(manifests, coveragePlan, deletes, identifier, partitionLookup);
        } catch (Exception e) {
            throw new RuntimeException("Cannot check global-index RowIDs from data manifests.", e);
        } finally {
            deletes.release();
            identifier.release();
            partitionLookup.clear();
        }

        for (IndexManifestEntry index : indexes) {
            GlobalIndexMeta globalIndex = index.indexFile().globalIndexMeta();
            Coverage coverage = coveragePlan.forIndex(index);
            if (!coverage.liveRanges.covers(
                    globalIndex.rowRangeStart(), globalIndex.rowRangeEnd())) {
                return Optional.of(conflict(index));
            }
        }
        return Optional.empty();
    }

    private List<ManifestFileMeta> candidateManifests(
            Snapshot snapshot, CoveragePlan coveragePlan) {
        PartitionPredicate partitionPredicate =
                PartitionPredicate.fromMultiple(partitionType, coveragePlan.byPartition.keySet());
        List<ManifestFileMeta> result = new ArrayList<>();
        for (ManifestFileMeta manifest : manifestList.readDataManifests(snapshot)) {
            Long minRowId = manifest.minRowId();
            Long maxRowId = manifest.maxRowId();
            if (minRowId != null
                    && maxRowId != null
                    && !coveragePlan.requestedRanges.overlaps(minRowId, maxRowId)) {
                continue;
            }
            if (partitionPredicate != null
                    && !partitionPredicate.test(
                            manifest.numAddedFiles() + manifest.numDeletedFiles(),
                            manifest.partitionStats().minValues(),
                            manifest.partitionStats().maxValues(),
                            manifest.partitionStats().nullCounts())) {
                continue;
            }
            result.add(manifest);
        }
        return result;
    }

    private void collectDeletes(
            List<ManifestFileMeta> manifests,
            CoveragePlan coveragePlan,
            CompactFileIdentifierSet deletes,
            ByteArrayLookupKey partitionLookup)
            throws Exception {
        ReusableIdentifier identifier = new ReusableIdentifier();
        try {
            for (ManifestFileMeta manifest : manifests) {
                if (manifest.numDeletedFiles() == 0) {
                    continue;
                }
                try (CloseableIterator<ProjectedManifestEntry> entries =
                        manifestFile.scan(
                                manifest.fileName(),
                                ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
                    while (entries.hasNext()) {
                        ProjectedManifestEntry entry = entries.next();
                        if (!entry.isDelete()) {
                            continue;
                        }
                        Coverage coverage = coveragePlan.coverage(entry, partitionLookup);
                        if (coverage != null) {
                            deletes.add(coverage.id, identifier.replace(entry));
                        }
                    }
                }
            }
        } finally {
            identifier.release();
        }
    }

    private void collectLiveRanges(
            List<ManifestFileMeta> manifests,
            CoveragePlan coveragePlan,
            CompactFileIdentifierSet deletes,
            ReusableIdentifier identifier,
            ByteArrayLookupKey partitionLookup)
            throws Exception {
        for (ManifestFileMeta manifest : manifests) {
            if (manifest.numAddedFiles() == 0) {
                continue;
            }
            try (CloseableIterator<ProjectedManifestEntry> entries =
                    manifestFile.scan(manifest.fileName(), LIVE_FILE_PROJECTION)) {
                while (entries.hasNext()) {
                    ProjectedManifestEntry entry = entries.next();
                    if (!entry.isAdd()) {
                        continue;
                    }
                    Coverage coverage = coveragePlan.coverage(entry, partitionLookup);
                    if (coverage == null
                            || deletes.contains(coverage.id, identifier.replace(entry))) {
                        continue;
                    }
                    ProjectedDataFileMeta file = entry.file();
                    if (!file.hasFirstRowId()) {
                        continue;
                    }
                    long start = file.nonNullFirstRowId();
                    long end = Math.addExact(start, file.rowCount() - 1L);
                    if (coverage.requestedRanges.overlaps(start, end)) {
                        coverage.liveRanges.add(start, end);
                    }
                }
            }
        }
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

    private static RuntimeException conflict(IndexManifestEntry index) {
        return new RuntimeException(
                String.format(
                        "Global index row ID existence conflict: index file '%s' references row "
                                + "range %s, but this range is not fully covered by current data "
                                + "files. The referenced row IDs may have been reassigned or "
                                + "removed by a concurrent commit.",
                        index.indexFile().fileName(),
                        index.indexFile().globalIndexMeta().rowRange()));
    }

    private static final class Coverage {

        private final int id;
        private final PrimitiveRowRanges requestedRanges = new PrimitiveRowRanges(1);
        private final PrimitiveRowRanges liveRanges = new PrimitiveRowRanges(16);

        private Coverage(int id) {
            this.id = id;
        }
    }

    private static final class CoveragePlan {

        private final Map<BinaryRow, Map<Integer, Coverage>> byPartition = new LinkedHashMap<>();
        private final Map<ByteArrayKey, Map<Integer, Coverage>> byPartitionBytes = new HashMap<>();
        private final PrimitiveRowRanges requestedRanges;

        private CoveragePlan(List<IndexManifestEntry> indexes) {
            requestedRanges = new PrimitiveRowRanges(indexes.size());
            int nextCoverageId = 0;
            for (IndexManifestEntry index : indexes) {
                GlobalIndexMeta globalIndex = index.indexFile().globalIndexMeta();
                Map<Integer, Coverage> byBucket = byPartition.get(index.partition());
                if (byBucket == null) {
                    byBucket = new HashMap<>();
                    byPartition.put(index.partition().copy(), byBucket);
                }
                Coverage coverage = byBucket.get(index.bucket());
                if (coverage == null) {
                    coverage = new Coverage(nextCoverageId++);
                    byBucket.put(index.bucket(), coverage);
                }
                coverage.requestedRanges.add(
                        globalIndex.rowRangeStart(), globalIndex.rowRangeEnd());
                requestedRanges.add(globalIndex.rowRangeStart(), globalIndex.rowRangeEnd());
            }
            byPartition.forEach(
                    (partition, byBucket) ->
                            byPartitionBytes.put(
                                    new ByteArrayKey(serializeBinaryRow(partition)), byBucket));
        }

        private Coverage forIndex(IndexManifestEntry index) {
            return byPartition.get(index.partition()).get(index.bucket());
        }

        private @Nullable Coverage coverage(
                ProjectedManifestEntry entry, ByteArrayLookupKey partitionLookup) {
            partitionLookup.reset(entry.partitionBytes());
            try {
                Map<Integer, Coverage> byBucket = byPartitionBytes.get(partitionLookup);
                return byBucket == null ? null : byBucket.get(entry.bucket());
            } finally {
                partitionLookup.clear();
            }
        }
    }
}
