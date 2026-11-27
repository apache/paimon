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
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.ManifestSidecar;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TriFilter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createBinaryPartitions;

/** A util class to read manifest files. */
@ThreadSafe
public class ManifestsReader {

    private final RowType partitionType;
    private final String partitionDefaultValue;
    private final SnapshotManager snapshotManager;
    private final ManifestList.Factory manifestListFactory;

    private boolean onlyReadRealBuckets = false;
    @Nullable private Integer specifiedBucket = null;
    @Nullable private Filter<Integer> bucketFilter = null;
    @Nullable private TriFilter<BinaryRow, Integer, Integer> totalAwareBucketFilter = null;
    @Nullable private Integer specifiedLevel = null;
    @Nullable private PartitionPredicate partitionFilter = null;
    // Auth partition filter (ANDed with partitionFilter); kept separate so it can be reset each
    // plan without touching the base partitionFilter.
    @Nullable private PartitionPredicate authPartitionFilter = null;
    @Nullable private BiFilter<Integer, Integer> levelMinMaxFilter = null;
    @Nullable protected RowRangeIndex rowRangeIndex = null;
    @Nullable private RoaringNavigableMap64 manifestGroupCandidates;

    public ManifestsReader(
            RowType partitionType,
            String partitionDefaultValue,
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory) {
        this.partitionType = partitionType;
        this.partitionDefaultValue = partitionDefaultValue;
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
    }

    public ManifestsReader onlyReadRealBuckets() {
        this.onlyReadRealBuckets = true;
        return this;
    }

    public ManifestsReader withBucket(int bucket) {
        this.specifiedBucket = bucket;
        return this;
    }

    public ManifestsReader withBucketFilter(Filter<Integer> bucketFilter) {
        this.bucketFilter = bucketFilter;
        return this;
    }

    public ManifestsReader withTotalAwareBucketFilter(
            TriFilter<BinaryRow, Integer, Integer> totalAwareBucketFilter) {
        this.totalAwareBucketFilter = totalAwareBucketFilter;
        return this;
    }

    public ManifestsReader withLevel(int level) {
        this.specifiedLevel = level;
        return this;
    }

    public ManifestsReader withLevelMinMaxFilter(BiFilter<Integer, Integer> minMaxFilter) {
        this.levelMinMaxFilter = minMaxFilter;
        return this;
    }

    public ManifestsReader withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    public ManifestsReader withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    public ManifestsReader withPartitionsFilter(List<Map<String, String>> partitions) {
        return withPartitionFilter(
                createBinaryPartitions(partitions, partitionType, partitionDefaultValue));
    }

    public ManifestsReader withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    /** Overwrites the auth-derived partition filter; {@code null} clears it. */
    public ManifestsReader withAuthPartitionFilter(@Nullable PartitionPredicate predicate) {
        this.authPartitionFilter = predicate;
        return this;
    }

    public ManifestsReader withRowRangeIndex(RowRangeIndex rowRangeIndex) {
        this.rowRangeIndex = rowRangeIndex;
        return this;
    }

    /**
     * Selects whole groups of overlapping manifests for a row-ID candidate bitmap in an ALL scan.
     */
    public ManifestsReader withManifestGroupCandidates(@Nullable RoaringNavigableMap64 candidates) {
        this.manifestGroupCandidates = candidates;
        return this;
    }

    @Nullable
    RoaringNavigableMap64 manifestGroupCandidates() {
        return manifestGroupCandidates;
    }

    @Nullable
    public PartitionPredicate partitionFilter() {
        if (partitionFilter == null) {
            return authPartitionFilter;
        }
        if (authPartitionFilter == null) {
            return partitionFilter;
        }
        return PartitionPredicate.and(Arrays.asList(partitionFilter, authPartitionFilter));
    }

    public Result read(@Nullable Snapshot specifiedSnapshot, ScanMode scanMode) {
        List<ManifestFileMeta> manifests;
        Snapshot snapshot =
                specifiedSnapshot == null ? snapshotManager.latestSnapshot() : specifiedSnapshot;
        if (snapshot == null) {
            manifests = Collections.emptyList();
        } else {
            manifests = readManifests(snapshot, scanMode);
        }

        // Compute the effective partition filter once (it ANDs the base and auth slots) instead of
        // rebuilding it per manifest.
        PartitionPredicate effectivePartitionFilter = partitionFilter();
        BucketFilter effectiveBucketFilter =
                BucketFilter.create(
                        onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
        List<ManifestFileMeta> filtered =
                manifests.stream()
                        .filter(
                                m ->
                                        filterManifestFileMeta(
                                                m, effectivePartitionFilter, effectiveBucketFilter))
                        .collect(Collectors.toList());
        RoaringNavigableMap64 candidates = manifestGroupCandidates;
        if (scanMode == ScanMode.ALL && candidates != null) {
            RowRangeIndex retainedGroups =
                    selectManifestGroupRanges(filtered, candidates, Collections.emptyMap());
            filtered = filterManifestsByRowRanges(filtered, retainedGroups);
        }
        return new Result(snapshot, manifests, filtered);
    }

    /** Returns the selected ADD row domains, or null if group pruning cannot narrow the scan. */
    @Nullable
    static RowRangeIndex selectManifestGroupRanges(
            List<ManifestFileMeta> manifests,
            RoaringNavigableMap64 candidates,
            Map<ManifestFileMeta, List<ManifestEntry>> mixedEntries) {
        return selectManifestGroupRanges(
                manifests, candidates, mixedEntries, Collections.emptyMap());
    }

    @Nullable
    static RowRangeIndex selectManifestGroupRanges(
            List<ManifestFileMeta> manifests,
            RoaringNavigableMap64 candidates,
            Map<ManifestFileMeta, List<ManifestEntry>> mixedEntries,
            Map<ManifestFileMeta, Optional<ManifestSidecar.Selection>> addBlocks) {
        List<Range> addRanges = new ArrayList<>(manifests.size());
        for (ManifestFileMeta manifest : manifests) {
            if (manifest.numAddedFiles() == 0) {
                continue;
            }
            List<ManifestEntry> entries = mixedEntries.get(manifest);
            if (entries == null) {
                ManifestSidecar.Selection selection =
                        addBlocks.getOrDefault(manifest, Optional.empty()).orElse(null);
                List<Range> blockBounds = selection == null ? null : selection.rowIdBounds();
                if (blockBounds != null) {
                    addRanges.addAll(blockBounds);
                } else if (hasValidRowIdBounds(manifest)) {
                    addRanges.add(new Range(manifest.minRowId(), manifest.maxRowId()));
                } else {
                    return null;
                }
            } else {
                for (ManifestEntry entry : entries) {
                    if (entry.kind() == FileKind.ADD) {
                        Long first = entry.file().firstRowId();
                        long count = entry.file().rowCount();
                        if (first == null
                                || first < 0
                                || count <= 0
                                || count - 1 > Long.MAX_VALUE - first) {
                            return null;
                        }
                        addRanges.add(new Range(first, first + (count - 1)));
                    }
                }
            }
        }

        // Only ADDs can supply companion files. Exclude DELETE ranges from the connectivity
        // of groups whenever the ADD ranges are known.
        List<Range> groups = Range.sortAndMergeOverlap(addRanges);
        BitSet matches = candidates.intersectingRanges(groups);
        int matchedCount = matches.cardinality();
        if (mixedEntries.isEmpty()
                && matchedCount == groups.size()
                && addRanges.size() == manifests.size()) {
            return null;
        }

        List<Range> retained = new ArrayList<>(matchedCount);
        for (int i = matches.nextSetBit(0); i >= 0; i = matches.nextSetBit(i + 1)) {
            retained.add(groups.get(i));
        }
        return RowRangeIndex.create(retained);
    }

    static List<ManifestFileMeta> filterManifestsByRowRanges(
            List<ManifestFileMeta> manifests, @Nullable RowRangeIndex retainedGroups) {
        if (retainedGroups == null) {
            return manifests;
        }
        if (retainedGroups.ranges().isEmpty()) {
            return Collections.emptyList();
        }
        // Replay deletes for the retained ADDs. Unknown DELETE bounds may cover any group.
        return manifests.stream()
                .filter(
                        manifest ->
                                !hasValidRowIdBounds(manifest)
                                        || retainedGroups.intersects(
                                                manifest.minRowId(), manifest.maxRowId()))
                .collect(Collectors.toList());
    }

    private static boolean hasValidRowIdBounds(ManifestFileMeta manifest) {
        Long min = manifest.minRowId();
        Long max = manifest.maxRowId();
        return min != null && max != null && min >= 0 && max >= min;
    }

    private List<ManifestFileMeta> readManifests(Snapshot snapshot, ScanMode scanMode) {
        ManifestList manifestList = manifestListFactory.create();
        switch (scanMode) {
            case ALL:
                return manifestList.readDataManifests(snapshot);
            case DELTA:
                return manifestList.readDeltaManifests(snapshot);
            case CHANGELOG:
                return manifestList.readChangelogManifests(snapshot);
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
    }

    private boolean filterManifestByRowRanges(ManifestFileMeta manifest) {
        if (rowRangeIndex == null) {
            return true;
        }
        Long min = manifest.minRowId();
        Long max = manifest.maxRowId();
        if (min == null || max == null) {
            return true;
        }

        return this.rowRangeIndex.intersects(min, max);
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestFileMeta(
            ManifestFileMeta manifest,
            @Nullable PartitionPredicate effectivePartitionFilter,
            @Nullable BucketFilter effectiveBucketFilter) {
        if (effectiveBucketFilter != null && !effectiveBucketFilter.mayContain(manifest)) {
            return false;
        }

        Integer minLevel = manifest.minLevel();
        Integer maxLevel = manifest.maxLevel();
        if (minLevel != null && maxLevel != null) {
            if (specifiedLevel != null
                    && (specifiedLevel < minLevel || specifiedLevel > maxLevel)) {
                return false;
            }
            if (levelMinMaxFilter != null && !levelMinMaxFilter.test(minLevel, maxLevel)) {
                return false;
            }
        }

        if (effectivePartitionFilter != null) {
            SimpleStats stats = manifest.partitionStats();
            if (!effectivePartitionFilter.test(
                    manifest.numAddedFiles() + manifest.numDeletedFiles(),
                    stats.minValues(),
                    stats.maxValues(),
                    stats.nullCounts())) {
                return false;
            }
        }

        if (!filterManifestByRowRanges(manifest)) {
            return false;
        }

        return true;
    }

    /** Result for reading manifest files. */
    public static final class Result {

        public final Snapshot snapshot;
        public final List<ManifestFileMeta> allManifests;
        public final List<ManifestFileMeta> filteredManifests;

        public Result(
                Snapshot snapshot,
                List<ManifestFileMeta> allManifests,
                List<ManifestFileMeta> filteredManifests) {
            this.snapshot = snapshot;
            this.allManifests = allManifests;
            this.filteredManifests = filteredManifests;
        }
    }

    public static Result emptyResult() {
        return new Result(null, Collections.emptyList(), Collections.emptyList());
    }
}
