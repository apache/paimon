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
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    @Nullable private Integer specifiedLevel = null;
    @Nullable private PartitionPredicate partitionFilter = null;
    @Nullable private BiFilter<Integer, Integer> levelMinMaxFilter = null;
    @Nullable protected List<Range> rowRanges;

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

    public ManifestsReader withRowRanges(List<Range> rowRanges) {
        this.rowRanges = rowRanges;
        return this;
    }

    @Nullable
    public PartitionPredicate partitionFilter() {
        return partitionFilter;
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

        List<ManifestFileMeta> filtered =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        return new Result(snapshot, manifests, filtered);
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
        if (rowRanges == null) {
            return true;
        }
        Long min = manifest.minRowId();
        Long max = manifest.maxRowId();
        if (min == null || max == null) {
            return true;
        }

        Range manifestRowRange = new Range(min, max);

        for (Range expected : rowRanges) {
            if (Range.intersection(manifestRowRange, expected) != null) {
                return true;
            }
        }
        return false;
    }

    /** Note: Keep this thread-safe. */
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        Integer minBucket = manifest.minBucket();
        Integer maxBucket = manifest.maxBucket();
        if (minBucket != null && maxBucket != null) {
            if (onlyReadRealBuckets && maxBucket < 0) {
                return false;
            }
            if (specifiedBucket != null
                    && (specifiedBucket < minBucket || specifiedBucket > maxBucket)) {
                return false;
            }
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

        if (partitionFilter != null) {
            SimpleStats stats = manifest.partitionStats();
            if (!partitionFilter.test(
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
