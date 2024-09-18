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
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A util class to plan manifest files. */
@ThreadSafe
public class ManifestPlanner {

    private final RowType partitionType;
    private final SnapshotManager snapshotManager;
    private final ManifestList.Factory manifestListFactory;

    @Nullable private PartitionPredicate partitionFilter = null;

    public ManifestPlanner(
            RowType partitionType,
            SnapshotManager snapshotManager,
            ManifestList.Factory manifestListFactory) {
        this.partitionType = partitionType;
        this.snapshotManager = snapshotManager;
        this.manifestListFactory = manifestListFactory;
    }

    public ManifestPlanner withPartitionFilter(Predicate predicate) {
        this.partitionFilter = PartitionPredicate.fromPredicate(partitionType, predicate);
        return this;
    }

    public ManifestPlanner withPartitionFilter(List<BinaryRow> partitions) {
        this.partitionFilter = PartitionPredicate.fromMultiple(partitionType, partitions);
        return this;
    }

    public ManifestPlanner withPartitionFilter(PartitionPredicate predicate) {
        this.partitionFilter = predicate;
        return this;
    }

    @Nullable
    public PartitionPredicate partitionFilter() {
        return partitionFilter;
    }

    public Pair<Snapshot, List<ManifestFileMeta>> plan(
            @Nullable Snapshot specifiedSnapshot, ScanMode scanMode) {
        List<ManifestFileMeta> manifests;
        Snapshot snapshot =
                specifiedSnapshot == null ? snapshotManager.latestSnapshot() : specifiedSnapshot;
        if (snapshot == null) {
            manifests = Collections.emptyList();
        } else {
            manifests = readManifests(snapshot, scanMode);
        }

        manifests =
                manifests.stream()
                        .filter(this::filterManifestFileMeta)
                        .collect(Collectors.toList());
        return Pair.of(snapshot, manifests);
    }

    private List<ManifestFileMeta> readManifests(Snapshot snapshot, ScanMode scanMode) {
        ManifestList manifestList = manifestListFactory.create();
        switch (scanMode) {
            case ALL:
                return manifestList.readDataManifests(snapshot);
            case DELTA:
                return manifestList.readDeltaManifests(snapshot);
            case CHANGELOG:
                if (snapshot.version() <= Snapshot.TABLE_STORE_02_VERSION) {
                    throw new UnsupportedOperationException(
                            "Unsupported snapshot version: " + snapshot.version());
                }
                return manifestList.readChangelogManifests(snapshot);
            default:
                throw new UnsupportedOperationException("Unknown scan kind " + scanMode.name());
        }
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
}
