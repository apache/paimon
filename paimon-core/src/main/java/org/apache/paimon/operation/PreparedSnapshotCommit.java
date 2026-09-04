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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.sink.CommitCallback;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A snapshot which has been fully prepared but has not necessarily been made visible yet.
 *
 * <p>The manifest files referenced by this object must not be deleted while the publish result is
 * unknown. Failed catalog transactions leave prepared metadata for orphan-file cleanup instead of
 * deleting it in the commit path.
 */
public final class PreparedSnapshotCommit {

    enum State {
        PREPARED,
        UNKNOWN,
        PUBLISHED,
        FINALIZED
    }

    @Nullable final Snapshot baseSnapshot;
    final Snapshot snapshot;
    final String branch;
    final List<PartitionStatistics> statistics;
    final CommitCallback.Context callbackContext;
    final List<SimpleFileEntry> baseDataFiles;

    final List<ManifestFileMeta> mergeBeforeManifests;
    final List<ManifestFileMeta> mergeAfterManifests;
    final boolean skipManifestMergeOnRetry;
    final long startedMillis;

    State state;

    PreparedSnapshotCommit(
            @Nullable Snapshot baseSnapshot,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics,
            List<SimpleFileEntry> baseDataFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests,
            boolean skipManifestMergeOnRetry,
            long startedMillis) {
        this.baseSnapshot = baseSnapshot;
        this.snapshot = snapshot;
        this.branch = branch;
        this.statistics = statistics;
        this.callbackContext =
                new CommitCallback.Context(
                        baseDataFiles, deltaFiles, indexFiles, snapshot, identifier);
        this.baseDataFiles = baseDataFiles;
        this.mergeBeforeManifests = mergeBeforeManifests;
        this.mergeAfterManifests = mergeAfterManifests;
        this.skipManifestMergeOnRetry = skipManifestMergeOnRetry;
        this.startedMillis = startedMillis;
        this.state = State.PREPARED;
    }

    @Nullable
    public String baseSnapshotUuid() {
        return baseSnapshot == null ? null : baseSnapshot.uuid();
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public String branch() {
        return branch;
    }

    public List<PartitionStatistics> statistics() {
        return statistics;
    }
}
