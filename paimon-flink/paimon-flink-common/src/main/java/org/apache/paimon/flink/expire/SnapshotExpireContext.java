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

package org.apache.paimon.flink.expire;

import org.apache.paimon.Snapshot;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Context for snapshot expiration. Holds runtime dependencies and shared state used by both {@link
 * SnapshotExpireTask} subclasses and {@link SnapshotExpireSink}.
 */
public class SnapshotExpireContext {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExpireContext.class);

    private final SnapshotManager snapshotManager;
    private final SnapshotDeletion snapshotDeletion;
    @Nullable private final ChangelogManager changelogManager;

    /** Pre-collected snapshot cache from planner to avoid redundant reads. */
    private final Map<Long, Snapshot> snapshotCache;

    @Nullable private final List<Snapshot> taggedSnapshots;
    @Nullable private Set<String> skippingSet;

    public SnapshotExpireContext(
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            @Nullable ChangelogManager changelogManager,
            @Nullable List<Snapshot> taggedSnapshots,
            Map<Long, Snapshot> snapshotCache) {
        this.snapshotManager = snapshotManager;
        this.snapshotDeletion = snapshotDeletion;
        this.changelogManager = changelogManager;
        this.taggedSnapshots = taggedSnapshots;
        this.snapshotCache = snapshotCache;
    }

    /**
     * Load a snapshot by ID.
     *
     * @return the snapshot, or null if not found
     */
    @Nullable
    public Snapshot loadSnapshot(long snapshotId) {
        Snapshot snapshot = snapshotCache.get(snapshotId);
        if (snapshot != null) {
            return snapshot;
        }
        try {
            return snapshotManager.tryGetSnapshot(snapshotId);
        } catch (FileNotFoundException e) {
            LOG.warn("Snapshot {} not found, skipping task", snapshotId);
            return null;
        }
    }

    public SnapshotManager snapshotManager() {
        return snapshotManager;
    }

    public SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }

    @Nullable
    public ChangelogManager changelogManager() {
        return changelogManager;
    }

    @Nullable
    public List<Snapshot> taggedSnapshots() {
        return taggedSnapshots;
    }

    @Nullable
    public Set<String> skippingSet() {
        return skippingSet;
    }

    public void setSkippingSet(Set<String> skippingSet) {
        this.skippingSet = skippingSet;
    }
}
