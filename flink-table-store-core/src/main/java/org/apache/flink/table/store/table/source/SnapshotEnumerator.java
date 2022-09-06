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

package org.apache.flink.table.store.table.source;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.Callable;

/** Enumerator to enumerate incremental snapshots. */
public class SnapshotEnumerator implements Callable<SnapshotEnumerator.EnumeratorResult> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotEnumerator.class);

    private final SnapshotManager snapshotManager;

    private final TableScan scan;

    private long nextSnapshotId;

    public SnapshotEnumerator(Path tablePath, TableScan scan, long currentSnapshot) {
        this.snapshotManager = new SnapshotManager(tablePath);
        this.scan = scan;
        this.nextSnapshotId = currentSnapshot + 1;
    }

    @Nullable
    @Override
    public EnumeratorResult call() {
        // TODO sync with processDiscoveredSplits to avoid too more splits in memory
        while (true) {
            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                // TODO check latest snapshot id, expired?
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return null;
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);
            if (snapshot.commitKind() != Snapshot.CommitKind.APPEND) {
                if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE) {
                    LOG.warn("Ignore overwrite snapshot id {}.", nextSnapshotId);
                }

                nextSnapshotId++;
                LOG.debug(
                        "Next snapshot id {} is not APPEND, but is {}, check next one.",
                        nextSnapshotId,
                        snapshot.commitKind());
                continue;
            }

            TableScan.Plan plan = scan.withSnapshot(nextSnapshotId).plan();
            EnumeratorResult result = new EnumeratorResult(nextSnapshotId, plan);
            LOG.debug("Find snapshot id {}.", nextSnapshotId);

            nextSnapshotId++;
            return result;
        }
    }

    /** Enumerator result. */
    public static class EnumeratorResult {

        public final long snapshotId;

        public final TableScan.Plan plan;

        private EnumeratorResult(long snapshotId, TableScan.Plan plan) {
            this.snapshotId = snapshotId;
            this.plan = plan;
        }
    }
}
