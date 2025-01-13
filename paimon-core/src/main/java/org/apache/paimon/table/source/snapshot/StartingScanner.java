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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;

import javax.annotation.Nullable;

import java.util.List;

/** Helper class for the first planning of {@link TableScan}. */
public interface StartingScanner {

    StartingContext startingContext();

    Result scan(SnapshotReader snapshotReader);

    List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader);

    /** Scan result of {@link #scan}. */
    interface Result {}

    /** Currently, there is no snapshot, need to wait for the snapshot to be generated. */
    class NoSnapshot implements Result {}

    static ScannedResult fromPlan(SnapshotReader.Plan plan) {
        return new ScannedResult(plan);
    }

    /** Result with scanned snapshot. Next snapshot should be the current snapshot plus 1. */
    class ScannedResult implements Result {

        private final SnapshotReader.Plan plan;

        public ScannedResult(SnapshotReader.Plan plan) {
            this.plan = plan;
        }

        public long currentSnapshotId() {
            return plan.snapshotId();
        }

        @Nullable
        public Long currentWatermark() {
            return plan.watermark();
        }

        public List<DataSplit> splits() {
            return (List) plan.splits();
        }

        public SnapshotReader.Plan plan() {
            return plan;
        }
    }

    /**
     * Return the next snapshot for followup scanning. The current snapshot is not scanned (even
     * doesn't exist), so there are no splits.
     */
    class NextSnapshot implements Result {

        private final long nextSnapshotId;

        public NextSnapshot(long nextSnapshotId) {
            this.nextSnapshotId = nextSnapshotId;
        }

        public long nextSnapshotId() {
            return nextSnapshotId;
        }
    }
}
