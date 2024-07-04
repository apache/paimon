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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;

/** Statistics for a scan operation. */
public class ScanStats {
    // the unit is milliseconds
    private final long duration;
    private final long scannedManifests;
    private final long skippedByPartitionAndStats;
    private final long skippedByBucketAndLevelFilter;

    private final long skippedByWholeBucketFiles;
    private final long skippedTableFiles;
    private final long resultedTableFiles;

    public ScanStats(
            long duration,
            long scannedManifests,
            long skippedByPartitionAndStats,
            long skippedByBucketAndLevelFilter,
            long skippedByWholeBucketFiles,
            long resultedTableFiles) {
        this.duration = duration;
        this.scannedManifests = scannedManifests;
        this.skippedByPartitionAndStats = skippedByPartitionAndStats;
        this.skippedByBucketAndLevelFilter = skippedByBucketAndLevelFilter;
        this.skippedByWholeBucketFiles = skippedByWholeBucketFiles;
        this.skippedTableFiles =
                skippedByPartitionAndStats
                        + skippedByBucketAndLevelFilter
                        + skippedByWholeBucketFiles;
        this.resultedTableFiles = resultedTableFiles;
    }

    @VisibleForTesting
    protected long getScannedManifests() {
        return scannedManifests;
    }

    @VisibleForTesting
    protected long getSkippedTableFiles() {
        return skippedTableFiles;
    }

    @VisibleForTesting
    protected long getResultedTableFiles() {
        return resultedTableFiles;
    }

    @VisibleForTesting
    protected long getSkippedByPartitionAndStats() {
        return skippedByPartitionAndStats;
    }

    @VisibleForTesting
    protected long getSkippedByBucketAndLevelFilter() {
        return skippedByBucketAndLevelFilter;
    }

    @VisibleForTesting
    protected long getSkippedByWholeBucketFiles() {
        return skippedByWholeBucketFiles;
    }

    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }
}
