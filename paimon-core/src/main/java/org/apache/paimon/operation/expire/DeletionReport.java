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

package org.apache.paimon.operation.expire;

import org.apache.paimon.data.BinaryRow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Report of a single snapshot expiration task. */
public class DeletionReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long snapshotId;

    /** Whether this task was skipped (e.g., snapshot already deleted). */
    private boolean skipped;

    /** Whether data files were deleted. */
    private boolean dataFilesDeleted;

    /** Whether changelog data files were deleted. */
    private boolean changelogDeleted;

    /** Whether manifest files were deleted. */
    private boolean manifestsDeleted;

    /** Whether snapshot metadata file was deleted. */
    private boolean snapshotDeleted;

    /** Buckets that had files deleted (for empty directory cleanup in parallel phase). */
    private Map<BinaryRow, Set<Integer>> deletionBuckets;

    public DeletionReport(long snapshotId) {
        this.snapshotId = snapshotId;
        this.skipped = false;
        this.dataFilesDeleted = false;
        this.changelogDeleted = false;
        this.manifestsDeleted = false;
        this.snapshotDeleted = false;
        this.deletionBuckets = new HashMap<>();
    }

    /**
     * Create a skipped report for a snapshot that was already deleted.
     *
     * @param snapshotId the snapshot ID
     * @return a skipped deletion report
     */
    public static DeletionReport skipped(long snapshotId) {
        DeletionReport report = new DeletionReport(snapshotId);
        report.skipped = true;
        return report;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public boolean isSkipped() {
        return skipped;
    }

    public void setDataFilesDeleted(boolean dataFilesDeleted) {
        this.dataFilesDeleted = dataFilesDeleted;
    }

    public void setChangelogDeleted(boolean changelogDeleted) {
        this.changelogDeleted = changelogDeleted;
    }

    public void setManifestsDeleted(boolean manifestsDeleted) {
        this.manifestsDeleted = manifestsDeleted;
    }

    public void setSnapshotDeleted(boolean snapshotDeleted) {
        this.snapshotDeleted = snapshotDeleted;
    }

    public void setDeletionBuckets(Map<BinaryRow, Set<Integer>> deletionBuckets) {
        this.deletionBuckets = deletionBuckets;
    }

    public Map<BinaryRow, Set<Integer>> deletionBuckets() {
        return deletionBuckets;
    }

    @Override
    public String toString() {
        return "DeletionReport{"
                + "snapshotId="
                + snapshotId
                + ", skipped="
                + skipped
                + ", dataFilesDeleted="
                + dataFilesDeleted
                + ", changelogDeleted="
                + changelogDeleted
                + ", manifestsDeleted="
                + manifestsDeleted
                + ", snapshotDeleted="
                + snapshotDeleted
                + ", deletionBucketsCount="
                + deletionBuckets.size()
                + '}';
    }
}
