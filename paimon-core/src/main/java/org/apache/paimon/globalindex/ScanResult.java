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

package org.apache.paimon.globalindex;

import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.utils.RowRangeIndex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Result of scanning data files for a global index build. */
public final class ScanResult<T> {

    private final long scanSnapshotId;
    private final RowRangeIndex rowRangeIndex;
    private final List<T> entries;
    private final List<IndexManifestEntry> deletedIndexEntries;

    public ScanResult(
            long scanSnapshotId,
            RowRangeIndex rowRangeIndex,
            List<T> entries,
            List<IndexManifestEntry> deletedIndexEntries) {
        checkArgument(scanSnapshotId > 0, "Scan snapshot id must be positive.");
        this.scanSnapshotId = scanSnapshotId;
        this.rowRangeIndex = rowRangeIndex;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.deletedIndexEntries =
                Collections.unmodifiableList(new ArrayList<>(deletedIndexEntries));
    }

    public long scanSnapshotId() {
        return scanSnapshotId;
    }

    public RowRangeIndex rowRangeIndex() {
        return rowRangeIndex;
    }

    public List<T> entries() {
        return entries;
    }

    public List<IndexManifestEntry> deletedIndexEntries() {
        return deletedIndexEntries;
    }

    public ScanResult<T> withEntries(List<T> entries) {
        return new ScanResult<>(scanSnapshotId, rowRangeIndex, entries, deletedIndexEntries);
    }
}
