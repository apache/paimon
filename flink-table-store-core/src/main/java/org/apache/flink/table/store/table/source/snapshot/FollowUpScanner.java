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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.StreamDataTableScan;

/** Helper class for the follow-up planning of {@link StreamDataTableScan}. */
public interface FollowUpScanner {

    boolean shouldScanSnapshot(Snapshot snapshot);

    default boolean shouldEndInput(Snapshot snapshot) {
        return false;
    }

    default boolean isBounded() {
        return false;
    }

    DataTableScan.DataFilePlan getPlan(long snapshotId, SnapshotSplitReader snapshotSplitReader);

    default DataTableScan.DataFilePlan getOverwriteChangesPlan(
            long snapshotId, SnapshotSplitReader snapshotSplitReader) {
        return new DataTableScan.DataFilePlan(
                snapshotId, snapshotSplitReader.withSnapshot(snapshotId).overwriteSplits());
    }
}
