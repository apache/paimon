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

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader.Plan;

/** Helper class for the follow-up planning of {@link StreamTableScan}. */
public interface FollowUpScanner {

    boolean shouldScanSnapshot(Snapshot snapshot);

    Plan scan(Snapshot snapshot, SnapshotReader snapshotReader);

    default Plan getOverwriteChangesPlan(Snapshot snapshot, SnapshotReader snapshotReader) {
        return snapshotReader.withSnapshot(snapshot).readChanges();
    }
}
