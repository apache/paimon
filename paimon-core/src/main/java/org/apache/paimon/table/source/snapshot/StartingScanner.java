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

import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** Helper class for the first planning of {@link TableScan}. */
public interface StartingScanner {

    @Nullable
    Result scan(SnapshotManager snapshotManager, SnapshotSplitReader snapshotSplitReader);

    /** Scan result of {@link #scan}. */
    class Result {

        private final long snapshotId;
        private final List<DataSplit> splits;

        public Result(long snapshotId) {
            this(snapshotId, Collections.emptyList());
        }

        public Result(long snapshotId, List<DataSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = splits;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public List<DataSplit> splits() {
            return splits;
        }
    }
}
