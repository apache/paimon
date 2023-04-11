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

import java.util.Collections;
import java.util.List;

/** Helper class for the first planning of {@link TableScan}. */
public interface StartingScanner {

    Result scan(SnapshotManager snapshotManager, SnapshotSplitReader snapshotSplitReader);

    /** Scan result of {@link #scan}. */
    interface Result {
        long snapshotId();

        List<DataSplit> splits();
    }

    /** A null Result. */
    class NullResult implements Result {
        @Override
        public long snapshotId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DataSplit> splits() {
            throw new UnsupportedOperationException();
        }
    }

    /** A non null Result. */
    abstract class NonNullResult implements Result {
        private final long snapshotId;
        private final List<DataSplit> splits;

        public NonNullResult(long snapshotId) {
            this(snapshotId, Collections.emptyList());
        }

        public NonNullResult(long snapshotId, List<DataSplit> splits) {
            this.snapshotId = snapshotId;
            this.splits = splits;
        }

        @Override
        public long snapshotId() {
            return snapshotId;
        }

        @Override
        public List<DataSplit> splits() {
            return splits;
        }
    }

    /** Contains an existing snapshot. */
    class ExistingSnapshotResult extends NonNullResult {
        public ExistingSnapshotResult(long snapshotId) {
            super(snapshotId);
        }

        public ExistingSnapshotResult(long snapshotId, List<DataSplit> splits) {
            super(snapshotId, splits);
        }
    }

    /** Contains a non-existing snapshot. */
    class NonExistingSnapshotResult extends NonNullResult {
        public NonExistingSnapshotResult(long snapshotId) {
            super(snapshotId);
        }
    }
}
