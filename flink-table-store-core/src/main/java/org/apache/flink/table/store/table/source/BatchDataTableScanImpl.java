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

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.source.snapshot.SnapshotSplitReader;
import org.apache.flink.table.store.table.source.snapshot.StartingScanner;

import javax.annotation.Nullable;

/** {@link DataTableScan} for batch planning. */
public class BatchDataTableScanImpl extends AbstractDataTableScan implements BatchDataTableScan {

    private final SnapshotManager snapshotManager;

    private StartingScanner startingScanner;

    private boolean hasNext;

    public BatchDataTableScanImpl(
            CoreOptions options,
            SnapshotSplitReader snapshotSplitReader,
            SnapshotManager snapshotManager) {
        super(options, snapshotSplitReader);
        this.snapshotManager = snapshotManager;
        this.hasNext = true;
    }

    @Override
    public BatchDataTableScan withStartingScanner(StartingScanner startingScanner) {
        this.startingScanner = startingScanner;
        return this;
    }

    @Override
    @Nullable
    public DataFilePlan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            return startingScanner.getPlan(snapshotManager, snapshotSplitReader);
        } else {
            throw new EndOfScanException();
        }
    }
}
