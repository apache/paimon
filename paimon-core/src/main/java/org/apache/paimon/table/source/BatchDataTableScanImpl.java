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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.SnapshotManager;

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
    public BatchDataTableScan withSnapshot(long snapshotId) {
        snapshotSplitReader.withSnapshot(snapshotId);
        return this;
    }

    @Override
    public BatchDataTableScan withFilter(Predicate predicate) {
        snapshotSplitReader.withFilter(predicate);
        return this;
    }

    @Override
    public BatchDataTableScan withKind(ScanKind scanKind) {
        snapshotSplitReader.withKind(scanKind);
        return this;
    }

    @Override
    public BatchDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotSplitReader.withLevelFilter(levelFilter);
        return this;
    }

    @Override
    public BatchDataTableScan withStartingScanner(StartingScanner startingScanner) {
        this.startingScanner = startingScanner;
        return this;
    }

    @Override
    public DataFilePlan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            StartingScanner.Result result =
                    startingScanner.scan(snapshotManager, snapshotSplitReader);
            return DataFilePlan.fromResult(result);
        } else {
            throw new EndOfScanException();
        }
    }
}
