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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.SnapshotManager;

/** {@link TableScan} implementation for batch planning. */
public class InnerTableScanImpl extends AbstractInnerTableScan {

    private final SnapshotManager snapshotManager;

    private StartingScanner startingScanner;

    private boolean hasNext;

    public InnerTableScanImpl(
            CoreOptions options, SnapshotReader snapshotReader, SnapshotManager snapshotManager) {
        super(options, snapshotReader);
        this.snapshotManager = snapshotManager;
        this.hasNext = true;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        snapshotReader.withFilter(predicate);
        return this;
    }

    @Override
    public DataFilePlan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            StartingScanner.Result result = startingScanner.scan(snapshotManager, snapshotReader);
            return DataFilePlan.fromResult(result);
        } else {
            throw new EndOfScanException();
        }
    }
}
