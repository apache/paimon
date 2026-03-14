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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.manifest.FileKind.ADD;

/** The abstract class for StartingScanner. */
public abstract class AbstractStartingScanner implements StartingScanner {

    protected final SnapshotManager snapshotManager;

    protected Long startingSnapshotId = null;

    AbstractStartingScanner(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    protected ScanMode startingScanMode() {
        return ScanMode.DELTA;
    }

    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId, startingScanMode() == ScanMode.ALL);
        }
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        Result result = scan(snapshotReader);
        if (result instanceof ScannedResult) {
            return mergeDataSplitsToPartitionEntries(((ScannedResult) result).splits());
        }
        return Collections.emptyList();
    }

    private static List<PartitionEntry> mergeDataSplitsToPartitionEntries(
            Collection<Split> splits) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (Split s : splits) {
            if (!(s instanceof DataSplit)) {
                throw new UnsupportedOperationException();
            }
            DataSplit split = (DataSplit) s;
            BinaryRow partition = split.partition();
            for (DataFileMeta file : split.dataFiles()) {
                PartitionEntry partitionEntry =
                        PartitionEntry.fromDataFile(
                                partition,
                                ADD,
                                file,
                                Optional.ofNullable(split.totalBuckets()).orElse(0));
                partitions.compute(
                        partition,
                        (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
            }

            // Ignore before files, because we don't know how to merge them
            // Ignore deletion files, because it is costly to read from it
        }
        return new ArrayList<>(partitions.values());
    }
}
