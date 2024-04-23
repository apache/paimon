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
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PlanImpl;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link StartingScanner} for incremental changes by snapshot. */
public class IncrementalStartingScanner extends AbstractStartingScanner {

    private long endingSnapshotId;

    private ScanMode scanMode;

    public IncrementalStartingScanner(
            SnapshotManager snapshotManager, long start, long end, ScanMode scanMode) {
        super(snapshotManager);
        this.startingSnapshotId = start;
        this.endingSnapshotId = end;
        this.scanMode = scanMode;
    }

    @Override
    public Result scan(SnapshotReader reader) {
        Map<Pair<BinaryRow, Integer>, List<DataFileMeta>> grouped = new HashMap<>();
        for (long i = startingSnapshotId + 1; i < endingSnapshotId + 1; i++) {
            List<DataSplit> splits = readSplits(reader, snapshotManager.snapshot(i));
            for (DataSplit split : splits) {
                grouped.computeIfAbsent(
                                Pair.of(split.partition(), split.bucket()), k -> new ArrayList<>())
                        .addAll(split.dataFiles());
            }
        }

        List<DataSplit> result = new ArrayList<>();
        for (Map.Entry<Pair<BinaryRow, Integer>, List<DataFileMeta>> entry : grouped.entrySet()) {
            BinaryRow partition = entry.getKey().getLeft();
            int bucket = entry.getKey().getRight();
            for (SplitGenerator.SplitGroup splitGroup :
                    reader.splitGenerator().splitForBatch(entry.getValue())) {
                // TODO pass deletion files
                result.add(
                        DataSplit.builder()
                                .withSnapshot(endingSnapshotId)
                                .withPartition(partition)
                                .withBucket(bucket)
                                .withDataFiles(splitGroup.files)
                                .build());
            }
        }

        return StartingScanner.fromPlan(new PlanImpl(null, endingSnapshotId, (List) result));
    }

    private List<DataSplit> readSplits(SnapshotReader reader, Snapshot s) {
        switch (scanMode) {
            case CHANGELOG:
                return readChangeLogSplits(reader, s);
            case DELTA:
                return readDeltaSplits(reader, s);
            default:
                throw new UnsupportedOperationException("Unsupported scan kind: " + scanMode);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<DataSplit> readDeltaSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() != CommitKind.APPEND) {
            // ignore COMPACT and OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.DELTA).read().splits();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<DataSplit> readChangeLogSplits(SnapshotReader reader, Snapshot s) {
        if (s.commitKind() == CommitKind.OVERWRITE) {
            // ignore OVERWRITE
            return Collections.emptyList();
        }
        return (List) reader.withSnapshot(s).withMode(ScanMode.CHANGELOG).read().splits();
    }
}
