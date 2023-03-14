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

import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.operation.ScanKind;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.table.source.SplitGenerator;
import org.apache.flink.table.store.utils.Filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Read splits from specified {@link Snapshot} with given configuration. */
public interface SnapshotSplitReader {

    SnapshotSplitReader withSnapshot(long snapshotId);

    SnapshotSplitReader withFilter(Predicate predicate);

    SnapshotSplitReader withKind(ScanKind scanKind);

    SnapshotSplitReader withLevelFilter(Filter<Integer> levelFilter);

    SnapshotSplitReader withBucket(int bucket);

    /** Get splits from snapshot. */
    List<DataSplit> splits();

    /** Get splits from an overwrite snapshot. */
    List<DataSplit> overwriteSplits();

    static List<DataSplit> generateSplits(
            long snapshotId,
            boolean isIncremental,
            boolean reverseRowKind,
            SplitGenerator splitGenerator,
            Map<BinaryRow, Map<Integer, List<DataFileMeta>>> groupedDataFiles) {
        List<DataSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> entry :
                groupedDataFiles.entrySet()) {
            BinaryRow partition = entry.getKey();
            Map<Integer, List<DataFileMeta>> buckets = entry.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry : buckets.entrySet()) {
                int bucket = bucketEntry.getKey();
                if (isIncremental) {
                    // Don't split when incremental
                    splits.add(
                            new DataSplit(
                                    snapshotId,
                                    partition,
                                    bucket,
                                    bucketEntry.getValue(),
                                    true,
                                    reverseRowKind));
                } else {
                    splitGenerator.split(bucketEntry.getValue()).stream()
                            .map(
                                    files ->
                                            new DataSplit(
                                                    snapshotId,
                                                    partition,
                                                    bucket,
                                                    files,
                                                    false,
                                                    reverseRowKind))
                            .forEach(splits::add);
                }
            }
        }
        return splits;
    }
}
