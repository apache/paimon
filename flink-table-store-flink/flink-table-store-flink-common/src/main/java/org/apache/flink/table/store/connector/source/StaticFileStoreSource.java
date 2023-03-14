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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.source.BatchDataTableScan;
import org.apache.flink.table.store.table.source.DataTableScan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

/** Bounded {@link FlinkSource} for reading records. It does not monitor new snapshots. */
public class StaticFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 3L;

    private final DataTable table;
    private final BatchDataTableScan.Factory scanFactory;
    private final Predicate predicate;

    public StaticFileStoreSource(
            DataTable table,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate,
            @Nullable Long limit) {
        this(table, projectedFields, predicate, limit, DataTable::newScan);
    }

    public StaticFileStoreSource(
            DataTable table,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate,
            @Nullable Long limit,
            BatchDataTableScan.Factory scanFactory) {
        super(table.newReadBuilder().withFilter(predicate).withProjection(projectedFields), limit);
        this.table = table;
        this.scanFactory = scanFactory;
        this.predicate = predicate;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        SnapshotManager snapshotManager = table.snapshotManager();

        Long snapshotId = null;
        Collection<FileStoreSourceSplit> splits;
        if (checkpoint == null) {
            splits = new ArrayList<>();
            FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();

            // read all splits from scan
            DataTableScan.DataFilePlan plan =
                    scanFactory.create(table).withFilter(predicate).plan();
            if (plan != null) {
                snapshotId = plan.snapshotId;
                splits.addAll(splitGenerator.createSplits(plan));
            }
        } else {
            // restore from checkpoint
            snapshotId = checkpoint.currentSnapshotId();
            splits = checkpoint.splits();
        }

        Snapshot snapshot = snapshotId == null ? null : snapshotManager.snapshot(snapshotId);
        return new StaticFileStoreSplitEnumerator(context, snapshot, splits);
    }
}
