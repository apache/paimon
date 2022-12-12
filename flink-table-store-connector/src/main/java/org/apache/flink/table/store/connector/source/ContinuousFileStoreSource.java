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
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.table.DataTable;
import org.apache.flink.table.store.table.source.DataTableScan;
import org.apache.flink.table.store.table.source.snapshot.ContinuousDataFileSnapshotEnumerator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

/** Unbounded {@link FlinkSource} for reading records. It continuously monitors new snapshots. */
public class ContinuousFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 3L;

    private final DataTable table;
    private final ContinuousDataFileSnapshotEnumerator.Factory enumeratorFactory;

    public ContinuousFileStoreSource(
            DataTable table,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate,
            @Nullable Long limit) {
        this(
                table,
                projectedFields,
                predicate,
                limit,
                ContinuousDataFileSnapshotEnumerator::create);
    }

    public ContinuousFileStoreSource(
            DataTable table,
            @Nullable int[][] projectedFields,
            @Nullable Predicate predicate,
            @Nullable Long limit,
            ContinuousDataFileSnapshotEnumerator.Factory enumeratorFactory) {
        super(table, projectedFields, predicate, limit);
        this.table = table;
        this.enumeratorFactory = enumeratorFactory;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        DataTableScan scan = table.newScan();
        if (predicate != null) {
            scan.withFilter(predicate);
        }

        Long nextSnapshotId = null;
        Collection<FileStoreSourceSplit> splits = new ArrayList<>();
        if (checkpoint != null) {
            nextSnapshotId = checkpoint.currentSnapshotId();
            splits = checkpoint.splits();
        }

        return new ContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                table.options().continuousDiscoveryInterval().toMillis(),
                enumeratorFactory.create(table, scan, nextSnapshotId));
    }
}
