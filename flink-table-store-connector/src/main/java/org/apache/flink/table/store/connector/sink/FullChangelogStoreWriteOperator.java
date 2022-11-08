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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.table.store.table.sink.SinkRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * A {@link StoreWriteOperator} for {@link
 * org.apache.flink.table.store.CoreOptions.ChangelogProducer#FULL_COMPACTION} changelog producer.
 */
public class FullChangelogStoreWriteOperator extends StoreWriteOperator {

    private final long checkpointThreshold;

    private transient Set<Tuple2<BinaryRowData, Integer>> currentWrittenBuckets;
    private transient NavigableMap<Long, Set<Tuple2<BinaryRowData, Integer>>> writtenBuckets;
    private transient ListState<Tuple3<Long, BinaryRowData, Integer>> writtenBucketState;
    private transient Long snapshotIdentifierToCheck;

    public FullChangelogStoreWriteOperator(
            FileStoreTable table,
            String initialCommitUser,
            @Nullable Map<String, String> overwritePartition,
            @Nullable LogSinkFunction logSinkFunction,
            long checkpointThreshold) {
        super(table, initialCommitUser, overwritePartition, logSinkFunction);
        this.checkpointThreshold = checkpointThreshold;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        TupleSerializer<Tuple3<Long, BinaryRowData, Integer>> writtenBucketStateSerializer =
                new TupleSerializer<>(
                        (Class<Tuple3<Long, BinaryRowData, Integer>>) (Class<?>) Tuple3.class,
                        new TypeSerializer[] {
                            LongSerializer.INSTANCE,
                            new BinaryRowDataSerializer(
                                    table.schema().logicalPartitionType().getFieldCount()),
                            IntSerializer.INSTANCE
                        });
        writtenBucketState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "table_store_written_buckets",
                                        writtenBucketStateSerializer));
        writtenBuckets = new TreeMap<>();
        writtenBucketState
                .get()
                .forEach(
                        t ->
                                writtenBuckets
                                        .computeIfAbsent(t.f0, k -> new HashSet<>())
                                        .add(Tuple2.of(t.f1, t.f2)));

        snapshotIdentifierToCheck = null;
    }

    @Override
    public void open() throws Exception {
        super.open();
        currentWrittenBuckets = new HashSet<>();
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        SinkRecord record = writeRecord(element);

        BinaryRowData partition = record.partition();
        int bucket = record.bucket();
        // partition is a reused BinaryRowData
        // we first check if the tuple exists to minimize copying
        if (!currentWrittenBuckets.contains(Tuple2.of(partition, bucket))) {
            currentWrittenBuckets.add(Tuple2.of(partition.copy(), bucket));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        List<Tuple3<Long, BinaryRowData, Integer>> writtenBucketList = new ArrayList<>();
        for (Map.Entry<Long, Set<Tuple2<BinaryRowData, Integer>>> entry :
                writtenBuckets.entrySet()) {
            for (Tuple2<BinaryRowData, Integer> bucket : entry.getValue()) {
                writtenBucketList.add(Tuple3.of(entry.getKey(), bucket.f0, bucket.f1));
            }
        }
        writtenBucketState.update(writtenBucketList);
    }

    @Override
    protected List<Committable> prepareCommit(boolean blocking, long checkpointId)
            throws IOException {
        if (snapshotIdentifierToCheck != null) {
            Optional<Snapshot> snapshot = findSnapshot(snapshotIdentifierToCheck);
            if (snapshot.map(s -> s.commitKind() == Snapshot.CommitKind.COMPACT).orElse(false)) {
                writtenBuckets.headMap(snapshotIdentifierToCheck, true).clear();
                snapshotIdentifierToCheck = null;
            }
        }

        if (!currentWrittenBuckets.isEmpty()) {
            writtenBuckets
                    .computeIfAbsent(checkpointId, k -> new HashSet<>())
                    .addAll(currentWrittenBuckets);
            currentWrittenBuckets.clear();
        }

        if (snapshotIdentifierToCheck == null // wait for last forced full compaction to complete
                && !writtenBuckets.isEmpty() // there should be something to compact
                && checkpointId - writtenBuckets.navigableKeySet().first() + 1
                        >= checkpointThreshold // checkpoints without full compaction exceeds
        ) {
            blocking = true;
        }

        if (blocking) {
            snapshotIdentifierToCheck = checkpointId;
            Set<Tuple2<BinaryRowData, Integer>> compactedBuckets = new HashSet<>();
            for (Set<Tuple2<BinaryRowData, Integer>> buckets : writtenBuckets.values()) {
                for (Tuple2<BinaryRowData, Integer> bucket : buckets) {
                    if (compactedBuckets.contains(bucket)) {
                        continue;
                    }
                    compactedBuckets.add(bucket);
                    try {
                        write.compact(bucket.f0, bucket.f1);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        return super.prepareCommit(blocking, checkpointId);
    }

    private Optional<Snapshot> findSnapshot(long identifierToCheck) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        if (latestId == null) {
            return Optional.empty();
        }

        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return Optional.empty();
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (!snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitIdentifier() == identifierToCheck) {
                return Optional.of(snapshot);
            } else if (snapshot.commitIdentifier() < identifierToCheck) {
                return Optional.empty();
            }
        }

        throw new RuntimeException(
                String.format(
                        "All snapshot from user %s has identifier larger than %d. "
                                + "This is rare but it might be that snapshots are expiring too fast.\n"
                                + "If this exception happens continuously, please consider increasing "
                                + CoreOptions.SNAPSHOT_TIME_RETAINED.key()
                                + " or "
                                + CoreOptions.SNAPSHOT_NUM_RETAINED_MIN,
                        commitUser,
                        identifierToCheck));
    }
}
