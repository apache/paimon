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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.BinaryRowSerializer;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * {@link StoreSinkWrite} for {@link CoreOptions.ChangelogProducer#FULL_COMPACTION} changelog
 * producer. This writer will perform full compaction once in a while.
 */
public class FullChangelogStoreSinkWrite extends StoreSinkWriteImpl {

    private static final Logger LOG = LoggerFactory.getLogger(FullChangelogStoreSinkWrite.class);

    private final long fullCompactionThresholdMs;

    private final Set<Tuple2<BinaryRow, Integer>> currentWrittenBuckets;
    private final NavigableMap<Long, Set<Tuple2<BinaryRow, Integer>>> writtenBuckets;
    private final ListState<Tuple3<Long, BinaryRow, Integer>> writtenBucketState;

    private Long currentFirstWriteMs;
    private final NavigableMap<Long, Long> firstWriteMs;
    private final ListState<Tuple2<Long, Long>> firstWriteMsState;

    private transient TreeSet<Long> commitIdentifiersToCheck;

    public FullChangelogStoreSinkWrite(
            FileStoreTable table,
            StateInitializationContext context,
            String initialCommitUser,
            IOManager ioManager,
            boolean isOverwrite,
            long fullCompactionThresholdMs)
            throws Exception {
        super(table, context, initialCommitUser, ioManager, isOverwrite);

        this.fullCompactionThresholdMs = fullCompactionThresholdMs;

        currentWrittenBuckets = new HashSet<>();
        TupleSerializer<Tuple3<Long, BinaryRow, Integer>> writtenBucketStateSerializer =
                new TupleSerializer<>(
                        (Class<Tuple3<Long, BinaryRow, Integer>>) (Class<?>) Tuple3.class,
                        new TypeSerializer[] {
                            LongSerializer.INSTANCE,
                            new BinaryRowSerializer(
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

        currentFirstWriteMs = null;
        TupleSerializer<Tuple2<Long, Long>> firstWriteMsStateSerializer =
                new TupleSerializer<>(
                        (Class<Tuple2<Long, Long>>) (Class<?>) Tuple2.class,
                        new TypeSerializer[] {LongSerializer.INSTANCE, LongSerializer.INSTANCE});
        firstWriteMsState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "first_write_ms", firstWriteMsStateSerializer));
        firstWriteMs = new TreeMap<>();
        firstWriteMsState.get().forEach(t -> firstWriteMs.put(t.f0, t.f1));

        commitIdentifiersToCheck = new TreeSet<>();
    }

    @Override
    public SinkRecord write(InternalRow rowData) throws Exception {
        SinkRecord sinkRecord = super.write(rowData);
        touchBucket(sinkRecord.partition(), sinkRecord.bucket());
        return sinkRecord;
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        super.compact(partition, bucket, fullCompaction);
        touchBucket(partition, bucket);
    }

    private void touchBucket(BinaryRow partition, int bucket) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("touch partition {}, bucket {}", partition, bucket);
        }

        // partition is a reused BinaryRow
        // we first check if the tuple exists to minimize copying
        if (!currentWrittenBuckets.contains(Tuple2.of(partition, bucket))) {
            currentWrittenBuckets.add(Tuple2.of(partition.copy(), bucket));
        }

        if (currentFirstWriteMs == null) {
            currentFirstWriteMs = System.currentTimeMillis();
        }
    }

    @Override
    public List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        checkSuccessfulFullCompaction();

        // check what buckets we've modified during this checkpoint interval
        if (!currentWrittenBuckets.isEmpty()) {
            writtenBuckets
                    .computeIfAbsent(checkpointId, k -> new HashSet<>())
                    .addAll(currentWrittenBuckets);
            currentWrittenBuckets.clear();
            firstWriteMs.putIfAbsent(checkpointId, currentFirstWriteMs);
            currentFirstWriteMs = null;
        }

        if (LOG.isDebugEnabled()) {
            for (Map.Entry<Long, Set<Tuple2<BinaryRow, Integer>>> checkpointIdAndBuckets :
                    writtenBuckets.entrySet()) {
                LOG.debug(
                        "Written buckets for checkpoint #{} are:", checkpointIdAndBuckets.getKey());
                for (Tuple2<BinaryRow, Integer> bucket : checkpointIdAndBuckets.getValue()) {
                    LOG.debug("  * partition {}, bucket {}", bucket.f0, bucket.f1);
                }
            }
        }

        if (!writtenBuckets.isEmpty() // there should be something to compact
                && System.currentTimeMillis() - firstWriteMs.firstEntry().getValue()
                        >= fullCompactionThresholdMs // time without full compaction exceeds
        ) {
            doCompaction = true;
        }

        if (doCompaction) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Submit full compaction for checkpoint #{}", checkpointId);
            }
            submitFullCompaction();
            commitIdentifiersToCheck.add(checkpointId);
        }

        return super.prepareCommit(doCompaction, checkpointId);
    }

    private void checkSuccessfulFullCompaction() {
        SnapshotManager snapshotManager = table.snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        if (latestId == null) {
            return;
        }
        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)
                    && snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                long commitIdentifier = snapshot.commitIdentifier();
                if (commitIdentifiersToCheck.contains(commitIdentifier)) {
                    // We found a full compaction snapshot triggered by `submitFullCompaction`
                    // method.
                    //
                    // Because `submitFullCompaction` will compact all buckets in `writtenBuckets`,
                    // thus a successful commit indicates that all previous buckets have been
                    // compacted.
                    //
                    // We must make sure that the compact snapshot is triggered by
                    // `submitFullCompaction`, because normal compaction may also trigger full
                    // compaction, but that only compacts a specific bucket, not all buckets
                    // recorded in `writtenBuckets`.
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Found full compaction snapshot #{} with identifier {}",
                                id,
                                commitIdentifier);
                    }
                    writtenBuckets.headMap(commitIdentifier, true).clear();
                    firstWriteMs.headMap(commitIdentifier, true).clear();
                    commitIdentifiersToCheck.headSet(commitIdentifier).clear();
                    break;
                }
            }
        }
    }

    private void submitFullCompaction() {
        Set<Tuple2<BinaryRow, Integer>> compactedBuckets = new HashSet<>();
        writtenBuckets.forEach(
                (checkpointId, buckets) -> {
                    for (Tuple2<BinaryRow, Integer> bucket : buckets) {
                        if (compactedBuckets.contains(bucket)) {
                            continue;
                        }
                        compactedBuckets.add(bucket);
                        try {
                            write.compact(bucket.f0, bucket.f1, true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        List<Tuple3<Long, BinaryRow, Integer>> writtenBucketList = new ArrayList<>();
        for (Map.Entry<Long, Set<Tuple2<BinaryRow, Integer>>> entry : writtenBuckets.entrySet()) {
            for (Tuple2<BinaryRow, Integer> bucket : entry.getValue()) {
                writtenBucketList.add(Tuple3.of(entry.getKey(), bucket.f0, bucket.f1));
            }
        }
        writtenBucketState.update(writtenBucketList);

        List<Tuple2<Long, Long>> firstWriteMsList = new ArrayList<>();
        for (Map.Entry<Long, Long> entry : firstWriteMs.entrySet()) {
            firstWriteMsList.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
        firstWriteMsState.update(firstWriteMsList);
    }
}
