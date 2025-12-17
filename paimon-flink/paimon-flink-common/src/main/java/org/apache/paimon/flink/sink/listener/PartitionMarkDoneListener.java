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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions.PartitionMarkDoneActionMode;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_MARK_DONE_MODE;

/** Mark partition done. */
public class PartitionMarkDoneListener implements CommitListener {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMarkDoneListener.class);

    private final InternalRowPartitionComputer partitionComputer;
    private final PartitionMarkDoneTrigger trigger;
    private final List<PartitionMarkDoneAction> actions;
    private final boolean waitCompaction;
    private final PartitionMarkDoneActionMode partitionMarkDoneActionMode;

    public static Optional<PartitionMarkDoneListener> create(
            ClassLoader cl,
            boolean isStreaming,
            boolean isRestored,
            OperatorStateStore stateStore,
            FileStoreTable table)
            throws Exception {
        CoreOptions coreOptions = table.coreOptions();
        Options options = coreOptions.toConfiguration();

        if (disablePartitionMarkDone(isStreaming, table, options)) {
            return Optional.empty();
        }

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());

        PartitionMarkDoneTrigger trigger =
                PartitionMarkDoneTrigger.create(coreOptions, isRestored, stateStore);

        List<PartitionMarkDoneAction> actions =
                PartitionMarkDoneAction.createActions(cl, table, coreOptions);

        boolean waitCompaction = false;
        if (!table.primaryKeys().isEmpty()) {
            // some situation should wait compaction to mark done, otherwise, some data may not be
            // readable, and there might be data delays
            if (coreOptions.deletionVectorsEnabled()) {
                waitCompaction = true;
            } else if (coreOptions.mergeEngine() == MergeEngine.FIRST_ROW) {
                waitCompaction = true;
            } else if (table.bucketMode() == BucketMode.POSTPONE_MODE) {
                waitCompaction = true;
            }
        }

        return Optional.of(
                new PartitionMarkDoneListener(
                        partitionComputer,
                        trigger,
                        actions,
                        waitCompaction,
                        options.get(PARTITION_MARK_DONE_MODE)));
    }

    private static boolean disablePartitionMarkDone(
            boolean isStreaming, FileStoreTable table, Options options) {
        boolean partitionMarkDoneWhenEndInput = options.get(PARTITION_MARK_DONE_WHEN_END_INPUT);
        if (!isStreaming && !partitionMarkDoneWhenEndInput) {
            return true;
        }

        Duration idleToDone = options.get(PARTITION_IDLE_TIME_TO_DONE);
        if (isStreaming && idleToDone == null) {
            return true;
        }

        return table.partitionKeys().isEmpty();
    }

    public PartitionMarkDoneListener(
            InternalRowPartitionComputer partitionComputer,
            PartitionMarkDoneTrigger trigger,
            List<PartitionMarkDoneAction> actions,
            boolean waitCompaction,
            PartitionMarkDoneActionMode partitionMarkDoneActionMode) {
        this.partitionComputer = partitionComputer;
        this.trigger = trigger;
        this.actions = actions;
        this.waitCompaction = waitCompaction;
        this.partitionMarkDoneActionMode = partitionMarkDoneActionMode;
    }

    @Override
    public void notifyCommittable(List<ManifestCommittable> committables) {
        if (partitionMarkDoneActionMode == PartitionMarkDoneActionMode.WATERMARK) {
            markDoneByWatermark(committables);
        } else {
            markDoneByProcessTime(committables);
        }
    }

    private void markDoneByProcessTime(List<ManifestCommittable> committables) {
        Set<BinaryRow> partitions = new HashSet<>();
        boolean endInput = false;
        for (ManifestCommittable committable : committables) {
            for (CommitMessage commitMessage : committable.fileCommittables()) {
                CommitMessageImpl message = (CommitMessageImpl) commitMessage;
                if (waitCompaction || !message.newFilesIncrement().isEmpty()) {
                    partitions.add(message.partition());
                }
            }
            if (committable.identifier() == Long.MAX_VALUE) {
                endInput = true;
            }
        }

        partitions.stream()
                .map(partitionComputer::generatePartValues)
                .map(PartitionPathUtils::generatePartitionPath)
                .forEach(trigger::notifyPartition);

        markDone(trigger.donePartitions(endInput), actions);
    }

    private void markDoneByWatermark(List<ManifestCommittable> committables) {
        // extract watermarks from committables and update partition watermarks
        Tuple2<Map<BinaryRow, Long>, Boolean> extractedWatermarks =
                extractPartitionWatermarks(committables);
        Map<BinaryRow, Long> partitionWatermarks = extractedWatermarks.f0;
        boolean endInput = extractedWatermarks.f1;
        Optional<Long> latestWatermark = partitionWatermarks.values().stream().max(Long::compareTo);

        if (!latestWatermark.isPresent()) {
            LOG.warn("No watermark found in this batch of committables, skip partition mark done.");
            return;
        }

        partitionWatermarks.forEach(
                (row, value) -> {
                    String partition =
                            PartitionPathUtils.generatePartitionPath(
                                    partitionComputer.generatePartValues(row));
                    trigger.notifyPartition(partition, value);
                });

        markDone(trigger.donePartitions(endInput, latestWatermark.get(), true), actions);
    }

    private Tuple2<Map<BinaryRow, Long>, Boolean> extractPartitionWatermarks(
            List<ManifestCommittable> committables) {
        boolean endInput = false;
        Map<BinaryRow, Long> partitionWatermarks = new HashMap<>();
        for (ManifestCommittable committable : committables) {
            Long watermark = committable.watermark();
            if (watermark != null) {
                for (CommitMessage commitMessage : committable.fileCommittables()) {
                    CommitMessageImpl message = (CommitMessageImpl) commitMessage;
                    if (waitCompaction || !message.newFilesIncrement().isEmpty()) {
                        partitionWatermarks.compute(
                                message.partition(),
                                (partition, old) ->
                                        old == null ? watermark : Math.max(old, watermark));
                    }
                }
            }

            if (committable.identifier() == Long.MAX_VALUE) {
                endInput = true;
            }
        }

        return Tuple2.of(partitionWatermarks, endInput);
    }

    public static void markDone(List<String> partitions, List<PartitionMarkDoneAction> actions) {
        for (String partition : partitions) {
            try {
                for (PartitionMarkDoneAction action : actions) {
                    action.markDone(partition);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void snapshotState() throws Exception {
        trigger.snapshotState();
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAllQuietly(actions);
    }
}
