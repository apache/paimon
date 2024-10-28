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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_TIME_INTERVAL;
import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** Trigger to mark partitions done with streaming job. */
public class PartitionMarkDoneTrigger implements PartitionTrigger {

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
            new ListStateDescriptor<>(
                    "mark-done-pending-partitions",
                    new ListSerializer<>(StringSerializer.INSTANCE));

    private final State state;
    private final PartitionTimeExtractor timeExtractor;
    // can be null when markDoneWhenEndInput is true
    @Nullable private final Long timeInterval;
    // can be null when markDoneWhenEndInput is true
    @Nullable private final Long idleTime;
    private final boolean markDoneWhenEndInput;
    private final Map<String, Long> pendingPartitions;
    private final List<PartitionMarkDoneAction> actions;
    private final boolean waitCompaction;
    private final InternalRowPartitionComputer partitionComputer;

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            @Nullable Duration timeInterval,
            @Nullable Duration idleTime,
            boolean markDoneWhenEndInput,
            List<PartitionMarkDoneAction> actions,
            boolean waitCompaction,
            InternalRowPartitionComputer partitionComputer)
            throws Exception {
        this(
                state,
                timeExtractor,
                timeInterval,
                idleTime,
                System.currentTimeMillis(),
                markDoneWhenEndInput,
                actions,
                waitCompaction,
                partitionComputer);
    }

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            @Nullable Duration timeInterval,
            @Nullable Duration idleTime,
            long currentTimeMillis,
            boolean markDoneWhenEndInput,
            List<PartitionMarkDoneAction> actions,
            boolean waitCompaction,
            InternalRowPartitionComputer partitionComputer)
            throws Exception {
        this.pendingPartitions = new HashMap<>();
        this.state = state;
        this.timeExtractor = timeExtractor;
        this.timeInterval = timeInterval == null ? null : timeInterval.toMillis();
        this.idleTime = idleTime == null ? null : idleTime.toMillis();
        this.markDoneWhenEndInput = markDoneWhenEndInput;
        this.actions = actions;
        this.waitCompaction = waitCompaction;
        this.partitionComputer = partitionComputer;
        state.restore().forEach(p -> pendingPartitions.put(p, currentTimeMillis));
    }

    @Override
    public void notifyCommittable(List<ManifestCommittable> committables) {
        Set<BinaryRow> partitions = new HashSet<>();
        boolean endInput = false;
        for (ManifestCommittable committable : committables) {
            for (CommitMessage commitMessage : committable.fileCommittables()) {
                CommitMessageImpl message = (CommitMessageImpl) commitMessage;
                if (waitCompaction
                        || !message.indexIncrement().isEmpty()
                        || !message.newFilesIncrement().isEmpty()) {
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
                .forEach(this::notifyPartition);
        markDone(donePartitions(endInput), actions);
    }

    public void notifyPartition(String partition) {
        notifyPartition(partition, System.currentTimeMillis());
    }

    @VisibleForTesting
    void notifyPartition(String partition, long currentTimeMillis) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.put(partition, currentTimeMillis);
        }
    }

    public List<String> donePartitions(boolean endInput) {
        return donePartitions(endInput, System.currentTimeMillis());
    }

    @VisibleForTesting
    List<String> donePartitions(boolean endInput, long currentTimeMillis) {
        if (endInput && markDoneWhenEndInput) {
            return new ArrayList<>(pendingPartitions.keySet());
        }

        if (timeInterval == null || idleTime == null) {
            return Collections.emptyList();
        }

        List<String> needDone = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter = pendingPartitions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            String partition = entry.getKey();

            long lastUpdateTime = entry.getValue();
            long partitionStartTime =
                    extractDateTime(partition)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
            long partitionEndTime = partitionStartTime + timeInterval;
            lastUpdateTime = Math.max(lastUpdateTime, partitionEndTime);

            if (currentTimeMillis - lastUpdateTime > idleTime) {
                needDone.add(partition);
                iter.remove();
            }
        }
        return needDone;
    }

    @VisibleForTesting
    LocalDateTime extractDateTime(String partition) {
        try {
            return timeExtractor.extract(extractPartitionSpecFromPath(new Path(partition)));
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Can't extract datetime from partition " + partition, e);
        }
    }

    public void snapshotState() throws Exception {
        state.update(new ArrayList<>(pendingPartitions.keySet()));
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
    public void close() throws IOException {
        IOUtils.closeAllQuietly(actions);
    }

    /** State to store partitions. */
    public interface State {
        List<String> restore() throws Exception;

        void update(List<String> partitions) throws Exception;
    }

    /** State to store partitions with streaming job. */
    private static class PartitionMarkDoneTriggerState implements State {

        private final boolean isRestored;
        private final ListState<List<String>> pendingPartitionsState;

        public PartitionMarkDoneTriggerState(boolean isRestored, OperatorStateStore stateStore)
                throws Exception {
            this.isRestored = isRestored;
            this.pendingPartitionsState = stateStore.getListState(PENDING_PARTITIONS_STATE_DESC);
        }

        @Override
        public List<String> restore() throws Exception {
            List<String> pendingPartitions = new ArrayList<>();
            if (isRestored) {
                Iterator<List<String>> state = pendingPartitionsState.get().iterator();
                if (state.hasNext()) {
                    pendingPartitions.addAll(state.next());
                }
            }
            return pendingPartitions;
        }

        @Override
        public void update(List<String> partitions) throws Exception {
            pendingPartitionsState.update(Collections.singletonList(partitions));
        }
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

    public static Optional<PartitionTrigger> create(
            CoreOptions coreOptions,
            boolean isStreaming,
            boolean isRestored,
            OperatorStateStore stateStore,
            FileStoreTable table)
            throws Exception {

        if (disablePartitionMarkDone(isStreaming, table, coreOptions.toConfiguration())) {
            return Optional.empty();
        }

        List<PartitionMarkDoneAction> actions =
                PartitionMarkDoneAction.createActions(table, coreOptions);

        // if batch read skip level 0 files, we should wait compaction to mark done
        // otherwise, some data may not be readable, and there might be data delays
        boolean waitCompaction =
                !table.primaryKeys().isEmpty()
                        && (coreOptions.deletionVectorsEnabled()
                                || coreOptions.mergeEngine() == CoreOptions.MergeEngine.FIRST_ROW);

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());

        Options options = coreOptions.toConfiguration();
        return Optional.of(
                new PartitionMarkDoneTrigger(
                        new PartitionMarkDoneTrigger.PartitionMarkDoneTriggerState(
                                isRestored, stateStore),
                        new PartitionTimeExtractor(
                                coreOptions.partitionTimestampPattern(),
                                coreOptions.partitionTimestampFormatter()),
                        options.get(PARTITION_TIME_INTERVAL),
                        options.get(PARTITION_IDLE_TIME_TO_DONE),
                        options.get(PARTITION_MARK_DONE_WHEN_END_INPUT),
                        actions,
                        waitCompaction,
                        partitionComputer));
    }
}
