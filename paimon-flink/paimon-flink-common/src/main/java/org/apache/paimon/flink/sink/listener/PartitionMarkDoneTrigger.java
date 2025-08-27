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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_TIME_INTERVAL;
import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** Trigger to mark partitions done with streaming job. */
public class PartitionMarkDoneTrigger {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionMarkDoneTrigger.class);
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

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            @Nullable Duration timeInterval,
            @Nullable Duration idleTime,
            boolean markDoneWhenEndInput)
            throws Exception {
        this(
                state,
                timeExtractor,
                timeInterval,
                idleTime,
                System.currentTimeMillis(),
                markDoneWhenEndInput);
    }

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            @Nullable Duration timeInterval,
            @Nullable Duration idleTime,
            long currentTimeMillis,
            boolean markDoneWhenEndInput)
            throws Exception {
        this.pendingPartitions = new HashMap<>();
        this.state = state;
        this.timeExtractor = timeExtractor;
        this.timeInterval = timeInterval == null ? null : timeInterval.toMillis();
        this.idleTime = idleTime == null ? null : idleTime.toMillis();
        this.markDoneWhenEndInput = markDoneWhenEndInput;
        state.restore().forEach(p -> pendingPartitions.put(p, currentTimeMillis));
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
        return donePartitions(endInput, System.currentTimeMillis(), false);
    }

    List<String> donePartitions(boolean endInput, long currentTimeMillis) {
        return donePartitions(endInput, currentTimeMillis, false);
    }

    @VisibleForTesting
    List<String> donePartitions(
            boolean endInput, long currentTimeMillis, boolean watermarkEnabled) {
        if (endInput && markDoneWhenEndInput) {
            return new ArrayList<>(pendingPartitions.keySet());
        }

        if (timeInterval == null || idleTime == null) {
            return Collections.emptyList();
        }
        LOG.debug(
                "End input is true and markDoneWhenEndInput is enabled, mark all pending partitions done: {}",
                String.join(",", pendingPartitions.keySet()));

        List<String> needDone = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter = pendingPartitions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            String partition = entry.getKey();
            long lastUpdateTime = entry.getValue();
            LOG.debug(
                    "Partition {} is in progress, last update time: {}",
                    partition,
                    entry.getValue());

            long partitionStartTime;
            Optional<LocalDateTime> partitionLocalDateTimeOpt = extractDateTime(partition);
            // skip illegal partition
            if (!partitionLocalDateTimeOpt.isPresent()) {
                LOG.debug("Partition {} is illegal, skip it", partition);
                iter.remove();
                continue;
            }

            if (watermarkEnabled) {
                // watermark should be compared as UTC time
                partitionStartTime =
                        partitionLocalDateTimeOpt
                                .get()
                                .atZone(ZoneId.of("UTC"))
                                .toInstant()
                                .toEpochMilli();
            } else {
                partitionStartTime =
                        partitionLocalDateTimeOpt
                                .get()
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
            }
            long partitionEndTime = partitionStartTime + timeInterval;
            lastUpdateTime = Math.max(lastUpdateTime, partitionEndTime);
            LOG.debug(
                    "Partition {} start time: {}, end time: {}, last update time after compare: {}",
                    partition,
                    partitionStartTime,
                    partitionEndTime,
                    lastUpdateTime);

            if (currentTimeMillis - lastUpdateTime > idleTime) {
                LOG.debug(
                        "Partition {} is idle for {} greater than idleTime {}, mark it done",
                        partition,
                        currentTimeMillis - lastUpdateTime,
                        idleTime);
                needDone.add(partition);
                iter.remove();
            } else {
                LOG.debug(
                        "Partition {} is idle for {} less than idleTime {}, no not mark it done",
                        partition,
                        currentTimeMillis - lastUpdateTime,
                        idleTime);
            }
        }
        LOG.debug("Need done partitions: {}", String.join(",", needDone));
        return needDone;
    }

    @VisibleForTesting
    Optional<LocalDateTime> extractDateTime(String partition) {
        try {
            return Optional.of(
                    timeExtractor.extract(extractPartitionSpecFromPath(new Path(partition))));
        } catch (DateTimeParseException e) {
            LOG.warn(
                    "Can't extract datetime from partition {}, please check configuration items 'partition.timestamp-formatter' and 'partition.timestamp-pattern'.",
                    partition);
            return Optional.empty();
        }
    }

    public void snapshotState() throws Exception {
        state.update(new ArrayList<>(pendingPartitions.keySet()));
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

    public static PartitionMarkDoneTrigger create(
            CoreOptions coreOptions, boolean isRestored, OperatorStateStore stateStore)
            throws Exception {
        Options options = coreOptions.toConfiguration();
        return new PartitionMarkDoneTrigger(
                new PartitionMarkDoneTrigger.PartitionMarkDoneTriggerState(isRestored, stateStore),
                new PartitionTimeExtractor(
                        coreOptions.partitionTimestampPattern(),
                        coreOptions.partitionTimestampFormatter()),
                options.get(PARTITION_TIME_INTERVAL),
                options.get(PARTITION_IDLE_TIME_TO_DONE),
                options.get(PARTITION_MARK_DONE_WHEN_END_INPUT));
    }
}
