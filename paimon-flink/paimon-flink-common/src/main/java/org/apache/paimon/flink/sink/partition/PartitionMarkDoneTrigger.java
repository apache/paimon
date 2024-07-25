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
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

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

import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_TIME_INTERVAL;
import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** Trigger to mark partitions done with streaming job. */
public class PartitionMarkDoneTrigger {

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
            new ListStateDescriptor<>(
                    "mark-done-pending-partitions",
                    new ListSerializer<>(StringSerializer.INSTANCE));

    private final State state;
    private final PartitionTimeExtractor timeExtractor;
    private long timeInterval;
    private long idleTime;
    private final boolean partitionMarkDoneWhenEndInput;
    private final Map<String, Long> pendingPartitions;

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            Duration timeInterval,
            Duration idleTime,
            boolean partitionMarkDoneWhenEndInput)
            throws Exception {
        this(
                state,
                timeExtractor,
                timeInterval,
                idleTime,
                System.currentTimeMillis(),
                partitionMarkDoneWhenEndInput);
    }

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            Duration timeInterval,
            Duration idleTime,
            long currentTimeMillis,
            boolean partitionMarkDoneWhenEndInput)
            throws Exception {
        this.pendingPartitions = new HashMap<>();
        this.state = state;
        this.timeExtractor = timeExtractor;
        if (timeInterval != null) {
            this.timeInterval = timeInterval.toMillis();
        }
        if (idleTime != null) {
            this.idleTime = idleTime.toMillis();
        }
        this.partitionMarkDoneWhenEndInput = partitionMarkDoneWhenEndInput;
        state.restore().forEach(p -> pendingPartitions.put(p, currentTimeMillis));
    }

    public void notifyPartition(String partition) {
        notifyPartition(partition, System.currentTimeMillis());
    }

    public void notifyPartition(String partition, long currentTimeMillis) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.put(partition, currentTimeMillis);
        }
    }

    public List<String> donePartitions(boolean endInput) {
        return donePartitions(endInput, System.currentTimeMillis());
    }

    public List<String> donePartitions(boolean endInput, long currentTimeMillis) {
        if (endInput && partitionMarkDoneWhenEndInput) {
            return new ArrayList<>(pendingPartitions.keySet());
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
                pendingPartitions.addAll(pendingPartitionsState.get().iterator().next());
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
