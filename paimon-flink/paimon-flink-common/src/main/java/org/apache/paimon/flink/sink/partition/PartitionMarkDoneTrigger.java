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

import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.utils.StringUtils;

import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/** Trigger to mark partitions done. */
public class PartitionMarkDoneTrigger {

    private final State state;
    private final PartitionTimeExtractor timeExtractor;
    private final long timeInterval;
    private final long idleTime;
    private final Map<String, Long> pendingPartitions;

    public PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            Duration timeInterval,
            Duration idleTime)
            throws Exception {
        this(state, timeExtractor, timeInterval, idleTime, System.currentTimeMillis());
    }

    PartitionMarkDoneTrigger(
            State state,
            PartitionTimeExtractor timeExtractor,
            Duration timeInterval,
            Duration idleTime,
            long currentTimeMillis)
            throws Exception {
        this.state = state;
        this.timeExtractor = timeExtractor;
        this.timeInterval = timeInterval.toMillis();
        this.idleTime = idleTime.toMillis();
        this.pendingPartitions = new HashMap<>();
        state.restore().forEach(p -> pendingPartitions.put(p, currentTimeMillis));
    }

    public void notifyPartition(String partition) {
        notifyPartition(partition, System.currentTimeMillis());
    }

    void notifyPartition(String partition, long currentTimeMillis) {
        if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
            this.pendingPartitions.put(partition, currentTimeMillis);
        }
    }

    public List<String> donePartitions() {
        return donePartitions(System.currentTimeMillis());
    }

    public List<String> donePartitions(long currentTimeMillis) {
        List<String> needDone = new ArrayList<>();
        Iterator<Map.Entry<String, Long>> iter = pendingPartitions.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, Long> entry = iter.next();
            String partition = entry.getKey();

            long lastUpdateTime = entry.getValue();
            long partitionStartTime =
                    timeExtractor
                            .extract(extractPartitionSpecFromPath(new Path(partition)))
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

    public void snapshotState() throws Exception {
        state.update(new ArrayList<>(pendingPartitions.keySet()));
    }

    /** State to store partitions. */
    public interface State {

        List<String> restore() throws Exception;

        void update(List<String> partitions) throws Exception;
    }
}
