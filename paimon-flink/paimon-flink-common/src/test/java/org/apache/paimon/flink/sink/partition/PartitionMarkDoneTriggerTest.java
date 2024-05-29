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

import org.apache.paimon.partition.PartitionTimeExtractor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionMarkDoneTriggerTest {

    @Test
    public void test() throws Exception {
        List<String> pendingPartitions = new ArrayList<>();
        PartitionMarkDoneTrigger.State state =
                new PartitionMarkDoneTrigger.State() {
                    @Override
                    public List<String> restore() {
                        return new ArrayList<>(pendingPartitions);
                    }

                    @Override
                    public void update(List<String> partitions) {
                        pendingPartitions.clear();
                        pendingPartitions.addAll(partitions);
                    }
                };

        PartitionTimeExtractor extractor = new PartitionTimeExtractor("$dt", "yyyy-MM-dd");
        Duration timeInternal = Duration.ofDays(1);
        Duration idleTime = Duration.ofMinutes(15);
        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        state, extractor, timeInternal, idleTime, toEpochMillis("2024-02-01"));

        // test not reach partition end + idle time
        trigger.notifyPartition("dt=2024-02-02", toEpochMillis("2024-02-01"));
        List<String> partitions = trigger.donePartitions(toEpochMillis("2024-02-03"));
        assertThat(partitions).isEmpty();

        // test state
        assertThat(pendingPartitions).isEmpty();
        trigger.snapshotState();
        assertThat(pendingPartitions).containsOnly("dt=2024-02-02");

        // test trigger
        partitions = trigger.donePartitions(toEpochMillis("2024-02-03") + idleTime.toMillis());
        assertThat(partitions).isEmpty();
        partitions = trigger.donePartitions(toEpochMillis("2024-02-03") + idleTime.toMillis() + 1);
        assertThat(partitions).containsOnly("dt=2024-02-02");

        // test state
        trigger.snapshotState();
        assertThat(pendingPartitions).isEmpty();

        // test refresh
        trigger.notifyPartition("dt=2024-02-03", toEpochMillis("2024-02-03"));
        trigger.notifyPartition("dt=2024-02-03", toEpochMillis("2024-02-04") + idleTime.toMillis());
        partitions = trigger.donePartitions(toEpochMillis("2024-02-04") + idleTime.toMillis() + 1);
        assertThat(partitions).isEmpty();
        partitions =
                trigger.donePartitions(toEpochMillis("2024-02-04") + 2 * idleTime.toMillis() + 1);
        assertThat(partitions).containsOnly("dt=2024-02-03");

        // test restore
        pendingPartitions.add("dt=2024-02-04");
        trigger =
                new PartitionMarkDoneTrigger(
                        state, extractor, timeInternal, idleTime, toEpochMillis("2024-02-06"));
        partitions = trigger.donePartitions(toEpochMillis("2024-02-06"));
        assertThat(partitions).isEmpty();
        partitions = trigger.donePartitions(toEpochMillis("2024-02-06") + idleTime.toMillis() + 1);
        assertThat(partitions).containsOnly("dt=2024-02-04");
    }

    private long toEpochMillis(String dt) {
        return LocalDateTime.of(LocalDate.parse(dt), LocalTime.MIN)
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}
