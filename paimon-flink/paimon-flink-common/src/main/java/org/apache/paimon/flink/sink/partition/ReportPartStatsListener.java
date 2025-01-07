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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This listener will collect data from the newly touched partition and then decide when to trigger
 * a report based on the partition's idle time.
 */
public class ReportPartStatsListener implements PartitionListener {

    @SuppressWarnings("unchecked")
    private static final ListStateDescriptor<Map<String, Long>> PENDING_REPORT_STATE_DESC =
            new ListStateDescriptor<>(
                    "pending-report-hms-partition",
                    new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE));

    private final InternalRowPartitionComputer partitionComputer;
    private final PartitionStatisticsReporter partitionStatisticsReporter;
    private final ListState<Map<String, Long>> pendingPartitionsState;
    private final Map<String, Long> pendingPartitions;
    private final long idleTime;

    private ReportPartStatsListener(
            InternalRowPartitionComputer partitionComputer,
            PartitionStatisticsReporter partitionStatisticsReporter,
            OperatorStateStore store,
            boolean isRestored,
            long idleTime)
            throws Exception {
        this.partitionComputer = partitionComputer;
        this.partitionStatisticsReporter = partitionStatisticsReporter;
        this.pendingPartitionsState = store.getListState(PENDING_REPORT_STATE_DESC);
        this.pendingPartitions = new HashMap<>();
        if (isRestored) {
            Iterator<Map<String, Long>> it = pendingPartitionsState.get().iterator();
            if (it.hasNext()) {
                Map<String, Long> state = it.next();
                pendingPartitions.putAll(state);
            }
        }
        this.idleTime = idleTime;
    }

    public void notifyCommittable(List<ManifestCommittable> committables) {
        Set<String> partition = new HashSet<>();
        boolean endInput = false;
        for (ManifestCommittable committable : committables) {
            for (CommitMessage commitMessage : committable.fileCommittables()) {
                CommitMessageImpl message = (CommitMessageImpl) commitMessage;
                if (!message.newFilesIncrement().isEmpty()
                        || !message.compactIncrement().isEmpty()) {
                    partition.add(
                            PartitionPathUtils.generatePartitionPath(
                                    partitionComputer.generatePartValues(message.partition())));
                }
            }
            if (committable.identifier() == Long.MAX_VALUE) {
                endInput = true;
            }
        }
        // append to map
        long current = System.currentTimeMillis();
        partition.forEach(p -> pendingPartitions.put(p, current));

        try {
            Map<String, Long> partitions = reportPartition(endInput);
            for (Map.Entry<String, Long> entry : partitions.entrySet()) {
                partitionStatisticsReporter.report(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Long> reportPartition(boolean endInput) {
        if (endInput) {
            return pendingPartitions;
        }

        Iterator<Map.Entry<String, Long>> iterator = pendingPartitions.entrySet().iterator();
        Map<String, Long> result = new HashMap<>();
        long current = System.currentTimeMillis();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (current - entry.getValue() > idleTime) {
                result.put(entry.getKey(), entry.getValue());
                iterator.remove();
            }
        }

        return result;
    }

    public void snapshotState() throws Exception {
        pendingPartitionsState.update(Collections.singletonList(pendingPartitions));
    }

    public static Optional<ReportPartStatsListener> create(
            boolean isRestored, OperatorStateStore stateStore, FileStoreTable table)
            throws Exception {

        CoreOptions coreOptions = table.coreOptions();
        Options options = coreOptions.toConfiguration();
        if (options.get(FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_REPORT_STATISTIC).toMillis()
                <= 0) {
            return Optional.empty();
        }

        if ((table.partitionKeys().isEmpty())) {
            return Optional.empty();
        }

        if (!coreOptions.partitionedTableInMetastore()) {
            return Optional.empty();
        }

        if (table.catalogEnvironment().metastoreClientFactory() == null) {
            return Optional.empty();
        }

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        table.partitionKeys().toArray(new String[0]),
                        coreOptions.legacyPartitionName());

        return Optional.of(
                new ReportPartStatsListener(
                        partitionComputer,
                        new PartitionStatisticsReporter(
                                table,
                                table.catalogEnvironment().metastoreClientFactory().create()),
                        stateStore,
                        isRestored,
                        options.get(FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_REPORT_STATISTIC)
                                .toMillis()));
    }

    @Override
    public void close() throws IOException {
        if (partitionStatisticsReporter != null) {
            partitionStatisticsReporter.close();
        }
    }
}
