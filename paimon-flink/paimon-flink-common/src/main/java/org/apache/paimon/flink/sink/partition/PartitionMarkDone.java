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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_TIME_INTERVAL;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Mark partition done. */
public class PartitionMarkDone implements Closeable {

    private static final ListStateDescriptor<List<String>> PENDING_PARTITIONS_STATE_DESC =
            new ListStateDescriptor<>(
                    "mark-done-pending-partitions",
                    new ListSerializer<>(StringSerializer.INSTANCE));

    private final InternalRowPartitionComputer partitionComputer;
    private final PartitionMarkDoneTrigger trigger;
    private final List<PartitionMarkDoneAction> actions;

    @Nullable
    public static PartitionMarkDone create(
            boolean isStreaming,
            boolean isRestored,
            OperatorStateStore stateStore,
            FileStoreTable table)
            throws Exception {
        if (!isStreaming) {
            return null;
        }

        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys.isEmpty()) {
            return null;
        }

        CoreOptions coreOptions = table.coreOptions();
        Options options = coreOptions.toConfiguration();

        Duration idleToDone = options.get(PARTITION_IDLE_TIME_TO_DONE);
        if (idleToDone == null) {
            return null;
        }

        MetastoreClient.Factory metastoreClientFactory =
                table.catalogEnvironment().metastoreClientFactory();

        String partitionMarkDownAction = options.get(PARTITION_MARK_DONE_ACTION);
        if (partitionMarkDownAction.contains("done-partition")) {
            checkNotNull(
                    metastoreClientFactory,
                    "Cannot mark done partition for table without metastore.");
            checkArgument(
                    coreOptions.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        coreOptions.partitionDefaultName(),
                        table.schema().logicalPartitionType(),
                        partitionKeys.toArray(new String[0]));

        PartitionMarkDoneTrigger trigger =
                new PartitionMarkDoneTrigger(
                        new PartitionMarkDoneTriggerState(isRestored, stateStore),
                        new PartitionTimeExtractor(
                                coreOptions.partitionTimestampPattern(),
                                coreOptions.partitionTimestampFormatter()),
                        options.get(PARTITION_TIME_INTERVAL),
                        idleToDone);

        List<PartitionMarkDoneAction> actions =
                Arrays.asList(partitionMarkDownAction.split(",")).stream()
                        .map(
                                action -> {
                                    switch (action) {
                                        case "success-file":
                                            return new SuccessFileMarkDoneAction(
                                                    table.fileIO(), table.location());
                                        case "done-partition":
                                            return new AddDonePartitionAction(
                                                    metastoreClientFactory.create());
                                        default:
                                            throw new UnsupportedOperationException(action);
                                    }
                                })
                        .collect(Collectors.toList());

        return new PartitionMarkDone(partitionComputer, trigger, actions);
    }

    public PartitionMarkDone(
            InternalRowPartitionComputer partitionComputer,
            PartitionMarkDoneTrigger trigger,
            List<PartitionMarkDoneAction> actions) {
        this.partitionComputer = partitionComputer;
        this.trigger = trigger;
        this.actions = actions;
    }

    public void notifyCommittable(List<ManifestCommittable> committables) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (ManifestCommittable committable : committables) {
            committable.fileCommittables().stream()
                    .map(CommitMessage::partition)
                    .forEach(partitions::add);
        }

        partitions.stream()
                .map(partitionComputer::generatePartValues)
                .map(PartitionPathUtils::generatePartitionPath)
                .forEach(trigger::notifyPartition);

        for (String partition : trigger.donePartitions()) {
            try {
                for (PartitionMarkDoneAction action : actions) {
                    action.markDone(partition);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void snapshotState() throws Exception {
        trigger.snapshotState();
    }

    @Override
    public void close() throws IOException {
        for (PartitionMarkDoneAction action : actions) {
            action.close();
        }
    }

    private static class PartitionMarkDoneTriggerState implements PartitionMarkDoneTrigger.State {

        private final boolean isRestored;
        private final ListState<List<String>> pendingPartitionsState;

        private PartitionMarkDoneTriggerState(boolean isRestored, OperatorStateStore stateStore)
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
}
