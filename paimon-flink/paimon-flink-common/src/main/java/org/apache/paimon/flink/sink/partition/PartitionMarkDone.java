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
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.PartitionPathUtils;

import org.apache.flink.api.common.state.OperatorStateStore;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_IDLE_TIME_TO_DONE;

/** Mark partition done. */
public class PartitionMarkDone implements PartitionListener {

    private final InternalRowPartitionComputer partitionComputer;
    private final PartitionMarkDoneTrigger trigger;
    private final List<PartitionMarkDoneAction> actions;
    private final boolean waitCompaction;

    public static Optional<PartitionMarkDone> create(
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

        // if batch read skip level 0 files, we should wait compaction to mark done
        // otherwise, some data may not be readable, and there might be data delays
        boolean waitCompaction =
                !table.primaryKeys().isEmpty()
                        && (coreOptions.deletionVectorsEnabled()
                                || coreOptions.mergeEngine() == MergeEngine.FIRST_ROW);

        return Optional.of(
                new PartitionMarkDone(partitionComputer, trigger, actions, waitCompaction));
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

    public PartitionMarkDone(
            InternalRowPartitionComputer partitionComputer,
            PartitionMarkDoneTrigger trigger,
            List<PartitionMarkDoneAction> actions,
            boolean waitCompaction) {
        this.partitionComputer = partitionComputer;
        this.trigger = trigger;
        this.actions = actions;
        this.waitCompaction = waitCompaction;
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
                .forEach(trigger::notifyPartition);

        markDone(trigger.donePartitions(endInput), actions);
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
