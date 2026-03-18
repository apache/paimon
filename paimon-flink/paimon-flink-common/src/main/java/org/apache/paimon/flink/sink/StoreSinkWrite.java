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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableRunnable;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;

/** Helper class of {@link PrepareCommitOperator} for different types of paimon sinks. */
public interface StoreSinkWrite {

    void setWriteRestore(WriteRestore writeRestore);

    @Nullable
    SinkRecord write(InternalRow rowData) throws Exception;

    @Nullable
    SinkRecord write(InternalRow rowData, int bucket) throws Exception;

    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) throws IOException;

    void snapshotState() throws Exception;

    boolean streamingMode();

    void close() throws Exception;

    /**
     * Replace the internal {@link TableWriteImpl} with the one provided by {@code
     * newWriteProvider}. The state of the old {@link TableWriteImpl} will also be transferred to
     * the new {@link TableWriteImpl} by {@link TableWriteImpl#checkpoint()} and {@link
     * TableWriteImpl#restore(List)}.
     *
     * <p>Currently, this method is only used by CDC sinks because they need to deal with schema
     * changes. {@link TableWriteImpl} with the new schema will be provided by {@code
     * newWriteProvider}.
     */
    void replace(FileStoreTable newTable) throws Exception;

    /** Provider of {@link StoreSinkWrite}. */
    @FunctionalInterface
    interface Provider extends Serializable {

        /**
         * TODO: The argument list has become too complicated. Build {@link TableWriteImpl} directly
         * in caller and simplify the argument list.
         */
        StoreSinkWrite provide(
                FileStoreTable table,
                String commitUser,
                StoreSinkWriteState state,
                IOManager ioManager,
                MemoryPoolFactory memoryPoolFactory,
                @Nullable MetricGroup metricGroup);
    }

    static StoreSinkWrite.Provider createWriteProvider(
            FileStoreTable fileStoreTable,
            CheckpointConfig checkpointConfig,
            boolean isStreaming,
            boolean ignorePreviousFiles,
            boolean hasSinkMaterializer) {
        SerializableRunnable assertNoSinkMaterializer =
                () ->
                        Preconditions.checkArgument(
                                !hasSinkMaterializer,
                                String.format(
                                        "Sink materializer must not be used with Paimon sink. "
                                                + "Please set '%s' to '%s' in Flink's config.",
                                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE
                                                .key(),
                                        ExecutionConfigOptions.UpsertMaterialize.NONE.name()));

        Options options = fileStoreTable.coreOptions().toConfiguration();
        CoreOptions.ChangelogProducer changelogProducer =
                fileStoreTable.coreOptions().changelogProducer();
        boolean waitCompaction;
        CoreOptions coreOptions = fileStoreTable.coreOptions();
        if (coreOptions.writeOnly()) {
            waitCompaction = false;
        } else {
            waitCompaction = coreOptions.prepareCommitWaitCompaction();
            int deltaCommits = -1;
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS);
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits =
                        (int)
                                (fullCompactionThresholdMs
                                        / checkpointConfig.getCheckpointInterval());
            }

            if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION
                    || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) -> {
                    assertNoSinkMaterializer.run();
                    return new GlobalFullCompactionSinkWrite(
                            table,
                            commitUser,
                            state,
                            ioManager,
                            ignorePreviousFiles,
                            waitCompaction,
                            finalDeltaCommits,
                            isStreaming,
                            memoryPoolFactory,
                            metricGroup);
                };
            }

            if (coreOptions.needLookup()) {
                return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) -> {
                    assertNoSinkMaterializer.run();
                    return new LookupSinkWrite(
                            table,
                            commitUser,
                            state,
                            ioManager,
                            ignorePreviousFiles,
                            waitCompaction,
                            isStreaming,
                            memoryPoolFactory,
                            metricGroup);
                };
            }
        }

        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) -> {
            assertNoSinkMaterializer.run();
            return new StoreSinkWriteImpl(
                    table,
                    commitUser,
                    state,
                    ioManager,
                    ignorePreviousFiles,
                    waitCompaction,
                    isStreaming,
                    memoryPoolFactory,
                    metricGroup);
        };
    }
}
