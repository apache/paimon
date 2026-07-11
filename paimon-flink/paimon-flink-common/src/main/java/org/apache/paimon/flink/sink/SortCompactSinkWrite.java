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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.append.SortCompactSequenceUtils;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowKindGenerator;
import org.apache.paimon.table.sink.SequencePreservingRowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowKindFilter;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * {@link StoreSinkWrite} for sort compact on primary-key tables.
 *
 * <p>Always uses {@code withSortCompactWrite(true)}, {@code ignorePreviousFiles=true}, and {@code
 * waitCompaction=false} so sorted output lands in {@code newFilesIncrement} and write-stage
 * changelog is disabled. Snapshot-ordering tables additionally preserve {@code _SEQUENCE_NUMBER}.
 */
public class SortCompactSinkWrite extends StoreSinkWriteImpl {

    public SortCompactSinkWrite(
            FileStoreTable table,
            String commitUser,
            StoreSinkWriteState state,
            IOManager ioManager,
            boolean ignorePreviousFiles,
            boolean waitCompaction,
            boolean isStreamingMode,
            MemoryPoolFactory memoryPoolFactory,
            @Nullable MetricGroup metricGroup) {
        super(
                table,
                commitUser,
                state,
                ioManager,
                ignorePreviousFiles,
                waitCompaction,
                isStreamingMode,
                memoryPoolFactory,
                metricGroup);
    }

    @Override
    protected TableWriteImpl<?> newTableWrite(FileStoreTable table) {
        // Use table.rowType() directly: super() calls this method before subclass fields are
        // initialized.
        FileStoreWrite<KeyValue> storeWrite =
                ((KeyValueFileStore) table.store())
                        .newWrite(commitUser, state.getSubtaskId())
                        .withSortCompactWrite(true);
        TableWriteImpl<KeyValue> tableWrite;
        if (table.coreOptions().snapshotSequenceOrdering() && !table.primaryKeys().isEmpty()) {
            RowType logicalRowType = table.rowType();
            KeyValue reuse = new KeyValue();
            tableWrite =
                    new TableWriteImpl<>(
                            SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(
                                    logicalRowType),
                            storeWrite,
                            new SequencePreservingRowKeyExtractor(table.schema()),
                            SortCompactSequenceUtils.sequencePreservingExtractor(
                                    logicalRowType, reuse),
                            null,
                            RowKindFilter.of(table.coreOptions()));
        } else {
            KeyValue reuse = new KeyValue();
            tableWrite =
                    new TableWriteImpl<>(
                            table.rowType(),
                            storeWrite,
                            table.createRowKeyExtractor(),
                            (record, rowKind) ->
                                    reuse.replace(
                                            record.primaryKey(),
                                            KeyValue.UNKNOWN_SEQUENCE,
                                            rowKind,
                                            record.row()),
                            RowKindGenerator.create(table.schema(), table.store().options()),
                            RowKindFilter.of(table.coreOptions()));
        }
        tableWrite
                .withIOManager(paimonIOManager)
                .withIgnorePreviousFiles(ignorePreviousFiles)
                .withMemoryPoolFactory(memoryPoolFactory);
        if (metricGroup != null) {
            tableWrite.withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        }
        return tableWrite;
    }

    @Override
    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        // Batch endInput always passes waitCompaction=true, but sort compact write must not wait
        // for inline compaction. The committer rewrites append-style newFiles into compactAfter.
        return super.prepareCommit(false, checkpointId);
    }

    public static StoreSinkWrite.Provider provider() {
        return (table, commitUser, state, ioManager, memoryPoolFactory, metricGroup) ->
                new SortCompactSinkWrite(
                        table,
                        commitUser,
                        state,
                        ioManager,
                        true,
                        false,
                        false,
                        memoryPoolFactory,
                        metricGroup);
    }
}
