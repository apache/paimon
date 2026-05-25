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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Phase 1 operator for data evolution streaming upsert. Maintains a {@link UpsertKeyIndex} per
 * partition, classifies each record as UPDATE or INSERT, and emits tagged {@link UpsertRecord}s
 * downstream for the Phase 2 write operator.
 */
public class UpsertClassifyOperator extends AbstractStreamOperator<UpsertRecord>
        implements OneInputStreamOperator<InternalRow, UpsertRecord>, BoundedOneInput {

    private static final Logger LOG = LoggerFactory.getLogger(UpsertClassifyOperator.class);

    private final FileStoreTable table;
    private final List<String> upsertKeyColumns;

    private transient RowType upsertKeyType;
    private transient RowCompactedSerializer keySerializer;
    private transient InternalRow.FieldGetter[] keyFieldGetters;
    private transient InternalRow.FieldGetter[] partitionFieldGetters;
    private transient IOManager paimonIOManager;
    private transient InternalRowSerializer rowSerializer;
    private transient InternalRowSerializer partitionSerializer;

    private transient Map<BinaryRow, UpsertKeyIndex> partitionIndices;
    private transient Map<BinaryRow, LinkedHashMap<BytesKey, InternalRow>> buffer;
    private transient long lastSyncedSnapshotId;
    private transient SnapshotManager snapshotManager;
    private transient File indexBaseDir;

    public UpsertClassifyOperator(
            StreamOperatorParameters<UpsertRecord> parameters,
            FileStoreTable table,
            List<String> upsertKeyColumns) {
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
        this.table = table;
        this.upsertKeyColumns = upsertKeyColumns;
    }

    @Override
    public void open() throws Exception {
        super.open();

        RowType tableRowType = table.rowType();

        List<DataField> keyFields = new ArrayList<>();
        int[] keyFieldIndices = new int[upsertKeyColumns.size()];
        for (int i = 0; i < upsertKeyColumns.size(); i++) {
            int idx = tableRowType.getFieldIndex(upsertKeyColumns.get(i));
            Preconditions.checkArgument(
                    idx >= 0, "Upsert key column not found: " + upsertKeyColumns.get(i));
            keyFieldIndices[i] = idx;
            keyFields.add(tableRowType.getFields().get(idx));
        }
        this.upsertKeyType = new RowType(keyFields);
        this.keySerializer = new RowCompactedSerializer(upsertKeyType);
        this.keyFieldGetters = new InternalRow.FieldGetter[upsertKeyColumns.size()];
        for (int i = 0; i < upsertKeyColumns.size(); i++) {
            keyFieldGetters[i] =
                    InternalRow.createFieldGetter(
                            tableRowType.getTypeAt(keyFieldIndices[i]), keyFieldIndices[i]);
        }

        List<String> partitionKeys = table.partitionKeys();
        this.partitionFieldGetters = new InternalRow.FieldGetter[partitionKeys.size()];
        for (int i = 0; i < partitionKeys.size(); i++) {
            int idx = tableRowType.getFieldIndex(partitionKeys.get(i));
            partitionFieldGetters[i] =
                    InternalRow.createFieldGetter(tableRowType.getTypeAt(idx), idx);
        }
        if (partitionKeys.isEmpty()) {
            this.partitionSerializer = null;
        } else {
            RowType partRowType = tableRowType.project(partitionKeys.toArray(new String[0]));
            this.partitionSerializer = new InternalRowSerializer(partRowType);
        }

        this.rowSerializer = new InternalRowSerializer(tableRowType);
        this.partitionIndices = new HashMap<>();
        this.buffer = new HashMap<>();

        this.snapshotManager = table.store().snapshotManager();
        Long latestId = snapshotManager.latestSnapshotId();
        this.lastSyncedSnapshotId = latestId != null ? latestId : -1;

        this.indexBaseDir =
                new File(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectories()[0],
                        "data-evolution-upsert-classify-" + UUID.randomUUID());
        if (!indexBaseDir.mkdirs()) {
            throw new IOException("Failed to create index directory: " + indexBaseDir);
        }

        this.paimonIOManager =
                new IOManagerImpl(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectories()[0]
                                .getPath());
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        InternalRow row = element.getValue();
        RowKind kind = row.getRowKind();
        Preconditions.checkArgument(
                kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER,
                "Data evolution upsert only supports +I and +U, but got: %s",
                kind);
        BinaryRow partition = extractPartition(row);
        BytesKey keyBytes = extractKeyBytes(row);
        InternalRow copiedRow = rowSerializer.copy(row);

        buffer.computeIfAbsent(partition, k -> new LinkedHashMap<>()).put(keyBytes, copiedRow);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushBuffer();
    }

    @Override
    public void endInput() throws Exception {
        flushBuffer();
    }

    private void flushBuffer() throws Exception {
        incrementalSyncIndex();

        for (Map.Entry<BinaryRow, LinkedHashMap<BytesKey, InternalRow>> partitionEntry :
                buffer.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            LinkedHashMap<BytesKey, InternalRow> keyRows = partitionEntry.getValue();
            UpsertKeyIndex index = getOrBootstrap(partition);

            for (Map.Entry<BytesKey, InternalRow> entry : keyRows.entrySet()) {
                byte[] keyBytes = entry.getKey().bytes;
                InternalRow row = entry.getValue();

                Long rowId = index.lookupRowId(keyBytes);
                if (rowId != null) {
                    long firstRowId = index.lookupFirstRowId(rowId);
                    long offset = rowId - firstRowId;
                    output.collect(
                            new StreamRecord<>(
                                    new UpsertRecord(partition, firstRowId, offset, row)));
                } else {
                    output.collect(new StreamRecord<>(new UpsertRecord(partition, -1, -1, row)));
                }
            }
        }
        buffer.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (UpsertKeyIndex index : partitionIndices.values()) {
            index.close();
        }
        partitionIndices.clear();
        if (paimonIOManager != null) {
            paimonIOManager.close();
        }
    }

    private void incrementalSyncIndex() throws Exception {
        Long latestId = snapshotManager.latestSnapshotIdFromFileSystem();
        if (latestId == null || latestId <= lastSyncedSnapshotId) {
            return;
        }

        for (Map.Entry<BinaryRow, UpsertKeyIndex> entry : partitionIndices.entrySet()) {
            entry.getValue().incrementalSync(table, entry.getKey(), lastSyncedSnapshotId, latestId);
        }

        lastSyncedSnapshotId = latestId;
    }

    private BinaryRow extractPartition(InternalRow row) {
        if (partitionFieldGetters.length == 0) {
            return BinaryRow.EMPTY_ROW;
        }
        GenericRow partRow = new GenericRow(partitionFieldGetters.length);
        for (int i = 0; i < partitionFieldGetters.length; i++) {
            partRow.setField(i, partitionFieldGetters[i].getFieldOrNull(row));
        }
        return partitionSerializer.toBinaryRow(partRow).copy();
    }

    private BytesKey extractKeyBytes(InternalRow row) {
        GenericRow keyRow = new GenericRow(keyFieldGetters.length);
        for (int i = 0; i < keyFieldGetters.length; i++) {
            keyRow.setField(i, keyFieldGetters[i].getFieldOrNull(row));
        }
        return new BytesKey(keySerializer.serializeToBytes(keyRow));
    }

    private UpsertKeyIndex getOrBootstrap(BinaryRow partition) throws Exception {
        UpsertKeyIndex index = partitionIndices.get(partition);
        if (index != null) {
            return index;
        }

        File partDir = new File(indexBaseDir, "part-" + UUID.randomUUID());
        if (!partDir.mkdirs()) {
            throw new IOException("Failed to create partition index dir: " + partDir);
        }
        index = new UpsertKeyIndex(partDir, upsertKeyType);

        if (lastSyncedSnapshotId > 0) {
            CoreOptions coreOptions = table.coreOptions();
            index.bootstrap(
                    table,
                    partition,
                    lastSyncedSnapshotId,
                    paimonIOManager,
                    coreOptions.writeBufferSize() / 2,
                    coreOptions.pageSize(),
                    coreOptions.localSortMaxNumFileHandles(),
                    coreOptions.spillCompressOptions());
            LOG.info("Bootstrapped upsert key index for partition {}", partition);
        }

        partitionIndices.put(partition, index);
        return index;
    }

    /** Wrapper for byte[] with proper equals/hashCode. */
    static final class BytesKey {
        final byte[] bytes;

        BytesKey(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BytesKey)) {
                return false;
            }
            return Arrays.equals(bytes, ((BytesKey) o).bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    /** Factory for creating {@link UpsertClassifyOperator}. */
    public static class Factory extends AbstractStreamOperatorFactory<UpsertRecord>
            implements OneInputStreamOperatorFactory<InternalRow, UpsertRecord> {

        private final FileStoreTable table;
        private final List<String> upsertKeyColumns;

        public Factory(FileStoreTable table, List<String> upsertKeyColumns) {
            this.table = table;
            this.upsertKeyColumns = upsertKeyColumns;
            this.chainingStrategy = ChainingStrategy.ALWAYS;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<UpsertRecord>> T createStreamOperator(
                StreamOperatorParameters<UpsertRecord> parameters) {
            return (T) new UpsertClassifyOperator(parameters, table, upsertKeyColumns);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return UpsertClassifyOperator.class;
        }
    }
}
