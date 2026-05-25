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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/**
 * Phase 2 operator for data evolution streaming upsert. Receives tagged {@link UpsertRecord}s from
 * {@link UpsertClassifyOperator} after the firstRowId-based shuffle. Performs partial writes for
 * UPDATE records and normal appends for INSERT records.
 *
 * <p>The shuffle guarantees that all updates targeting the same file (same firstRowId) arrive at
 * the same subtask, eliminating concurrent-write conflicts.
 */
public class UpsertWriteOperator extends PrepareCommitOperator<UpsertRecord, Committable> {

    private final FileStoreTable table;

    private transient List<UpsertRecord> buffered;
    private transient AbstractFileStoreWrite<InternalRow> tableWrite;
    private transient InnerTableRead tableRead;
    private transient ProjectedRow projectedRow;
    private transient SnapshotManager snapshotManager;
    private transient Map<Long, List<DataFileMeta>> firstIdToFiles;
    private transient RowType fullWriteType;
    private transient RowType readType;

    public UpsertWriteOperator(
            StreamOperatorParameters<Committable> parameters, FileStoreTable table) {
        super(parameters, Options.fromMap(table.options()));
        this.table =
                table.copy(Collections.singletonMap(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G"));
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.fullWriteType = table.rowType();
        this.buffered = new ArrayList<>();

        this.snapshotManager = table.store().snapshotManager();

        this.readType = SpecialFields.rowTypeWithRowId(fullWriteType);
        this.tableRead = table.newRead().withReadType(readType);
        this.projectedRow = ProjectedRow.from(fullWriteType, readType);

        @SuppressWarnings({"unchecked", "resource"})
        TableWriteImpl<InternalRow> writeImpl =
                (TableWriteImpl<InternalRow>)
                        table.newBatchWriteBuilder().newWrite().withWriteType(fullWriteType);
        this.tableWrite = (AbstractFileStoreWrite<InternalRow>) writeImpl.getWrite();

        this.firstIdToFiles = new HashMap<>();
        refreshFileMetadata();
    }

    @Override
    public void processElement(StreamRecord<UpsertRecord> element) throws Exception {
        buffered.add(element.getValue());
    }

    @Override
    protected List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        try {
            refreshFileMetadata();

            Map<BinaryRow, TreeMap<Long, TreeMap<Long, InternalRow>>> updatesByPartition =
                    new HashMap<>();
            Map<BinaryRow, List<InternalRow>> insertsByPartition = new HashMap<>();

            for (UpsertRecord record : buffered) {
                if (record.isInsert()) {
                    insertsByPartition
                            .computeIfAbsent(record.partition(), k -> new ArrayList<>())
                            .add(record.row());
                } else {
                    updatesByPartition
                            .computeIfAbsent(record.partition(), k -> new TreeMap<>())
                            .computeIfAbsent(record.firstRowId(), k -> new TreeMap<>())
                            .put(record.offset(), record.row());
                }
            }
            buffered.clear();

            List<Committable> committables = new ArrayList<>();

            for (Map.Entry<BinaryRow, TreeMap<Long, TreeMap<Long, InternalRow>>> partEntry :
                    updatesByPartition.entrySet()) {
                BinaryRow partition = partEntry.getKey();
                for (Map.Entry<Long, TreeMap<Long, InternalRow>> fileEntry :
                        partEntry.getValue().entrySet()) {
                    CommitMessage msg =
                            writePartialUpdate(partition, fileEntry.getKey(), fileEntry.getValue());
                    committables.add(new Committable(checkpointId, msg));
                }
            }

            for (Map.Entry<BinaryRow, List<InternalRow>> partEntry :
                    insertsByPartition.entrySet()) {
                BinaryRow partition = partEntry.getKey();
                List<InternalRow> rows = partEntry.getValue();

                RecordWriter<InternalRow> writer = tableWrite.createWriter(partition, 0);
                try {
                    for (InternalRow row : rows) {
                        writer.write(row);
                    }
                    CommitIncrement increment = writer.prepareCommit(false);
                    List<DataFileMeta> newFiles = increment.newFilesIncrement().newFiles();
                    CommitMessage msg =
                            new CommitMessageImpl(
                                    partition,
                                    0,
                                    null,
                                    new DataIncrement(
                                            newFiles,
                                            Collections.emptyList(),
                                            Collections.emptyList()),
                                    CompactIncrement.emptyIncrement());
                    committables.add(new Committable(checkpointId, msg));
                } finally {
                    writer.close();
                }
            }

            return committables;
        } catch (Exception e) {
            throw new IOException("Error in prepareCommit", e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (tableWrite != null) {
            tableWrite.close();
        }
    }

    private void refreshFileMetadata() {
        Long latestId = snapshotManager.latestSnapshotId();
        if (latestId == null) {
            return;
        }

        List<ManifestEntry> allEntries =
                table.store()
                        .newScan()
                        .withManifestEntryFilter(
                                entry ->
                                        entry.file().firstRowId() != null
                                                && !isBlobFile(entry.file().fileName())
                                                && !isVectorStoreFile(entry.file().fileName()))
                        .withSnapshot(latestId)
                        .plan()
                        .files();

        firstIdToFiles.clear();
        for (ManifestEntry entry : allEntries) {
            long firstRowId = entry.file().nonNullFirstRowId();
            firstIdToFiles.computeIfAbsent(firstRowId, k -> new ArrayList<>()).add(entry.file());
        }
    }

    private CommitMessage writePartialUpdate(
            BinaryRow partition, long firstRowId, TreeMap<Long, InternalRow> updates)
            throws Exception {
        List<DataFileMeta> oldFiles =
                firstIdToFiles.getOrDefault(firstRowId, Collections.emptyList());
        Preconditions.checkState(
                !oldFiles.isEmpty(),
                String.format("Cannot find files for firstRowId: %s", firstRowId));

        long rowCount = oldFiles.get(0).rowCount();

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(oldFiles)
                        .withBucketPath(
                                table.store().pathFactory().bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();

        int[] nonNullCols = computeAlwaysNonNullColumns(updates);
        boolean isPartialColumn = nonNullCols.length < fullWriteType.getFieldCount();

        ProjectedRow writeProjection;
        if (isPartialColumn) {
            RowType partialWriteType = fullWriteType.project(nonNullCols);
            tableWrite.withWriteType(partialWriteType);
            writeProjection = ProjectedRow.from(partialWriteType, readType);
        } else {
            writeProjection = projectedRow;
        }

        RecordWriter<InternalRow> writer = tableWrite.createWriter(partition, 0);
        try {
            //noinspection resource
            try (CloseableIterator<InternalRow> reader =
                    tableRead.createReader(dataSplit).toCloseableIterator()) {
                long offset = 0;
                while (reader.hasNext()) {
                    InternalRow originalRow = reader.next();
                    InternalRow updateRow = updates.get(offset);
                    if (updateRow != null) {
                        writeProjection.replaceRow(updateRow);
                    } else {
                        writeProjection.replaceRow(originalRow);
                    }
                    writer.write(writeProjection);
                    offset++;
                }

                Preconditions.checkState(
                        offset == rowCount,
                        String.format(
                                "Written num %s not equal to original row num %s",
                                offset, rowCount));
            }

            CommitIncrement written = writer.prepareCommit(false);
            List<DataFileMeta> newFiles = written.newFilesIncrement().newFiles();
            Preconditions.checkState(
                    newFiles.size() == 1, "Partial update should produce exactly one file");
            DataFileMeta newFile = newFiles.get(0).assignFirstRowId(firstRowId);

            List<DataFileMeta> deletedFiles = isPartialColumn ? Collections.emptyList() : oldFiles;
            return new CommitMessageImpl(
                    partition,
                    0,
                    null,
                    new DataIncrement(
                            Collections.singletonList(newFile),
                            deletedFiles,
                            Collections.emptyList()),
                    CompactIncrement.emptyIncrement());
        } finally {
            writer.close();
            if (isPartialColumn) {
                tableWrite.withWriteType(fullWriteType);
            }
        }
    }

    private int[] computeAlwaysNonNullColumns(TreeMap<Long, InternalRow> updates) {
        int colCount = fullWriteType.getFieldCount();
        boolean[] allNonNull = new boolean[colCount];
        Arrays.fill(allNonNull, true);
        for (InternalRow row : updates.values()) {
            for (int i = 0; i < colCount; i++) {
                if (row.isNullAt(i)) {
                    allNonNull[i] = false;
                }
            }
        }
        int count = 0;
        for (boolean b : allNonNull) {
            if (b) {
                count++;
            }
        }
        int[] result = new int[count];
        int idx = 0;
        for (int i = 0; i < colCount; i++) {
            if (allNonNull[i]) {
                result[idx++] = i;
            }
        }
        return result;
    }

    /** Factory for creating {@link UpsertWriteOperator}. */
    public static class Factory extends PrepareCommitOperator.Factory<UpsertRecord, Committable> {

        private final FileStoreTable table;

        public Factory(FileStoreTable table) {
            super(Options.fromMap(table.options()));
            this.table = table;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<Committable>> T createStreamOperator(
                StreamOperatorParameters<Committable> parameters) {
            return (T) new UpsertWriteOperator(parameters, table);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return UpsertWriteOperator.class;
        }
    }
}
