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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RecordWriter;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;

/**
 * The Flink Batch Operator to process sorted new rows for data-evolution partial write. It assumes
 * that input data has already been shuffled by firstRowId and sorted by rowId.
 */
public class DataEvolutionPartialWriteOperator
        extends BoundedOneInputOperator<InternalRow, Committable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionPartialWriteOperator.class);

    private final FileStoreTable table;

    // dataType
    private final RowType dataType;
    private final InternalRow.FieldGetter[] fieldGetters;
    private final int rowIdIndex;

    // data type excludes of _ROW_ID field.
    private final RowType writeType;

    private List<Committable> committables = new ArrayList<>();

    // --------------------- transient fields ---------------------------

    // first-row-id related fields
    private transient FirstRowIdLookup firstRowIdLookup;
    private transient Map<Long, BinaryRow> firstIdToPartition;
    private transient Map<Long, List<DataFileMeta>> firstIdToFiles;

    private transient InnerTableRead tableRead;
    private transient AbstractFileStoreWrite<InternalRow> tableWrite;
    private transient Writer writer;

    public DataEvolutionPartialWriteOperator(FileStoreTable table, RowType dataType) {
        this.table = table;
        List<String> fieldNames =
                dataType.getFieldNames().stream()
                        .filter(name -> !SpecialFields.ROW_ID.name().equals(name))
                        .collect(Collectors.toList());
        this.writeType = table.rowType().project(fieldNames);
        this.dataType =
                SpecialFields.rowTypeWithRowId(table.rowType()).project(dataType.getFieldNames());
        this.rowIdIndex = this.dataType.getFieldIndex(SpecialFields.ROW_ID.name());
        this.fieldGetters = new InternalRow.FieldGetter[dataType.getFieldCount()];
        List<DataField> fields = this.dataType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            this.fieldGetters[i] = InternalRow.createFieldGetter(fields.get(i).type(), i);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        // initialize row-id-related data structures
        TreeSet<Long> rowIdSet = new TreeSet<>();
        firstIdToPartition = new HashMap<>();
        firstIdToFiles = new HashMap<>();

        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withManifestEntryFilter(
                                entry ->
                                        entry.file().firstRowId() != null
                                                && !isBlobFile(entry.file().fileName()))
                        .plan()
                        .files();

        for (ManifestEntry entry : entries) {
            DataFileMeta fileMeta = entry.file();
            long firstRowId = fileMeta.nonNullFirstRowId();

            rowIdSet.add(firstRowId);
            firstIdToFiles.computeIfAbsent(firstRowId, k -> new ArrayList<>()).add(fileMeta);
            firstIdToPartition.put(firstRowId, entry.partition());
        }
        firstRowIdLookup = new FirstRowIdLookup(new ArrayList<>(rowIdSet));

        // initialize table read & table write
        tableRead = table.newRead().withReadType(dataType);
        @SuppressWarnings("unchecked")
        TableWriteImpl<InternalRow> writeImpl =
                (TableWriteImpl<InternalRow>)
                        table.newBatchWriteBuilder().newWrite().withWriteType(writeType);
        tableWrite = (AbstractFileStoreWrite<InternalRow>) (writeImpl.getWrite());
    }

    @Override
    public void endInput() throws Exception {
        finishWriter();

        for (Committable committable : committables) {
            output.collect(new StreamRecord<>(committable));
        }
    }

    @Override
    public void processElement(StreamRecord<InternalRow> element) throws Exception {
        InternalRow row = element.getValue();
        long rowId = row.getLong(rowIdIndex);

        if (writer == null || !writer.contains(rowId)) {
            finishWriter();
            writer = createWriter(firstRowIdLookup.lookup(rowId));
        }

        writer.write(row);
    }

    private void finishWriter() throws Exception {
        if (writer != null) {
            committables.add(writer.finish());
        }
        writer = null;
    }

    private Writer createWriter(long firstRowId) throws IOException {
        LOG.debug("Creating writer for row id {}", firstRowId);

        BinaryRow partition = firstIdToPartition.get(firstRowId);
        Preconditions.checkNotNull(
                partition,
                String.format("Cannot find the partition for firstRowId: %s ", firstRowId));

        List<DataFileMeta> files = firstIdToFiles.get(firstRowId);
        Preconditions.checkState(
                files != null && !files.isEmpty(),
                String.format("Cannot find files for firstRowId: %s", firstRowId));

        long rowCount = files.get(0).rowCount();

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(files)
                        .withBucketPath(
                                table.store().pathFactory().bucketPath(partition, 0).toString())
                        .rawConvertible(false)
                        .build();

        return new Writer(
                tableRead.createReader(dataSplit),
                tableWrite.createWriter(partition, 0),
                firstRowId,
                rowCount);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (tableWrite != null) {
            tableWrite.close();
        }
    }

    /**
     * The writer to write partial columns for a single file. The written new file should be aligned
     * with existing ones.
     */
    private class Writer {
        // reader for original data
        private final CloseableIterator<InternalRow> reader;

        // writer to write new columns
        private final RecordWriter<InternalRow> writer;

        private final ProjectedRow reusedRow;
        private final long firstRowId;
        private final long rowCount;
        private final BinaryRow partition;

        private long writtenNum = 0;
        private long prevRowId = -1;

        Writer(
                RecordReader<InternalRow> reader,
                RecordWriter<InternalRow> writer,
                long firstRowId,
                long rowCount) {
            this.reader = reader.toCloseableIterator();
            this.writer = writer;
            this.reusedRow = ProjectedRow.from(writeType, dataType);
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.partition = firstIdToPartition.get(firstRowId);
        }

        void write(InternalRow row) throws Exception {
            long currentRowId = row.getLong(rowIdIndex);

            // for rows with duplicated _ROW_ID, just choose the first one.
            if (checkDuplication(currentRowId)) {
                return;
            }

            if (!reader.hasNext()) {
                throw new IllegalStateException(
                        "New file should be aligned with original file, it's a bug.");
            }

            InternalRow originalRow;
            while (reader.hasNext()) {
                originalRow = reader.next();
                long originalRowId = originalRow.getLong(rowIdIndex);

                if (originalRowId < currentRowId) {
                    // new row is absent, we should use the original row
                    reusedRow.replaceRow(originalRow);
                    writer.write(reusedRow);
                    writtenNum++;
                } else if (originalRowId == currentRowId) {
                    // new row is present, we should use the new row
                    reusedRow.replaceRow(row);
                    writer.write(reusedRow);
                    writtenNum++;
                    break;
                } else {
                    // original row id > new row id, this means there are duplicated row ids
                    // in the input rows, it cannot happen here.
                    throw new IllegalStateException("Duplicated row id " + currentRowId);
                }
            }
        }

        private Committable finish() throws Exception {
            // 1. write remaining original rows
            try {
                InternalRow row;
                while (reader.hasNext()) {
                    row = reader.next();
                    reusedRow.replaceRow(row);
                    writer.write(reusedRow);
                    writtenNum++;
                }
            } finally {
                reader.close();
            }

            Preconditions.checkState(
                    writtenNum == rowCount,
                    String.format(
                            "Written num %s not equal to original row num %s, it's a bug.",
                            writtenNum, rowCount));

            // 2. finish writer
            CommitIncrement written = writer.prepareCommit(false);
            List<DataFileMeta> fileMetas = written.newFilesIncrement().newFiles();
            Preconditions.checkState(
                    fileMetas.size() == 1, "This is a bug, Writer could only produce one file");
            DataFileMeta fileMeta = fileMetas.get(0).assignFirstRowId(firstRowId);

            CommitMessage commitMessage =
                    new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            new DataIncrement(
                                    Collections.singletonList(fileMeta),
                                    Collections.emptyList(),
                                    Collections.emptyList()),
                            CompactIncrement.emptyIncrement());

            return new Committable(Long.MAX_VALUE, commitMessage);
        }

        private boolean contains(long rowId) {
            return rowId >= firstRowId && rowId < firstRowId + rowCount;
        }

        private boolean checkDuplication(long rowId) {
            if (prevRowId == rowId) {
                return true;
            }
            prevRowId = rowId;
            return false;
        }
    }
}
