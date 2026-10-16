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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobArrayPlaceholder;
import org.apache.paimon.data.BlobMapPlaceholder;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
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
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobDescriptorResolvingRow;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.UriReaderFactory;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/**
 * The Flink Batch Operator to process sorted new rows for data-evolution partial write. It assumes
 * that input data has already been shuffled by firstRowId and sorted by rowId.
 */
public class DataEvolutionPartialWriteOperator
        extends BoundedOneInputOperator<InternalRow, Committable> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionPartialWriteOperator.class);

    private final FileStoreTable table;
    private final Long baseSnapshotId;
    private final UriReaderFactory blobDescriptorReaderFactory;
    private final boolean nestedFieldEnabled;

    // dataType
    private final RowType dataType;
    private final int rowIdIndex;

    // data type excludes of _ROW_ID field.
    private final RowType writeType;

    // Original-file read type. Raw blob columns are omitted so gap rows do not pull payloads.
    private final RowType originalReadType;
    private final int originalRowIdIndex;
    private final boolean hasRawBlobFields;
    // writeType index -> originalReadType index, or -1 when the field is a raw blob placeholder.
    private final int[] rawBlobGapMapping;
    private final Object[] rawBlobGapPlaceholders;

    // --------------------- transient fields ---------------------------

    private transient List<Committable> committables;

    // first-row-id related fields
    private transient FirstRowIdLookup firstRowIdLookup;
    private transient Map<Long, BinaryRow> firstIdToPartition;
    private transient Map<Long, List<DataFileMeta>> firstIdToFiles;

    private transient InnerTableRead tableRead;
    private transient AbstractFileStoreWrite<InternalRow> tableWrite;
    private transient Writer writer;

    private static Map<String, String> dataEvolutionWriteOptions() {
        // Data evolution requires a single output file: disable both size and row rolling.
        // Append buffering serializes the row before the blob writer sees it. Placeholders cannot
        // be serialized, and a buffer flush can close the file before this range is finished.
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.TARGET_FILE_SIZE.key(), "99999 G");
        options.put(CoreOptions.TARGET_FILE_ROW_NUM.key(), String.valueOf(Long.MAX_VALUE));
        options.put(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), "false");
        return options;
    }

    public DataEvolutionPartialWriteOperator(
            FileStoreTable table,
            RowType sourceType,
            List<String> writePaths,
            Long baseSnapshotId,
            UriReaderFactory blobDescriptorReaderFactory) {
        this.table = table.copy(dataEvolutionWriteOptions());
        this.baseSnapshotId = baseSnapshotId;
        this.blobDescriptorReaderFactory = blobDescriptorReaderFactory;
        this.nestedFieldEnabled = this.table.coreOptions().dataEvolutionNestedFieldEnabled();
        this.writeType =
                nestedFieldEnabled
                        ? table.rowType().projectByPaths(writePaths)
                        : table.rowType().project(writePaths);
        // sourceType is already pruned to the written columns (with partial nested structs) and
        // carries the table's field ids, so it is used directly as the read/data type.
        this.dataType = sourceType;
        this.rowIdIndex = this.dataType.getFieldIndex(SpecialFields.ROW_ID.name());
        Set<String> rawBlobFields = rawBlobFieldNames(this.writeType, this.table.coreOptions());
        this.hasRawBlobFields = !rawBlobFields.isEmpty();
        this.originalReadType = readTypeWithoutRawBlobs(this.dataType, rawBlobFields);
        this.originalRowIdIndex = this.originalReadType.getFieldIndex(SpecialFields.ROW_ID.name());
        this.rawBlobGapMapping = new int[this.writeType.getFieldCount()];
        this.rawBlobGapPlaceholders = new Object[this.writeType.getFieldCount()];
        for (int i = 0; i < this.writeType.getFieldCount(); i++) {
            DataField field = this.writeType.getFields().get(i);
            if (rawBlobFields.contains(field.name())) {
                this.rawBlobGapMapping[i] = -1;
                this.rawBlobGapPlaceholders[i] = rawBlobPlaceholder(field.type());
            } else {
                int originalIndex = this.originalReadType.getFieldIndex(field.name());
                Preconditions.checkState(
                        originalIndex >= 0,
                        "Partial write field '%s' is missing from the original read type.",
                        field.name());
                this.rawBlobGapMapping[i] = originalIndex;
                this.rawBlobGapPlaceholders[i] = null;
            }
        }
    }

    private static Set<String> rawBlobFieldNames(RowType writeType, CoreOptions options) {
        Set<String> inlineFields = options.blobInlineField();
        Set<String> rawBlobFields = new HashSet<>();
        for (DataField field : writeType.getFields()) {
            if (BlobType.isBlobFileField(field.type()) && !inlineFields.contains(field.name())) {
                rawBlobFields.add(field.name());
            }
        }
        return rawBlobFields;
    }

    private static RowType readTypeWithoutRawBlobs(RowType dataType, Set<String> rawBlobFields) {
        if (rawBlobFields.isEmpty()) {
            return dataType;
        }
        List<DataField> fields = new ArrayList<>();
        for (DataField field : dataType.getFields()) {
            if (!rawBlobFields.contains(field.name())) {
                fields.add(field);
            }
        }
        return new RowType(fields);
    }

    private static Object rawBlobPlaceholder(DataType type) {
        if (type.getTypeRoot() == DataTypeRoot.ARRAY) {
            return BlobArrayPlaceholder.INSTANCE;
        }
        if (type.getTypeRoot() == DataTypeRoot.MAP) {
            return BlobMapPlaceholder.INSTANCE;
        }
        return BlobPlaceholder.INSTANCE;
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
                                                && !isBlobFile(entry.file().fileName())
                                                && !isVectorStoreFile(entry.file().fileName()))
                        .withSnapshot(baseSnapshotId)
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
        // The split built below contains only non-blob files. Raw blob columns must be removed
        // from the read type; leaving them in would read null or fail. Unchanged rows are written
        // as placeholders and resolved from the previous blob file.
        tableRead = table.newRead().withReadType(originalReadType);
        @SuppressWarnings("unchecked")
        TableWriteImpl<InternalRow> writeImpl =
                (TableWriteImpl<InternalRow>)
                        table.newBatchWriteBuilder().newWrite().withWriteType(writeType);
        tableWrite = (AbstractFileStoreWrite<InternalRow>) (writeImpl.getWrite());

        committables = new ArrayList<>();
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

        private final ProjectedRow updatedRow;
        private final BlobDescriptorResolvingRow descriptorRow;
        private final RawBlobGapRow gapRow;
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
            this.updatedRow = ProjectedRow.from(writeType, dataType);
            // Sort serializes descriptor BLOBs as bytes and BinaryRow.getBlob reopens them with a
            // null FileIO. Reattach the catalog-aware reader before the blob writer copies bytes.
            // The null index set is intentional: resolve every column in source index space.
            // ProjectedRow wraps this row, so its projected indexes must not be passed here.
            this.descriptorRow = new BlobDescriptorResolvingRow(blobDescriptorReaderFactory, null);
            this.gapRow =
                    hasRawBlobFields
                            ? new RawBlobGapRow(rawBlobGapMapping, rawBlobGapPlaceholders)
                            : null;
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            this.partition = firstIdToPartition.get(firstRowId);
        }

        private InternalRow rowForOriginal(InternalRow originalRow) {
            if (gapRow == null) {
                return updatedRow.replaceRow(originalRow);
            }
            return gapRow.replace(originalRow);
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
                long originalRowId = originalRow.getLong(originalRowIdIndex);

                if (originalRowId < currentRowId) {
                    // Unchanged row. Raw blob fields are placeholders, not copied payloads.
                    writer.write(rowForOriginal(originalRow));
                    writtenNum++;
                } else if (originalRowId == currentRowId) {
                    // new row is present, we should use the new row.
                    // descriptorRow is inside ProjectedRow and sees unprojected source indexes.
                    updatedRow.replaceRow(descriptorRow.replace(row));
                    writer.write(updatedRow);
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
                    writer.write(rowForOriginal(row));
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
            try {
                CommitIncrement written = writer.prepareCommit(false);
                List<DataFileMeta> fileMetas =
                        assignFirstRowIds(written.newFilesIncrement().newFiles(), firstRowId);

                CommitMessage commitMessage =
                        new CommitMessageImpl(
                                        partition,
                                        0,
                                        null,
                                        new DataIncrement(
                                                fileMetas,
                                                Collections.emptyList(),
                                                Collections.emptyList()),
                                        CompactIncrement.emptyIncrement())
                                .withCheckFromSnapshot(baseSnapshotId);

                return new Committable(Long.MAX_VALUE, commitMessage);
            } finally {
                writer.close();
            }
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

    /**
     * Assigns first row ids for a partial write. A blob-only update produces blob files and no
     * normal file. A mixed update produces at most one normal file plus the dedicated blob files.
     * Blob files for the same column are ordered and cover the target row-id range in sequence.
     */
    private static List<DataFileMeta> assignFirstRowIds(
            List<DataFileMeta> dataFiles, long firstRowId) {
        if (dataFiles.isEmpty()) {
            throw new IllegalStateException(
                    "This is a bug, data-evolution partial write produced no files.");
        }
        List<DataFileMeta> assigned = new ArrayList<>(dataFiles.size());
        Map<String, Long> blobFieldStarts = new HashMap<>();
        int normalFileCount = 0;
        long vectorStoreStart = firstRowId;
        for (DataFileMeta file : dataFiles) {
            if (isBlobFile(file.fileName())) {
                String blobFieldName = file.writeCols().get(0);
                long blobStart =
                        blobFieldStarts.containsKey(blobFieldName)
                                ? blobFieldStarts.get(blobFieldName)
                                : firstRowId;
                assigned.add(file.assignFirstRowId(blobStart));
                blobFieldStarts.put(blobFieldName, blobStart + file.rowCount());
            } else if (isVectorStoreFile(file.fileName())) {
                assigned.add(file.assignFirstRowId(vectorStoreStart));
                vectorStoreStart += file.rowCount();
            } else {
                normalFileCount++;
                if (normalFileCount > 1) {
                    throw new IllegalStateException(
                            "This is a bug: data-evolution partial write should produce at most one "
                                    + "normal file, but produced "
                                    + normalFileCount
                                    + " files. Files: "
                                    + dataFiles);
                }
                assigned.add(file.assignFirstRowId(firstRowId));
            }
        }
        return assigned;
    }

    /**
     * Gap row for a managed BLOB partial write. Non-blob fields are copied from the original file.
     * Raw blob fields are placeholders so unchanged payloads stay in the previous blob file.
     */
    private static final class RawBlobGapRow implements InternalRow {

        private final int[] indexMapping;
        private final Object[] placeholders;
        private InternalRow row;

        private RawBlobGapRow(int[] indexMapping, Object[] placeholders) {
            this.indexMapping = indexMapping;
            this.placeholders = placeholders;
        }

        private RawBlobGapRow replace(InternalRow row) {
            this.row = row;
            return this;
        }

        @Override
        public int getFieldCount() {
            return indexMapping.length;
        }

        @Override
        public RowKind getRowKind() {
            return row.getRowKind();
        }

        @Override
        public void setRowKind(RowKind kind) {
            row.setRowKind(kind);
        }

        @Override
        public boolean isNullAt(int pos) {
            if (indexMapping[pos] < 0) {
                return false;
            }
            return row.isNullAt(indexMapping[pos]);
        }

        @Override
        public boolean getBoolean(int pos) {
            return row.getBoolean(indexMapping[pos]);
        }

        @Override
        public byte getByte(int pos) {
            return row.getByte(indexMapping[pos]);
        }

        @Override
        public short getShort(int pos) {
            return row.getShort(indexMapping[pos]);
        }

        @Override
        public int getInt(int pos) {
            return row.getInt(indexMapping[pos]);
        }

        @Override
        public long getLong(int pos) {
            return row.getLong(indexMapping[pos]);
        }

        @Override
        public float getFloat(int pos) {
            return row.getFloat(indexMapping[pos]);
        }

        @Override
        public double getDouble(int pos) {
            return row.getDouble(indexMapping[pos]);
        }

        @Override
        public BinaryString getString(int pos) {
            return row.getString(indexMapping[pos]);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return row.getDecimal(indexMapping[pos], precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return row.getTimestamp(indexMapping[pos], precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return row.getBinary(indexMapping[pos]);
        }

        @Override
        public Variant getVariant(int pos) {
            return row.getVariant(indexMapping[pos]);
        }

        @Override
        public Blob getBlob(int pos) {
            if (indexMapping[pos] < 0) {
                return (Blob) placeholders[pos];
            }
            return row.getBlob(indexMapping[pos]);
        }

        @Override
        public InternalArray getArray(int pos) {
            if (indexMapping[pos] < 0) {
                return (InternalArray) placeholders[pos];
            }
            return row.getArray(indexMapping[pos]);
        }

        @Override
        public InternalVector getVector(int pos) {
            return row.getVector(indexMapping[pos]);
        }

        @Override
        public InternalMap getMap(int pos) {
            if (indexMapping[pos] < 0) {
                return (InternalMap) placeholders[pos];
            }
            return row.getMap(indexMapping[pos]);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return row.getRow(indexMapping[pos], numFields);
        }
    }
}
