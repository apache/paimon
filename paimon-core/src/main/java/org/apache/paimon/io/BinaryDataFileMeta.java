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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Reusable binary view of a projected {@link DataFileMeta}.
 *
 * <p>The view is mutable and only valid while its backing {@link InternalRow} is valid. Accessors
 * fail explicitly when their fields were not projected. Operations which create a modified data
 * file are not supported.
 */
public final class BinaryDataFileMeta implements DataFileMeta {

    private final Projection projection;
    private @Nullable InternalRow row;

    private BinaryDataFileMeta(Projection projection) {
        this.projection = projection;
    }

    /** Replaces the backing row and returns this reusable view. */
    public BinaryDataFileMeta replace(InternalRow row) {
        checkArgument(row != null, "Data file row cannot be null.");
        if (row.getFieldCount() != projection.fieldCount) {
            throw new IllegalArgumentException(
                    String.format(
                            "Data file row field count %s does not match projected field count %s.",
                            row.getFieldCount(), projection.fieldCount));
        }
        this.row = row;
        return this;
    }

    /** Drops the reference to the current row before its reader batch is released. */
    public void clear() {
        row = null;
    }

    @Override
    public String fileName() {
        return fileNameBinary().toString();
    }

    public BinaryString fileNameBinary() {
        BinaryString fileName = currentRow().getString(requiredPosition(Fields.FILE_NAME));
        checkState(fileName != null, "Data file name cannot be null.");
        return fileName;
    }

    @Override
    public long fileSize() {
        return currentRow().getLong(requiredPosition(Fields.FILE_SIZE));
    }

    @Override
    public long rowCount() {
        return currentRow().getLong(requiredPosition(Fields.ROW_COUNT));
    }

    @Override
    public Optional<Long> deleteRowCount() {
        int position = requiredPosition(Fields.DELETE_ROW_COUNT);
        InternalRow row = currentRow();
        return row.isNullAt(position) ? Optional.empty() : Optional.of(row.getLong(position));
    }

    public boolean hasEmbeddedIndex() {
        return !currentRow().isNullAt(requiredPosition(Fields.EMBEDDED_FILE_INDEX));
    }

    @Nullable
    @Override
    public byte[] embeddedIndex() {
        int position = requiredPosition(Fields.EMBEDDED_FILE_INDEX);
        InternalRow row = currentRow();
        return row.isNullAt(position) ? null : row.getBinary(position);
    }

    @Override
    public BinaryRow minKey() {
        return deserializeBinaryRow(currentRow().getBinary(requiredPosition(Fields.MIN_KEY)));
    }

    @Override
    public BinaryRow maxKey() {
        return deserializeBinaryRow(currentRow().getBinary(requiredPosition(Fields.MAX_KEY)));
    }

    @Override
    public SimpleStats keyStats() {
        return stats(Fields.KEY_STATS);
    }

    @Override
    public SimpleStats valueStats() {
        return stats(Fields.VALUE_STATS);
    }

    @Override
    public long minSequenceNumber() {
        return currentRow().getLong(requiredPosition(Fields.MIN_SEQUENCE_NUMBER));
    }

    @Override
    public long maxSequenceNumber() {
        return currentRow().getLong(requiredPosition(Fields.MAX_SEQUENCE_NUMBER));
    }

    @Override
    public long schemaId() {
        return currentRow().getLong(requiredPosition(Fields.SCHEMA_ID));
    }

    @Override
    public int level() {
        return currentRow().getInt(requiredPosition(Fields.LEVEL));
    }

    public int extraFileCount() {
        return extraFilesArray().size();
    }

    public BinaryString extraFile(int position) {
        InternalArray extraFiles = extraFilesArray();
        checkArgument(
                position >= 0 && position < extraFiles.size(),
                "Extra file position is out of bounds.");
        checkState(!extraFiles.isNullAt(position), "Extra file name cannot be null.");
        return extraFiles.getString(position);
    }

    @Override
    public List<String> extraFiles() {
        return Collections.unmodifiableList(fromStringArrayData(extraFilesArray()));
    }

    @Override
    public Timestamp creationTime() {
        Timestamp creationTime =
                currentRow().getTimestamp(requiredPosition(Fields.CREATION_TIME), 3);
        checkState(creationTime != null, "Data file creation time cannot be null.");
        return creationTime;
    }

    @Override
    public String fileFormat() {
        String fileName = fileName();
        String[] split = fileName.split("\\.");
        if (split.length == 1) {
            throw new RuntimeException("Can't find format from file: " + fileName);
        }
        return split[split.length - 1];
    }

    public boolean hasExternalPath() {
        return !currentRow().isNullAt(requiredPosition(Fields.EXTERNAL_PATH));
    }

    public BinaryString externalPathBinary() {
        BinaryString externalPath = currentRow().getString(requiredPosition(Fields.EXTERNAL_PATH));
        checkState(externalPath != null, "External path cannot be null.");
        return externalPath;
    }

    @Override
    public Optional<String> externalPath() {
        return hasExternalPath() ? Optional.of(externalPathBinary().toString()) : Optional.empty();
    }

    @Override
    public Optional<String> externalPathDir() {
        return externalPath().map(Path::new).map(path -> path.getParent().toString());
    }

    @Override
    public Optional<FileSource> fileSource() {
        int position = requiredPosition(Fields.FILE_SOURCE);
        InternalRow row = currentRow();
        return row.isNullAt(position)
                ? Optional.empty()
                : Optional.of(FileSource.fromByteValue(row.getByte(position)));
    }

    @Nullable
    @Override
    public List<String> valueStatsCols() {
        return nullableStringArray(Fields.VALUE_STATS_COLS);
    }

    public boolean hasFirstRowId() {
        return !currentRow().isNullAt(requiredPosition(Fields.FIRST_ROW_ID));
    }

    @Nullable
    @Override
    public Long firstRowId() {
        int position = requiredPosition(Fields.FIRST_ROW_ID);
        InternalRow row = currentRow();
        return row.isNullAt(position) ? null : row.getLong(position);
    }

    @Nullable
    @Override
    public List<String> writeCols() {
        return nullableStringArray(Fields.WRITE_COLS);
    }

    public boolean containsWriteColumn(BinaryString fieldName) {
        int position = requiredPosition(Fields.WRITE_COLS);
        InternalRow row = currentRow();
        if (row.isNullAt(position)) {
            return false;
        }
        InternalArray writeColumns = row.getArray(position);
        checkState(writeColumns != null, "Data file write columns cannot be null.");
        for (int i = 0; i < writeColumns.size(); i++) {
            checkState(!writeColumns.isNullAt(i), "Data file write column cannot be null.");
            if (fieldName.equals(writeColumns.getString(i))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DataFileMeta upgrade(int newLevel) {
        throw unsupportedOperation("upgrade(int)");
    }

    @Override
    public DataFileMeta rename(String newFileName) {
        throw unsupportedOperation("rename(String)");
    }

    @Override
    public DataFileMeta copyWithoutStats() {
        throw unsupportedOperation("copyWithoutStats()");
    }

    @Override
    public DataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        throw unsupportedOperation("assignSequenceNumber(long, long)");
    }

    @Override
    public DataFileMeta assignFirstRowId(long firstRowId) {
        throw unsupportedOperation("assignFirstRowId(long)");
    }

    @Override
    public DataFileMeta newFirstRowId(@Nullable Long newFirstRowId) {
        throw unsupportedOperation("newFirstRowId(Long)");
    }

    @Override
    public DataFileMeta copy(List<String> newExtraFiles) {
        throw unsupportedOperation("copy(List)");
    }

    @Override
    public DataFileMeta newExternalPath(String newExternalPath) {
        throw unsupportedOperation("newExternalPath(String)");
    }

    @Override
    public DataFileMeta copy(byte[] newEmbeddedIndex) {
        throw unsupportedOperation("copy(byte[])");
    }

    private SimpleStats stats(int fieldIndex) {
        InternalRow stats =
                currentRow()
                        .getRow(requiredPosition(fieldIndex), SimpleStats.SCHEMA.getFieldCount());
        checkState(stats != null, "Data file stats cannot be null.");
        return SimpleStats.fromRow(stats);
    }

    private InternalArray extraFilesArray() {
        InternalArray extraFiles = currentRow().getArray(requiredPosition(Fields.EXTRA_FILES));
        checkState(extraFiles != null, "Data file extra files cannot be null.");
        return extraFiles;
    }

    @Nullable
    private List<String> nullableStringArray(int fieldIndex) {
        int position = requiredPosition(fieldIndex);
        InternalRow row = currentRow();
        return row.isNullAt(position) ? null : fromStringArrayData(row.getArray(position));
    }

    private InternalRow currentRow() {
        checkState(row != null, "Binary data file is not backed by a row.");
        return row;
    }

    private int requiredPosition(int fieldIndex) {
        int position = projection.fieldPositions[fieldIndex];
        if (position < 0) {
            throw unsupportedField(SCHEMA.getFields().get(fieldIndex).name());
        }
        return position;
    }

    private static UnsupportedOperationException unsupportedField(String fieldName) {
        return new UnsupportedOperationException(
                String.format(
                        "The selected binary data file projection does not contain %s.",
                        fieldName));
    }

    private static UnsupportedOperationException unsupportedOperation(String operation) {
        return new UnsupportedOperationException(
                String.format("Binary data file does not support %s.", operation));
    }

    private static int fieldIndex(String fieldName) {
        return SCHEMA.getFieldIndex(fieldName);
    }

    private static class Fields {

        private static final int FILE_NAME = fieldIndex(DataFileMeta.FILE_NAME);
        private static final int FILE_SIZE = fieldIndex(DataFileMeta.FILE_SIZE);
        private static final int ROW_COUNT = fieldIndex(DataFileMeta.ROW_COUNT);
        private static final int MIN_KEY = fieldIndex(DataFileMeta.MIN_KEY);
        private static final int MAX_KEY = fieldIndex(DataFileMeta.MAX_KEY);
        private static final int KEY_STATS = fieldIndex(DataFileMeta.KEY_STATS);
        private static final int VALUE_STATS = fieldIndex(DataFileMeta.VALUE_STATS);
        private static final int MIN_SEQUENCE_NUMBER = fieldIndex(DataFileMeta.MIN_SEQUENCE_NUMBER);
        private static final int MAX_SEQUENCE_NUMBER = fieldIndex(DataFileMeta.MAX_SEQUENCE_NUMBER);
        private static final int SCHEMA_ID = fieldIndex(DataFileMeta.SCHEMA_ID);
        private static final int LEVEL = fieldIndex(DataFileMeta.LEVEL);
        private static final int EXTRA_FILES = fieldIndex(DataFileMeta.EXTRA_FILES);
        private static final int CREATION_TIME = fieldIndex(DataFileMeta.CREATION_TIME);
        private static final int DELETE_ROW_COUNT = fieldIndex(DataFileMeta.DELETE_ROW_COUNT);
        private static final int EMBEDDED_FILE_INDEX = fieldIndex(DataFileMeta.EMBEDDED_FILE_INDEX);
        private static final int FILE_SOURCE = fieldIndex(DataFileMeta.FILE_SOURCE);
        private static final int VALUE_STATS_COLS = fieldIndex(DataFileMeta.VALUE_STATS_COLS);
        private static final int EXTERNAL_PATH = fieldIndex(DataFileMeta.EXTERNAL_PATH);
        private static final int FIRST_ROW_ID = fieldIndex(DataFileMeta.FIRST_ROW_ID);
        private static final int WRITE_COLS = fieldIndex(DataFileMeta.WRITE_COLS);
    }

    /** Projected data-file schema together with its bound binary field layout. */
    public static final class Projection {

        private final int fieldCount;
        private final int[] fieldPositions;

        private Projection(int fieldCount, int[] fieldPositions) {
            this.fieldCount = fieldCount;
            this.fieldPositions = fieldPositions;
        }

        public static Projection create(RowType projectedType) {
            checkArgument(projectedType != null, "Projected data file type cannot be null.");
            int[] fieldPositions = new int[SCHEMA.getFieldCount()];
            Arrays.fill(fieldPositions, -1);
            for (int i = 0; i < projectedType.getFieldCount(); i++) {
                DataField projectedField = projectedType.getFields().get(i);
                checkArgument(
                        SCHEMA.containsField(projectedField.id()),
                        "Unknown projected data file field '%s' (id %s).",
                        projectedField.name(),
                        projectedField.id());
                DataField dataFileField = SCHEMA.getField(projectedField.id());
                checkArgument(
                        projectedField.equals(dataFileField),
                        "Projected data file field '%s' does not match %s.",
                        projectedField.name(),
                        dataFileField);
                fieldPositions[SCHEMA.getFieldIndexByFieldId(projectedField.id())] = i;
            }
            return new Projection(projectedType.getFieldCount(), fieldPositions);
        }

        public BinaryDataFileMeta createDataFile() {
            return new BinaryDataFileMeta(this);
        }
    }
}
