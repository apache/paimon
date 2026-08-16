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

package org.apache.paimon.eslib.index;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.FileIOUtils;

import org.elasticsearch.eslib.api.ESIndexBuilder;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Multi-column writer that builds ESLib (Lucene-based) indexes. Accepts InternalRow with all
 * indexed fields and produces a single archive file.
 */
public class ESIndexGlobalIndexWriter
        implements GlobalIndexSingleColumnWriter, GlobalIndexMultiColumnWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "es-index";

    private final GlobalIndexFileWriter fileWriter;
    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final ESIndexBuilder builder;
    private long docCount;
    private Throwable writeFailure;
    private boolean finished;
    private boolean closed;

    public ESIndexGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, List<DataField> fields, ESIndexOptions indexOptions)
            throws IOException {
        this.fileWriter = Objects.requireNonNull(fileWriter, "fileWriter");
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("ES index requires at least one field");
        }
        this.indexOptions = Objects.requireNonNull(indexOptions, "indexOptions");
        List<DataField> fieldsCopy = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            DataField nonNullField = Objects.requireNonNull(field, "field");
            if (indexOptions.getConfig(nonNullField.name()) == null) {
                throw new IllegalArgumentException(
                        "Missing es-index configuration for field '" + nonNullField.name() + "'");
            }
            fieldsCopy.add(nonNullField);
        }
        this.fields = Collections.unmodifiableList(fieldsCopy);
        this.builder = ESIndexBuilderFactory.create(indexOptions.getFieldConfigs());
        this.docCount = 0;
        this.finished = false;
        this.closed = false;
    }

    @Override
    public void write(Object fieldData, long relativeRowId) {
        checkWritable();
        try {
            // Use the builder-supplied shard-relative row id as the Lucene docId (read side maps
            // _ROW_ID = rangeFrom + docId). finishDocument pads any skipped positions, so the
            // logical row count is the highest finished id plus one rather than the number of
            // write() calls.
            long docId = relativeRowId;
            DataField field = fields.get(0);
            FieldIndexConfig config = indexOptions.getConfig(field.name());
            if (fieldData == null) {
                // Null value: explicitly occupy this row's slot with an empty doc so
                // docId<->rowId stays dense; the absent field marks it null.
                // flushPendingDocs
                // is the back-stop.
                builder.addNullDoc(docId);
                builder.finishDocument(docId);
                docCount = docId + 1;
                return;
            }
            writeArrayPresence(field, docId);
            writeSingleField(fieldData, field, config, docId);
            builder.finishDocument(docId);
            docCount = docId + 1;
        } catch (IOException e) {
            RuntimeException failure =
                    new RuntimeException("Failed to write document to ES index", e);
            writeFailure = failure;
            throw failure;
        } catch (RuntimeException | Error e) {
            writeFailure = e;
            throw e;
        }
    }

    @Override
    public void write(long rowId, InternalRow row) {
        writeRow(rowId, row);
    }

    public void writeRow(long rowId, InternalRow row) {
        checkWritable();
        try {
            // Use the shard-relative rowId supplied by the builder as the Lucene docId; the read
            // side reconstructs the absolute id as `_ROW_ID = rangeFrom + docId`. Count every row.
            long docId = rowId;
            if (row == null) {
                builder.addNullDoc(docId);
                builder.finishDocument(docId);
                docCount = docId + 1;
                return;
            }
            boolean wroteAny = false;
            for (int i = 0; i < fields.size(); i++) {
                if (row.isNullAt(i)) {
                    continue;
                }
                DataField field = fields.get(i);
                FieldIndexConfig config = indexOptions.getConfig(field.name());
                writeArrayPresence(field, docId);
                writeField(row, i, field, config, docId);
                wroteAny = true;
            }
            if (!wroteAny) {
                // Every indexed field is null for this row: explicitly occupy its slot with an
                // empty
                // doc so docId<->rowId stays dense (incl. trailing all-null rows). flushPendingDocs
                // is the back-stop.
                builder.addNullDoc(docId);
            }
            builder.finishDocument(docId);
            docCount = docId + 1;
        } catch (IOException e) {
            RuntimeException failure =
                    new RuntimeException("Failed to write document to ES index", e);
            writeFailure = failure;
            throw failure;
        } catch (RuntimeException | Error e) {
            writeFailure = e;
            throw e;
        }
    }

    private void writeSingleField(
            Object fieldData, DataField field, FieldIndexConfig config, long docId)
            throws IOException {
        switch (config.indexType()) {
            case VECTOR:
                float[] vector = null;
                if (fieldData instanceof float[]) {
                    vector = (float[]) fieldData;
                } else if (fieldData instanceof InternalArray) {
                    vector = materializeVector((InternalArray) fieldData, field.name());
                }
                if (vector != null) {
                    validateVectorDimension(field.name(), vector, config);
                    builder.addVector(field.name(), docId, vector);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported vector value for field '"
                                    + field.name()
                                    + "': "
                                    + fieldData.getClass().getName());
                }
                break;
            case FULLTEXT:
                String singleText = fieldData.toString();
                builder.addTextField(field.name(), docId, singleText);
                writeKeywordSubField(field.name(), docId, singleText);
                break;
            case KEYWORD:
                String singleKeyword = fieldData.toString();
                builder.addScalarField(
                        field.name(),
                        docId,
                        singleKeyword,
                        org.elasticsearch.eslib.api.model.ScalarFieldType.KEYWORD);
                writeFullTextSubField(field.name(), docId, singleKeyword);
                break;
            case SCALAR:
            case DATE:
                Object value = extractScalar(fieldData, field.type());
                if (value != null) {
                    builder.addScalarField(field.name(), docId, value, config.scalarType());
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported es-index type "
                                + config.indexType()
                                + " for field '"
                                + field.name()
                                + "'.");
        }
    }

    private void writeField(
            InternalRow row, int pos, DataField field, FieldIndexConfig config, long docId)
            throws IOException {
        switch (config.indexType()) {
            case VECTOR:
                float[] vector = extractVector(row, pos, field);
                if (vector != null) {
                    validateVectorDimension(field.name(), vector, config);
                    builder.addVector(field.name(), docId, vector);
                }
                break;
            case FULLTEXT:
                String text = row.getString(pos).toString();
                builder.addTextField(field.name(), docId, text);
                writeKeywordSubField(field.name(), docId, text);
                break;
            case KEYWORD:
                String keyword = row.getString(pos).toString();
                builder.addScalarField(
                        field.name(),
                        docId,
                        keyword,
                        org.elasticsearch.eslib.api.model.ScalarFieldType.KEYWORD);
                writeFullTextSubField(field.name(), docId, keyword);
                break;
            case SCALAR:
            case DATE:
                Object value = extractScalar(row, pos, field.type());
                if (value != null) {
                    builder.addScalarField(field.name(), docId, value, config.scalarType());
                }
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported es-index type "
                                + config.indexType()
                                + " for field '"
                                + field.name()
                                + "'.");
        }
    }

    private void writeArrayPresence(DataField field, long docId) throws IOException {
        if (!(field.type() instanceof ArrayType)) {
            return;
        }
        String presenceField = indexOptions.arrayPresenceField(field.name());
        if (presenceField != null) {
            builder.addScalarField(
                    presenceField, docId, 1, org.elasticsearch.eslib.api.model.ScalarFieldType.INT);
        }
    }

    private static void validateVectorDimension(
            String fieldName, float[] vector, FieldIndexConfig config) {
        if (vector.length != config.dimension()) {
            throw new IllegalArgumentException(
                    "Vector field '"
                            + fieldName
                            + "' expects dimension "
                            + config.dimension()
                            + " but received "
                            + vector.length
                            + ".");
        }
    }

    /**
     * Multi-field: when a FULLTEXT column has a keyword sub-field configured, write the raw value
     * into {@code <field>.keyword} as an exact keyword term so exact filters can use it while
     * full-text match uses the analyzed primary field. No-op when the sub-field is disabled.
     */
    private void writeKeywordSubField(String fieldName, long docId, String value)
            throws IOException {
        String subField = indexOptions.keywordSubField(fieldName);
        if (subField != null) {
            builder.addScalarField(
                    subField,
                    docId,
                    value,
                    org.elasticsearch.eslib.api.model.ScalarFieldType.KEYWORD);
        }
    }

    /** Writes the analyzed multi-field paired with a KEYWORD primary field. */
    private void writeFullTextSubField(String fieldName, long docId, String value)
            throws IOException {
        String subField = indexOptions.fullTextSubField(fieldName);
        if (subField != null) {
            builder.addTextField(subField, docId, value);
        }
    }

    private float[] extractVector(InternalRow row, int pos, DataField field) {
        InternalArray vector =
                field.type() instanceof VectorType ? row.getVector(pos) : row.getArray(pos);
        return materializeVector(vector, field.name());
    }

    private static float[] materializeVector(InternalArray vector, String fieldName) {
        float[] values = new float[vector.size()];
        for (int i = 0; i < vector.size(); i++) {
            if (vector.isNullAt(i)) {
                throw new IllegalArgumentException(
                        "Vector field '"
                                + fieldName
                                + "' must not contain null elements; found null at position "
                                + i
                                + ".");
            }
            values[i] = vector.getFloat(i);
        }
        return values;
    }

    private Object extractScalar(InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case ARRAY:
                return extractScalarArray(row.getArray(pos), (ArrayType) type);
            case INTEGER:
                return row.getInt(pos);
            case SMALLINT:
                return (int) row.getShort(pos);
            case TINYINT:
                return (int) row.getByte(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos).toString();
            case DATE:
                // DATE is stored as an int day-count; TIME as an int millis-of-day.
            case TIME_WITHOUT_TIME_ZONE:
                return (long) row.getInt(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return row.getTimestamp(pos, ((TimestampType) type).getPrecision())
                        .getMillisecond();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return row.getTimestamp(pos, ((LocalZonedTimestampType) type).getPrecision())
                        .getMillisecond();
            default:
                return row.getString(pos).toString();
        }
    }

    private Object extractScalar(Object fieldData, DataType type) {
        if (type instanceof ArrayType && fieldData instanceof InternalArray) {
            return extractScalarArray((InternalArray) fieldData, (ArrayType) type);
        }
        switch (type.getTypeRoot()) {
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return fieldData instanceof Number ? ((Number) fieldData).longValue() : fieldData;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (fieldData instanceof Timestamp) {
                    return ((Timestamp) fieldData).getMillisecond();
                }
                if (fieldData instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) fieldData).getTime();
                }
                return fieldData instanceof Number ? ((Number) fieldData).longValue() : fieldData;
            default:
                break;
        }
        return fieldData;
    }

    private Object extractScalarArray(InternalArray array, ArrayType type) {
        Object[] values = new Object[array.size()];
        switch (type.getElementType().getTypeRoot()) {
            case TINYINT:
                for (int i = 0; i < array.size(); i++) {
                    values[i] = array.isNullAt(i) ? null : (int) array.getByte(i);
                }
                return values;
            case SMALLINT:
                for (int i = 0; i < array.size(); i++) {
                    values[i] = array.isNullAt(i) ? null : (int) array.getShort(i);
                }
                return values;
            case INTEGER:
                for (int i = 0; i < array.size(); i++) {
                    values[i] = array.isNullAt(i) ? null : array.getInt(i);
                }
                return values;
            case BIGINT:
                for (int i = 0; i < array.size(); i++) {
                    values[i] = array.isNullAt(i) ? null : array.getLong(i);
                }
                return values;
            case CHAR:
            case VARCHAR:
                for (int i = 0; i < array.size(); i++) {
                    values[i] = array.isNullAt(i) ? null : array.getString(i).toString();
                }
                return values;
            default:
                throw new IllegalArgumentException(
                        "Unsupported scalar array element type for es-index: "
                                + type.getElementType());
        }
    }

    @Override
    public List<ResultEntry> finish() {
        checkNotFinished();
        finished = true;
        Throwable failure = null;
        try {
            if (writeFailure != null) {
                throw new IllegalStateException(
                        "Cannot finish ES index after a document write failed", writeFailure);
            }
            if (docCount == 0) {
                // Nothing was indexed, but the builder already eagerly created a temp directory and
                // an open Lucene IndexWriter in its constructor; the finally block below releases
                // them. GlobalIndexWriter has no separate close(), so finish() must do the cleanup.
                return Collections.emptyList();
            }

            builder.build();
            Path outputDir = builder.getOutputDir();

            // Snapshot the file list ONCE so the archive layout and the offset table in meta agree
            // exactly (listFiles() order must not differ between packing and meta building).
            java.io.File[] segFiles = outputDir.toFile().listFiles();
            if (segFiles == null) {
                segFiles = new java.io.File[0];
            }

            byte[] meta = buildMeta(segFiles);

            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            // Stream the archive directly to the output stream — DO NOT materialise the entire
            // archive in memory. For real DiskBBQ output (1M × 768-dim vectors ≈ 3 GB per shard)
            // a ByteArrayOutputStream-backed approach hits both heap and DirectByteBuffer limits
            // (java.nio.Bits.reserveMemory OOM at Files.readAllBytes).
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                packDirectoryStream(segFiles, out);
                out.flush();
            }

            return Collections.singletonList(new ResultEntry(fileName, docCount, meta));
        } catch (IOException e) {
            RuntimeException wrapped = new RuntimeException("Failed to finish ES index build", e);
            failure = wrapped;
            throw wrapped;
        } catch (RuntimeException | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                close();
            } catch (Throwable cleanupFailure) {
                if (failure != null) {
                    failure.addSuppressed(cleanupFailure);
                } else {
                    rethrowCleanupFailure(cleanupFailure);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        finished = true;
        if (closed) {
            return;
        }
        Throwable failure = null;
        try {
            builder.close();
        } catch (Throwable e) {
            failure = e;
        }
        try {
            FileIOUtils.deleteDirectory(builder.getOutputDir().toFile());
        } catch (Throwable e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new IOException("Failed to clean up ES index builder", failure);
        }
        closed = true;
    }

    private static void rethrowCleanupFailure(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException("Failed to clean up ES index builder", failure);
    }

    private void checkNotFinished() {
        if (finished) {
            throw new IllegalStateException("ES index writer is already finished");
        }
    }

    private void checkWritable() {
        checkNotFinished();
        if (writeFailure != null) {
            throw new IllegalStateException(
                    "ES index writer cannot accept rows after a document write failed",
                    writeFailure);
        }
    }

    /**
     * Pack the given files into a single archive, streaming directly to {@code out}. Layout is
     * interleaved per file (big-endian): [4-byte file count] then for each file: [4-byte name
     * len][name bytes][8-byte data len][data bytes]. {@link #buildMeta} computes offsets against
     * this exact layout.
     *
     * <p>Streams each file via a small buffered copy so total memory usage is O(buffer), not
     * O(archive size). Returns the total number of bytes written.
     */
    private long packDirectoryStream(java.io.File[] segFiles, java.io.OutputStream out)
            throws IOException {
        // Wrap in BufferedOutputStream so DataOutputStream's small writes don't translate
        // to a flood of underlying OSS writes; the buffer is fixed (64 KB) regardless of file size.
        java.io.BufferedOutputStream bufOut = new java.io.BufferedOutputStream(out, 64 * 1024);
        DataOutputStream dos = new DataOutputStream(bufOut);

        long written = 0;
        dos.writeInt(segFiles.length);
        written += 4;

        byte[] copyBuf = new byte[64 * 1024];
        for (java.io.File file : segFiles) {
            byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
            long fileLen = file.length();
            dos.writeInt(nameBytes.length);
            dos.write(nameBytes);
            dos.writeLong(fileLen);
            written += 4 + nameBytes.length + 8;

            long copied = 0;
            try (java.io.InputStream in = Files.newInputStream(file.toPath())) {
                int n;
                while ((n = in.read(copyBuf)) > 0) {
                    dos.write(copyBuf, 0, n);
                    copied += n;
                }
            }
            if (copied != fileLen) {
                throw new IOException(
                        "Short read packing "
                                + file.getName()
                                + ": expected "
                                + fileLen
                                + " bytes but got "
                                + copied);
            }
            written += fileLen;
        }
        dos.flush();
        return written;
    }

    /**
     * Build the versioned metadata stored in ResultEntry. It persists the resolved per-field index
     * configuration followed by the archive file offset table, so readers use the same mapping that
     * built the index. {@link ESIndexFileMeta} also accepts the legacy offset-only format.
     *
     * <p>The offsets MUST match {@link #packDirectoryStream}'s interleaved layout: each file's data
     * begins right after its own [nameLen][name][dataLen] header.
     */
    private byte[] buildMeta(java.io.File[] segFiles) throws IOException {
        List<String> indexedFieldNames = new ArrayList<>(fields.size());
        List<String> indexedFieldTypes = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            indexedFieldNames.add(field.name());
            indexedFieldTypes.add(field.type().copy(true).asSQLString());
        }
        return ESIndexFileMeta.write(
                segFiles, indexedFieldNames, indexedFieldTypes, indexOptions.getFieldConfigs());
    }
}
