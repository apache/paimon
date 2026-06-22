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
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexMultiColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

import org.elasticsearch.eslib.api.ESIndexBuilder;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * Multi-column writer that builds ESLib (Lucene-based) indexes. Accepts InternalRow with all
 * indexed fields and produces a single archive file.
 */
public class ESIndexGlobalIndexWriter
        implements GlobalIndexSingleColumnWriter, GlobalIndexMultiColumnWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ESIndexGlobalIndexWriter.class);

    private static final String FILE_NAME_PREFIX = "es-index";

    private final GlobalIndexFileWriter fileWriter;
    private final List<DataField> fields;
    private final ESIndexOptions indexOptions;
    private final ESIndexBuilder builder;
    private long docCount;

    public ESIndexGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter, List<DataField> fields, ESIndexOptions indexOptions)
            throws IOException {
        this.fileWriter = fileWriter;
        this.fields = fields;
        this.indexOptions = indexOptions;
        this.builder = ESIndexBuilderFactory.create(indexOptions.getFieldConfigs());
        this.docCount = 0;
    }

    @Override
    public void write(Object fieldData, long relativeRowId) {
        if (fieldData == null) {
            return;
        }
        try {
            // Use the builder-supplied shard-relative row id as the Lucene docId (read side maps
            // _ROW_ID = rangeFrom + docId); track docCount so finish() reports a non-empty
            // rowCount.
            long docId = relativeRowId;
            docCount++;
            DataField field = fields.get(0);
            FieldIndexConfig config = indexOptions.getConfig(field.name());
            if (config == null) {
                return;
            }
            // [DIAG] dump first 5 fieldData values to verify input is non-empty / correctly shaped
            if (docId < 5) {
                LOG.info(
                        "[eslib-writer.write(Object)] docId={}, field={}, indexType={}, "
                                + "fieldData.class={}, sample={}",
                        docId,
                        field.name(),
                        config.indexType(),
                        fieldData.getClass().getName(),
                        sampleFieldData(fieldData));
            }
            writeSingleField(fieldData, field, config, docId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write document to ES index", e);
        }
    }

    @Override
    public void write(long rowId, InternalRow row) {
        writeRow(rowId, row);
    }

    public void writeRow(long rowId, InternalRow row) {
        if (row == null) {
            return;
        }
        try {
            // Use the shard-relative rowId supplied by the builder as the Lucene docId; the read
            // side reconstructs the absolute id as `_ROW_ID = rangeFrom + docId`.
            long docId = rowId;
            // Track the row count so finish() knows the archive is non-empty and reports rowCount.
            docCount++;
            for (int i = 0; i < fields.size(); i++) {
                if (row.isNullAt(i)) {
                    continue;
                }
                DataField field = fields.get(i);
                FieldIndexConfig config = indexOptions.getConfig(field.name());
                if (config == null) {
                    continue;
                }
                writeField(row, i, field, config, docId);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write document to ES index", e);
        }
    }

    /** Pretty-print a fieldData sample for diagnostics (vectors -> first 3 elements + length). */
    private static String sampleFieldData(Object fieldData) {
        if (fieldData == null) {
            return "null";
        }
        if (fieldData instanceof InternalArray) {
            float[] f = ((InternalArray) fieldData).toFloatArray();
            return "InternalArray(len="
                    + f.length
                    + ", first3="
                    + (f.length >= 3
                            ? "[" + f[0] + "," + f[1] + "," + f[2] + "]"
                            : java.util.Arrays.toString(f))
                    + ")";
        }
        if (fieldData instanceof InternalVector) {
            float[] f = ((InternalVector) fieldData).toFloatArray();
            return "InternalVector(len="
                    + f.length
                    + ", first3="
                    + (f.length >= 3
                            ? "[" + f[0] + "," + f[1] + "," + f[2] + "]"
                            : java.util.Arrays.toString(f))
                    + ")";
        }
        String s = fieldData.toString();
        return s.length() > 200 ? s.substring(0, 200) + "...(truncated " + s.length() + ")" : s;
    }

    private void writeSingleField(
            Object fieldData, DataField field, FieldIndexConfig config, long docId)
            throws IOException {
        switch (config.indexType()) {
            case VECTOR:
                float[] vector = null;
                if (fieldData instanceof InternalArray) {
                    vector = ((InternalArray) fieldData).toFloatArray();
                } else if (fieldData instanceof InternalVector) {
                    vector = ((InternalVector) fieldData).toFloatArray();
                }
                if (vector != null) {
                    builder.addVector(field.name(), docId, vector);
                }
                break;
            case FULLTEXT:
                builder.addTextField(field.name(), docId, fieldData.toString());
                break;
            case KEYWORD:
                builder.addScalarField(
                        field.name(),
                        docId,
                        fieldData.toString(),
                        org.elasticsearch.eslib.api.model.ScalarFieldType.KEYWORD);
                break;
            case SCALAR:
            case DATE:
                builder.addScalarField(field.name(), docId, fieldData, config.scalarType());
                break;
            default:
                break;
        }
    }

    private void writeField(
            InternalRow row, int pos, DataField field, FieldIndexConfig config, long docId)
            throws IOException {
        switch (config.indexType()) {
            case VECTOR:
                float[] vector = extractVector(row, pos, field.type());
                if (vector != null) {
                    builder.addVector(field.name(), docId, vector);
                }
                break;
            case FULLTEXT:
                String text = row.getString(pos).toString();
                builder.addTextField(field.name(), docId, text);
                break;
            case KEYWORD:
                String keyword = row.getString(pos).toString();
                builder.addScalarField(
                        field.name(),
                        docId,
                        keyword,
                        org.elasticsearch.eslib.api.model.ScalarFieldType.KEYWORD);
                break;
            case SCALAR:
            case DATE:
                Object value = extractScalar(row, pos, field.type());
                if (value != null) {
                    builder.addScalarField(field.name(), docId, value, config.scalarType());
                }
                break;
            default:
                break;
        }
    }

    private float[] extractVector(InternalRow row, int pos, DataType type) {
        if (type instanceof VectorType) {
            InternalVector vec = row.getVector(pos);
            return vec.toFloatArray();
        }
        InternalArray array = row.getArray(pos);
        return array.toFloatArray();
    }

    private Object extractScalar(InternalRow row, int pos, DataType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return row.getInt(pos);
            case BIGINT:
                return row.getLong(pos);
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos).toString();
            default:
                return row.getString(pos).toString();
        }
    }

    @Override
    public List<ResultEntry> finish() {
        if (docCount == 0) {
            return Collections.emptyList();
        }
        try {
            long t0 = System.currentTimeMillis();
            builder.build();
            long buildMs = System.currentTimeMillis() - t0;
            Path outputDir = builder.getOutputDir();

            // Snapshot the file list ONCE so the archive layout and the offset table in meta agree
            // exactly (listFiles() order must not differ between packing and meta building).
            java.io.File[] segFiles = outputDir.toFile().listFiles();
            if (segFiles == null) {
                segFiles = new java.io.File[0];
            }

            // === DIAGNOSTIC: dump the on-disk Lucene segment files produced by builder.build(),
            // BEFORE archiving. If totalBytes is tiny (~17KB), the bug is upstream (Lucene write
            // path produced empty files despite addVector being called); if it's large (~100MB+
            // for 1M × 768-dim DiskBBQ) but the archived .index file still ends up small, the bug
            // is in packDirectory/upload below.
            long totalBytes = 0;
            StringBuilder inventory = new StringBuilder();
            for (java.io.File f : segFiles) {
                totalBytes += f.length();
                inventory
                        .append("\n    ")
                        .append(String.format("%14d", f.length()))
                        .append("  ")
                        .append(f.getName());
            }
            LOG.info(
                    "[eslib-writer.finish] docCount={}, builder.build() took {}ms, outputDir={}, "
                            + "segment files={}, totalBytes={}:{}",
                    docCount,
                    buildMs,
                    outputDir,
                    segFiles.length,
                    totalBytes,
                    inventory.toString());

            byte[] meta = buildMeta(segFiles);

            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            // Stream the archive directly to the output stream — DO NOT materialise the entire
            // archive in memory. For real DiskBBQ output (1M × 768-dim vectors ≈ 3 GB per shard)
            // a ByteArrayOutputStream-backed approach hits both heap and DirectByteBuffer limits
            // (java.nio.Bits.reserveMemory OOM at Files.readAllBytes).
            long archiveBytesWritten;
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                archiveBytesWritten = packDirectoryStream(segFiles, out);
                out.flush();
            }
            LOG.info(
                    "[eslib-writer.finish] archive uploaded: name={}, archiveBytes.length={}, "
                            + "meta.length={}",
                    fileName,
                    archiveBytesWritten,
                    meta == null ? -1 : meta.length);

            builder.close();
            deleteDirectory(outputDir);

            return Collections.singletonList(new ResultEntry(fileName, docCount, meta));
        } catch (IOException e) {
            throw new RuntimeException("Failed to finish ES index build", e);
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
     * Build metadata stored in ResultEntry. Encodes the file offset table (big-endian) so the
     * reader can provide an ArchiveDataProvider without re-parsing the archive. Format: [4-byte
     * file count] then for each file: [4-byte name len][name bytes][8-byte offset][8-byte length].
     *
     * <p>The offsets MUST match {@link #packDirectory}'s interleaved layout: each file's data
     * begins right after its own [nameLen][name][dataLen] header.
     */
    private byte[] buildMeta(java.io.File[] segFiles) throws IOException {
        if (segFiles.length == 0) {
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(segFiles.length);

        long offset = 4; // past the [4-byte file count]
        for (java.io.File file : segFiles) {
            byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
            long fileLen = file.length();
            long dataOffset = offset + 4 + nameBytes.length + 8; // after this file's header
            dos.writeInt(nameBytes.length);
            dos.write(nameBytes);
            dos.writeLong(dataOffset);
            dos.writeLong(fileLen);
            offset = dataOffset + fileLen; // next file's header starts here
        }
        dos.flush();
        return baos.toByteArray();
    }

    private static void deleteDirectory(Path dir) {
        java.io.File[] segFiles = dir.toFile().listFiles();
        if (segFiles != null) {
            for (java.io.File file : segFiles) {
                file.delete();
            }
        }
        dir.toFile().delete();
    }
}
