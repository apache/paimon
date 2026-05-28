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
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VectorType;

import org.elasticsearch.eslib.api.ESIndexBuilder;
import org.elasticsearch.eslib.api.model.FieldIndexConfig;

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
public class ESIndexGlobalIndexWriter implements GlobalIndexMultiColumnWriter {

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
    public void write(InternalRow row) {
        if (row == null) {
            return;
        }
        try {
            long docId = docCount++;
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
            byte[] archiveBytes = packDirectory(segFiles);

            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                out.write(archiveBytes);
                out.flush();
            }

            builder.close();
            deleteDirectory(outputDir);

            return Collections.singletonList(new ResultEntry(fileName, docCount, meta));
        } catch (IOException e) {
            throw new RuntimeException("Failed to finish ES index build", e);
        }
    }

    /**
     * Pack the given files into a single archive (big-endian). Layout is interleaved per file:
     * [4-byte file count] then for each file: [4-byte name len][name bytes][8-byte data len][data
     * bytes]. {@link #buildMeta} computes offsets against this exact layout.
     */
    private byte[] packDirectory(java.io.File[] segFiles) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(segFiles.length);
        for (java.io.File file : segFiles) {
            byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
            byte[] data = Files.readAllBytes(file.toPath());
            dos.writeInt(nameBytes.length);
            dos.write(nameBytes);
            dos.writeLong(data.length);
            dos.write(data);
        }
        dos.flush();
        return baos.toByteArray();
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
