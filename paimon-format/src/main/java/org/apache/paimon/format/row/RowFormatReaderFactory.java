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

package org.apache.paimon.format.row;

import org.apache.paimon.compression.ZstdBlockDecompressor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.NestedProjectedRow;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Factory for creating {@link RowFormatReader}. */
public class RowFormatReaderFactory implements FormatReaderFactory {

    private static final int TAIL_PREFETCH_SIZE = 64 * 1024;

    private final RowType rowType;
    private final RowType projectedRowType;

    public RowFormatReaderFactory(RowType rowType, RowType projectedRowType) {
        this.rowType = rowType;
        this.projectedRowType = projectedRowType;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        FileIO fileIO = context.fileIO();
        Path path = context.filePath();
        long fileSize = context.fileSize();

        SeekableInputStream in = fileIO.newInputStream(path);

        // Ownership of the stream passes to RowFormatReader only on the last line. Everything
        // before it parses lengths and offsets taken from the file itself, so a truncated or
        // corrupt file can throw anywhere in between and would otherwise leak the stream.
        try {
            if (fileSize < RowFileFooter.FOOTER_SIZE) {
                throw new IOException(
                        String.format(
                                "Row file %s holds %d bytes, too few for a %d-byte footer.",
                                path, fileSize, RowFileFooter.FOOTER_SIZE));
            }

            int tailSize = (int) Math.min(TAIL_PREFETCH_SIZE, fileSize);
            long tailOffset = fileSize - tailSize;
            in.seek(tailOffset);
            byte[] tailBuf = new byte[tailSize];
            IOUtils.readFully(in, tailBuf);

            RowFileFooter footer =
                    RowFileFooter.readFrom(tailBuf, tailSize - RowFileFooter.FOOTER_SIZE);
            footer.validate(fileSize);

            RowBlockIndex blockIndex;
            if (footer.indexOffset >= tailOffset) {
                int indexOffsetInBuf = (int) (footer.indexOffset - tailOffset);
                byte[] indexData = new byte[footer.indexLength];
                System.arraycopy(tailBuf, indexOffsetInBuf, indexData, 0, footer.indexLength);
                blockIndex = RowBlockIndex.readFrom(indexData);
            } else {
                blockIndex = RowBlockIndex.readFrom(in, footer.indexOffset, footer.indexLength);
            }
            blockIndex.validate(footer);

            int schemaLength = footer.physicalSchemaLength(fileSize);
            RowType physicalSchema =
                    schemaLength == 0
                            ? rowType
                            : readPhysicalSchema(in, tailBuf, tailOffset, footer, schemaLength);
            return new RowFormatReader(
                    in,
                    path,
                    footer,
                    blockIndex,
                    physicalSchema,
                    // Each reader needs its own wrapper: it is mutated in place per row,
                    // so sharing one instance across readers corrupts interleaved reads.
                    NestedProjectedRow.create(rowType, projectedRowType, physicalSchema),
                    context.selection());
        } catch (Throwable t) {
            IOUtils.closeQuietly(in);
            throw t;
        }
    }

    private RowType readPhysicalSchema(
            SeekableInputStream in,
            byte[] tailBuf,
            long tailOffset,
            RowFileFooter footer,
            int schemaLength)
            throws IOException {
        long schemaOffset = footer.indexOffset + footer.indexLength;
        byte[] schemaBlock;
        int blockOffset;
        if (schemaOffset >= tailOffset) {
            schemaBlock = tailBuf;
            blockOffset = (int) (schemaOffset - tailOffset);
        } else {
            in.seek(schemaOffset);
            schemaBlock = new byte[schemaLength];
            IOUtils.readFully(in, schemaBlock);
            blockOffset = 0;
        }

        byte[] json = decompressSchema(schemaBlock, blockOffset, schemaLength);
        RowType schema;
        try {
            List<DataField> fields =
                    JsonSerdeUtil.fromJson(
                            new String(json, UTF_8), new TypeReference<List<DataField>>() {});
            schema = rowType.copy(fields);
        } catch (RuntimeException e) {
            throw new IOException("Invalid row file physical schema", e);
        }
        if (!schema.getFieldNames().equals(rowType.getFieldNames())) {
            throw new IOException("Row file physical schema does not match the data fields");
        }
        return schema;
    }

    static byte[] decompressSchema(byte[] block, int offset, int length) throws IOException {
        int rawLength = RowFileFooter.readIntLE(block, offset);
        if (rawLength <= 0 || rawLength > RowFileFooter.MAX_PHYSICAL_SCHEMA_SIZE) {
            throw new IOException(
                    "Invalid row file uncompressed physical schema length: " + rawLength);
        }
        byte[] json = new byte[rawLength];
        int actualLength;
        try {
            actualLength =
                    new ZstdBlockDecompressor()
                            .decompress(
                                    block, offset + Integer.BYTES, length - Integer.BYTES, json, 0);
        } catch (RuntimeException e) {
            throw new IOException("Invalid row file compressed physical schema", e);
        }
        if (actualLength != rawLength) {
            throw new IOException("Row file physical schema length does not match its header");
        }
        return json;
    }
}
