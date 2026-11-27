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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.NestedProjectedRow;

import java.io.IOException;

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
        String schemaJson;
        if (schemaOffset >= tailOffset) {
            schemaJson =
                    new String(tailBuf, (int) (schemaOffset - tailOffset), schemaLength, UTF_8);
        } else {
            in.seek(schemaOffset);
            byte[] schemaBytes = new byte[schemaLength];
            IOUtils.readFully(in, schemaBytes);
            schemaJson = new String(schemaBytes, UTF_8);
        }

        DataType schema;
        try {
            schema = JsonSerdeUtil.fromJson(schemaJson, DataType.class);
        } catch (RuntimeException e) {
            throw new IOException("Invalid row file physical schema", e);
        }
        if (!(schema instanceof RowType)
                || !((RowType) schema).getFieldNames().equals(rowType.getFieldNames())) {
            throw new IOException("Row file physical schema does not match the data fields");
        }
        return (RowType) schema;
    }
}
