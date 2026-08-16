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
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.NestedProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;

/** Factory for creating {@link RowFormatReader}. */
public class RowFormatReaderFactory implements FormatReaderFactory {

    private static final int TAIL_PREFETCH_SIZE = 64 * 1024;

    private final RowType rowType;
    @Nullable private final NestedProjectedRow projection;

    public RowFormatReaderFactory(RowType rowType, @Nullable NestedProjectedRow projection) {
        this.rowType = rowType;
        this.projection = projection;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        FileIO fileIO = context.fileIO();
        Path path = context.filePath();
        long fileSize = context.fileSize();

        SeekableInputStream in = fileIO.newInputStream(path);

        int tailSize = (int) Math.min(TAIL_PREFETCH_SIZE, fileSize);
        long tailOffset = fileSize - tailSize;
        in.seek(tailOffset);
        byte[] tailBuf = new byte[tailSize];
        IOUtils.readFully(in, tailBuf);

        RowFileFooter footer =
                RowFileFooter.readFrom(tailBuf, tailSize - RowFileFooter.FOOTER_SIZE);

        RowBlockIndex blockIndex;
        if (footer.indexOffset >= tailOffset) {
            int indexOffsetInBuf = (int) (footer.indexOffset - tailOffset);
            byte[] indexData = new byte[footer.indexLength];
            System.arraycopy(tailBuf, indexOffsetInBuf, indexData, 0, footer.indexLength);
            blockIndex = RowBlockIndex.readFrom(indexData);
        } else {
            blockIndex = RowBlockIndex.readFrom(in, footer.indexOffset, footer.indexLength);
        }

        return new RowFormatReader(
                in, path, footer, blockIndex, rowType, projection, context.selection());
    }
}
