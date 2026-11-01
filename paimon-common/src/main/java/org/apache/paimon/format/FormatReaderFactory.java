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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** A factory to create {@link RecordReader} for file. */
public interface FormatReaderFactory {

    FileRecordReader<InternalRow> createReader(Context context) throws IOException;

    default FileRecordReader<InternalRow> createReader(Context context, long offset, long length)
            throws IOException {
        throw new UnsupportedOperationException(
                String.format(
                        "Format %s does not support create reader with offset and length.",
                        getClass().getName()));
    }

    /**
     * Row positions of the file which may satisfy the pushed down filters, computed from file
     * metadata only, without reading any data. The ranges are sorted, not overlapping and relative
     * to the start of the file. Returns null when the format can not prune rows this way, callers
     * then read the whole file.
     */
    @Nullable
    default List<Range> candidateRowRanges(Context context) throws IOException {
        return null;
    }

    /** Context for creating reader. */
    interface Context {

        FileIO fileIO();

        Path filePath();

        long fileSize();

        @Nullable
        RoaringBitmap32 selection();

        /** Sizer shared by readers that support dynamic read batch sizing. */
        @Nullable
        default ReadBatchSizer readBatchSizer() {
            return null;
        }

        /** Cache of file metadata already read for the same file, null when not shared. */
        @Nullable
        default FileMetadataCache metadataCache() {
            return null;
        }
    }
}
