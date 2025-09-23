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

package org.apache.paimon.table.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.RowDataRollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.TwoPhaseCommitDirectSinkWriter;

import java.util.List;

/** {@link RecordWriter} for format table. */
public class FormatTableRecordWriter {

    private final FileIO fileIO;
    private final DataFilePathFactory pathFactory;
    private final RowType writeSchema;
    private final String fileCompression;
    private final FileFormat fileFormat;
    private final long targetFileSize;

    private TwoPhaseCommitDirectSinkWriter<InternalRow> twoPhaseCommitSinkWriter;

    public FormatTableRecordWriter(
            FileIO fileIO,
            FileFormat fileFormat,
            long targetFileSize,
            DataFilePathFactory pathFactory,
            RowType writeSchema,
            String fileCompression) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.fileCompression = fileCompression;
        this.writeSchema = writeSchema;
        this.fileFormat = fileFormat;
        this.targetFileSize = targetFileSize;
        this.twoPhaseCommitSinkWriter =
                new TwoPhaseCommitDirectSinkWriter<>(this::createRollingRowWriter);
    }

    public void write(InternalRow rowData) throws Exception {
        Preconditions.checkArgument(
                rowData.getRowKind().isAdd(),
                "Append-only writer can only accept insert or update_after row kind, but current row kind is: %s. "
                        + "You can configure 'ignore-delete' to ignore retract records.",
                rowData.getRowKind());
        boolean success = twoPhaseCommitSinkWriter.write(rowData);
        if (!success) {
            closeAndGetCommitters();
            success = twoPhaseCommitSinkWriter.write(rowData);
            if (!success) {
                // Should not get here, because writeBuffer will throw too big exception out.
                // But we throw again in case of something unexpected happens. (like someone changed
                // code in SpillableBuffer.)
                throw new RuntimeException("Mem table is too small to hold a single element.");
            }
        }
    }

    public List<TwoPhaseOutputStream.Committer> closeAndGetCommitters() throws Exception {
        return twoPhaseCommitSinkWriter.closeAndGetCommitters();
    }

    private RowDataRollingFileWriter createRollingRowWriter() {
        return new RowDataRollingFileWriter(
                fileIO,
                0L,
                fileFormat,
                targetFileSize,
                writeSchema,
                pathFactory,
                new LongCounter(0),
                fileCompression,
                null,
                new FileIndexOptions(),
                FileSource.APPEND,
                false,
                false,
                null,
                true);
    }

    public void close() throws Exception {
        twoPhaseCommitSinkWriter.close();
    }
}
