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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A simple {@link FormatWriter} implementation that wraps a {@link ParquetWriter}. */
public class ParquetBulkWriter implements FormatWriter {

    /** The ParquetWriter to write to. */
    private final ParquetWriter<InternalRow> parquetWriter;

    /** Cached footer metadata after close, used to avoid re-reading the file for stats. */
    @Nullable private ParquetMetadata footerMetadata;

    /**
     * Creates a new ParquetBulkWriter wrapping the given ParquetWriter.
     *
     * @param parquetWriter The ParquetWriter to write to.
     */
    public ParquetBulkWriter(ParquetWriter<InternalRow> parquetWriter) {
        this.parquetWriter = checkNotNull(parquetWriter, "parquetWriter");
    }

    @Override
    public void addElement(InternalRow datum) throws IOException {
        parquetWriter.write(datum);
    }

    @Override
    public void appendFile(FileIO fileIO, Path sourcePath, long fileLength) throws IOException {
        ParquetInputFile inputFile = ParquetInputFile.fromPath(fileIO, sourcePath, fileLength);
        try (ParquetFileReader reader =
                new ParquetFileReader(inputFile, ParquetReadOptions.builder().build(), null)) {
            reader.appendTo(parquetWriter.getFileWriter());
        }
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
        this.footerMetadata = parquetWriter.getFooter();
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && parquetWriter.getDataSize() >= targetSize;
    }

    @Nullable
    @Override
    public Object writerMetadata() {
        return footerMetadata;
    }
}
