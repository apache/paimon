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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.toFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final LongCounter seqNumCounter;
    private final SimpleStatsConverter statsArraySerializer;
    @Nullable private final FileIndexWriter fileIndexWriter;
    private final FileSource fileSource;

    public RowDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            RowType writeSchema,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            long schemaId,
            LongCounter seqNumCounter,
            String fileCompression,
            SimpleColStatsCollector.Factory[] statsCollectors,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource) {
        super(
                fileIO,
                factory,
                path,
                Function.identity(),
                writeSchema,
                simpleStatsExtractor,
                fileCompression,
                statsCollectors);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounter;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema);
        this.fileIndexWriter =
                FileIndexWriter.create(
                        fileIO, toFileIndexPath(path), writeSchema, fileIndexOptions);
        this.fileSource = fileSource;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        if (fileIndexWriter != null) {
            fileIndexWriter.write(row);
        }
        seqNumCounter.add(1L);
    }

    @Override
    public void close() throws IOException {
        if (fileIndexWriter != null) {
            fileIndexWriter.close();
        }
        super.close();
    }

    @Override
    public DataFileMeta result() throws IOException {
        SimpleStats stats = statsArraySerializer.toBinary(fieldStats());
        FileIndexWriter.FileIndexResult indexResult =
                fileIndexWriter == null ? FileIndexWriter.EMPTY_RESULT : fileIndexWriter.result();
        return DataFileMeta.forAppend(
                path.getName(),
                fileIO.getFileSize(path),
                recordCount(),
                stats,
                seqNumCounter.getValue() - super.recordCount(),
                seqNumCounter.getValue() - 1,
                schemaId,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                indexResult.embeddedIndexBytes(),
                fileSource);
    }
}
