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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final LongCounter seqNumCounter;
    private final boolean isExternalPath;
    private final SimpleStatsConverter statsArraySerializer;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;
    private final FileSource fileSource;
    @Nullable private final List<String> writeCols;

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            LongCounter seqNumCounter,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols) {
        super(fileIO, context, path, Function.identity(), writeSchema, asyncFileWrite);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounter;
        this.isExternalPath = isExternalPath;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema, statsDenseStore);
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), writeSchema, fileIndexOptions);
        this.fileSource = fileSource;
        this.writeCols = writeCols;
    }

    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(row);
        }
        seqNumCounter.add(1L);
    }

    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }

    @Override
    public DataFileMeta result() throws IOException {
        long fileSize = outputBytes;
        Pair<List<String>, SimpleStats> statsPair =
                statsArraySerializer.toBinary(fieldStats(fileSize));
        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();
        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                recordCount(),
                statsPair.getRight(),
                seqNumCounter.getValue() - super.recordCount(),
                seqNumCounter.getValue() - 1,
                schemaId,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                indexResult.embeddedIndexBytes(),
                fileSource,
                statsPair.getKey(),
                externalPath,
                null,
                writeCols);
    }
}
