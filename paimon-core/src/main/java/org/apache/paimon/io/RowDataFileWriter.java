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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link InternalRow}.
 * Also produces {@link DataFileMeta} after writing a file.
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final LongCounter seqNumCounter;
    private final FieldStatsArraySerializer statsArraySerializer;
    private final IndexWriter indexWriter;

    public RowDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            DataFilePathFactory pathFactory,
            RowType writeSchema,
            @Nullable TableStatsExtractor tableStatsExtractor,
            long schemaId,
            LongCounter seqNumCounter,
            String fileCompression,
            FieldStatsCollector.Factory[] statsCollectors,
            Map<String, Map<String, Options>> indexExpr,
            long indexSizeInMeta) {
        super(
                fileIO,
                factory,
                pathFactory.newPath(),
                Function.identity(),
                writeSchema,
                tableStatsExtractor,
                fileCompression,
                statsCollectors);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounter;
        this.statsArraySerializer = new FieldStatsArraySerializer(writeSchema);
        this.indexWriter =
                new IndexWriter(fileIO, pathFactory, writeSchema, indexExpr, indexSizeInMeta);
    }

    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        indexWriter.write(row);
        seqNumCounter.add(1L);
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
        super.close();
    }

    @Override
    public DataFileMeta result() throws IOException {
        BinaryTableStats stats = statsArraySerializer.toBinary(fieldStats());
        Pair<BinaryRow, List<String>> indexResult = indexWriter.result();
        return DataFileMeta.forAppend(
                path.getName(),
                fileIO.getFileSize(path),
                recordCount(),
                stats,
                indexResult.getLeft(),
                seqNumCounter.getValue() - super.recordCount(),
                seqNumCounter.getValue() - 1,
                schemaId,
                indexResult.getRight() == null ? Collections.emptyList() : indexResult.getRight());
    }
}
