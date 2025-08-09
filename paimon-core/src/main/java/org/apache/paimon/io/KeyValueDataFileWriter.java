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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RowHelper;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link KeyValue}s. Also
 * produces {@link DataFileMeta} after writing a file.
 *
 * <p>NOTE: records given to the writer must be sorted because it does not compare the min max keys
 * to produce {@link DataFileMeta}.
 */
public abstract class KeyValueDataFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDataFileWriter.class);

    protected final RowType keyType;
    protected final RowType valueType;
    private final long schemaId;
    private final int level;

    private final SimpleStatsConverter keyStatsConverter;
    private final boolean isExternalPath;
    private final SimpleStatsConverter valueStatsConverter;
    private final RowHelper keyKeeper;
    private final FileSource fileSource;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;

    private BinaryRow minKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;
    private long deleteRecordCount = 0;

    public KeyValueDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            RowType writeRowType,
            long schemaId,
            int level,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions,
            boolean isExternalPath) {
        super(fileIO, context, path, converter, writeRowType, options.asyncFileWrite());

        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new SimpleStatsConverter(keyType);
        this.isExternalPath = isExternalPath;
        this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
        this.keyKeeper = new RowHelper(keyType.getFieldTypes());
        this.fileSource = fileSource;
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), valueType, fileIndexOptions);
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);

        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(kv.value());
        }

        keyKeeper.copyInto(kv.key());
        if (minKey == null) {
            minKey = keyKeeper.copiedRow();
        }

        updateMinSeqNumber(kv);
        updateMaxSeqNumber(kv);

        if (kv.valueKind().isRetract()) {
            deleteRecordCount++;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write to Path " + path + " key value " + kv.toString(keyType, valueType));
        }
    }

    private void updateMinSeqNumber(KeyValue kv) {
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber());
    }

    private void updateMaxSeqNumber(KeyValue kv) {
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber());
    }

    @Override
    @Nullable
    public DataFileMeta result() throws IOException {
        if (recordCount() == 0) {
            return null;
        }

        long fileSize = outputBytes;
        Pair<SimpleColStats[], SimpleColStats[]> keyValueStats =
                fetchKeyValueStats(fieldStats(fileSize));

        SimpleStats keyStats = keyStatsConverter.toBinaryAllMode(keyValueStats.getKey());
        Pair<List<String>, SimpleStats> valueStatsPair =
                valueStatsConverter.toBinary(keyValueStats.getValue());

        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();

        String externalPath = isExternalPath ? path.toString() : null;
        return new DataFileMeta(
                path.getName(),
                fileSize,
                recordCount(),
                minKey,
                keyKeeper.copiedRow(),
                keyStats,
                valueStatsPair.getValue(),
                minSeqNumber,
                maxSeqNumber,
                schemaId,
                level,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                deleteRecordCount,
                indexResult.embeddedIndexBytes(),
                fileSource,
                valueStatsPair.getKey(),
                externalPath,
                null,
                null);
    }

    abstract Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats);

    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }
}
