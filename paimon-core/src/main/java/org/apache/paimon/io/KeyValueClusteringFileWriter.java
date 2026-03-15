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
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.KeyValueThinSerializer;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileIndexWriter.FileIndexResult;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * File writer for clustering mode. Different from the normal KeyValue file writer, in this class
 * minKey and maxKey store the clustering field instead of the primary key field.
 */
public class KeyValueClusteringFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueClusteringFileWriter.class);

    protected final RowType keyType;
    protected final RowType valueType;
    private final long schemaId;
    private final int level;

    private final SimpleStatsConverter keyStatsConverter;
    private final boolean thinMode;
    private final boolean isExternalPath;
    private final SimpleStatsConverter valueStatsConverter;
    private final Projection clusteringProjection;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;
    private final int[] keyStatMapping;

    private BinaryRow minClusteringFields = null;
    private BinaryRow currentClusteringFields = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;
    private long deleteRecordCount = 0;

    public KeyValueClusteringFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType keyType,
            RowType valueType,
            long schemaId,
            int level,
            boolean thinMode,
            CoreOptions options,
            FileIndexOptions fileIndexOptions,
            boolean isExternalPath) {
        super(
                fileIO,
                context,
                path,
                thinMode
                        ? new KeyValueThinSerializer(keyType, valueType)::toRow
                        : new KeyValueSerializer(keyType, valueType)::toRow,
                KeyValue.schema(RowType.of(), valueType),
                options.asyncFileWrite());
        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new SimpleStatsConverter(keyType);
        this.thinMode = thinMode;
        this.isExternalPath = isExternalPath;
        this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
        this.clusteringProjection =
                CodeGenUtils.newProjection(valueType, options.clusteringColumns());
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), valueType, fileIndexOptions);

        Map<Integer, Integer> idToIndex = new HashMap<>(valueType.getFieldCount());
        for (int i = 0; i < valueType.getFieldCount(); i++) {
            idToIndex.put(valueType.getFields().get(i).id(), i);
        }
        this.keyStatMapping = new int[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyStatMapping[i] =
                    idToIndex.get(
                            keyType.getFields().get(i).id() - SpecialFields.KEY_FIELD_ID_START);
        }
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);

        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(kv.value());
        }

        currentClusteringFields = clusteringProjection.apply(kv.value());
        if (minClusteringFields == null) {
            minClusteringFields = currentClusteringFields.copy();
        }

        updateMinSeqNumber(kv);
        updateMaxSeqNumber(kv);

        if (kv.valueKind().isRetract()) {
            deleteRecordCount++;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write to Path {} key value {}", path, kv.toString(keyType, valueType));
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

        long fileSize = outputBytes();
        SimpleColStats[] rowStats = fieldStats(fileSize);
        int offset = thinMode ? 2 : keyType.getFieldCount() + 2;
        SimpleColStats[] valFieldStats = Arrays.copyOfRange(rowStats, offset, rowStats.length);
        SimpleColStats[] keyColStats = new SimpleColStats[keyType.getFieldCount()];
        for (int i = 0; i < keyStatMapping.length; i++) {
            keyColStats[i] = valFieldStats[keyStatMapping[i]];
        }

        SimpleStats keyStats = keyStatsConverter.toBinaryAllMode(keyColStats);
        Pair<List<String>, SimpleStats> valueStatsPair =
                valueStatsConverter.toBinary(valFieldStats);

        FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();

        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.create(
                path.getName(),
                fileSize,
                recordCount(),
                minClusteringFields,
                currentClusteringFields,
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
                FileSource.COMPACT,
                valueStatsPair.getKey(),
                externalPath,
                null,
                null);
    }

    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }
}
