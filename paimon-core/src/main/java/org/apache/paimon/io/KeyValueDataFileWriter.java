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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * A {@link StatsCollectingSingleFileWriter} to write data files containing {@link KeyValue}s. Also
 * produces {@link DataFileMeta} after writing a file.
 *
 * <p>NOTE: records given to the writer must be sorted because it does not compare the min max keys
 * to produce {@link DataFileMeta}.
 */
public class KeyValueDataFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDataFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final long schemaId;
    private final int level;

    private final SimpleStatsConverter keyStatsConverter;
    private final SimpleStatsConverter valueStatsConverter;
    private final InternalRowSerializer keySerializer;
    private final FileSource fileSource;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;

    private BinaryRow minKey = null;
    private InternalRow maxKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;
    private long deleteRecordCount = 0;
    private final StateAbstractor stateAbstractor;

    public KeyValueDataFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            @Nullable SimpleStatsExtractor simpleStatsExtractor,
            long schemaId,
            int level,
            String compression,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions) {
        super(
                fileIO,
                factory,
                path,
                converter,
                KeyValue.schema(options.thinMode() ? RowType.of() : keyType, valueType),
                simpleStatsExtractor,
                compression,
                StatsCollectorFactories.createStatsFactories(
                        options,
                        KeyValue.schema(options.thinMode() ? RowType.of() : keyType, valueType)
                                .getFieldNames(),
                        keyType.getFieldNames()),
                options.asyncFileWrite());

        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new SimpleStatsConverter(keyType);
        this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
        this.keySerializer = new InternalRowSerializer(keyType);
        this.fileSource = fileSource;
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), valueType, fileIndexOptions);
        this.stateAbstractor = new StateAbstractor(keyType, valueType, options.thinMode());
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);

        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(kv.value());
        }

        updateMinKey(kv);
        updateMaxKey(kv);

        updateMinSeqNumber(kv);
        updateMaxSeqNumber(kv);

        if (kv.valueKind().isRetract()) {
            deleteRecordCount++;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write to Path " + path + " key value " + kv.toString(keyType, valueType));
        }
    }

    private void updateMinKey(KeyValue kv) {
        if (minKey == null) {
            minKey = keySerializer.toBinaryRow(kv.key()).copy();
        }
    }

    private void updateMaxKey(KeyValue kv) {
        maxKey = kv.key();
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

        Pair<SimpleColStats[], SimpleColStats[]> keyValueStats =
                stateAbstractor.fetchKeyValueStats(fieldStats());

        SimpleStats keyStats = keyStatsConverter.toBinaryAllMode(keyValueStats.getKey());
        Pair<List<String>, SimpleStats> valueStatsPair =
                valueStatsConverter.toBinary(keyValueStats.getValue());

        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();

        return new DataFileMeta(
                path.getName(),
                fileIO.getFileSize(path),
                recordCount(),
                minKey,
                keySerializer.toBinaryRow(maxKey).copy(),
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
                valueStatsPair.getKey());
    }

    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }

    private static class StateAbstractor {
        private final int numKeyFields;
        private final int numValueFields;
        // if keyStatMapping is not null, means thin mode on.
        @Nullable private final int[] keyStatMapping;

        public StateAbstractor(RowType keyType, RowType valueType, boolean thinMode) {
            this.numKeyFields = keyType.getFieldCount();
            this.numValueFields = valueType.getFieldCount();
            Map<Integer, Integer> idToIndex = new HashMap<>();
            for (int i = 0; i < valueType.getFieldCount(); i++) {
                idToIndex.put(valueType.getFields().get(i).id(), i);
            }
            if (thinMode) {
                this.keyStatMapping = new int[keyType.getFieldCount()];
                for (int i = 0; i < keyType.getFieldCount(); i++) {
                    keyStatMapping[i] =
                            idToIndex.get(
                                    keyType.getFields().get(i).id()
                                            - SpecialFields.KEY_FIELD_ID_START);
                }
            } else {
                this.keyStatMapping = null;
            }
        }

        Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats) {
            SimpleColStats[] keyStats = new SimpleColStats[numKeyFields];
            SimpleColStats[] valFieldStats = new SimpleColStats[numValueFields];

            // If thin mode only, there is no key stats in rowStats, so we only jump
            // _SEQUNCE_NUMBER_ and _ROW_KIND_ stats. Therefore, the 'from' value is 2.
            // Otherwise, we need to jump key stats, so the 'from' value is numKeyFields + 2.
            int valueFrom = thinMode() ? 2 : numKeyFields + 2;
            System.arraycopy(rowStats, valueFrom, valFieldStats, 0, numValueFields);
            if (thinMode()) {
                // Thin mode on, so need to map value stats to key stats.
                for (int i = 0; i < keyStatMapping.length; i++) {
                    keyStats[i] = valFieldStats[keyStatMapping[i]];
                }
            } else {
                // Thin mode off, just copy stats from rowStats.
                System.arraycopy(valFieldStats, 0, keyStats, 0, numKeyFields);
            }
            return Pair.of(keyStats, valFieldStats);
        }

        private boolean thinMode() {
            return keyStatMapping != null;
        }
    }
}
