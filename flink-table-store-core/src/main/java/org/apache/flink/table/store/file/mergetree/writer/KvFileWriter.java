/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.file.mergetree.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/** A {@link FileWriter} to write {@link KeyValue}. */
public class KvFileWriter extends BaseFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KvFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final FieldStatsArraySerializer keyStatsConverter;
    private final FieldStatsArraySerializer valueStatsConverter;

    private final int level;
    private final long schemaId;
    private final RowDataSerializer keySerializer;

    private BinaryRowData minKey = null;
    private RowData maxKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;

    public KvFileWriter(
            RowType keyType,
            RowType valueType,
            FieldStatsArraySerializer keyStatsConverter,
            FieldStatsArraySerializer valueStatsConverter,
            Factory<KeyValue, Metric> writerFactory,
            Path path,
            int level,
            long schemaId) {
        super(writerFactory, path);
        this.keyType = keyType;
        this.valueType = valueType;
        this.keyStatsConverter = keyStatsConverter;
        this.valueStatsConverter = valueStatsConverter;

        this.level = level;
        this.schemaId = schemaId;
        this.keySerializer = new RowDataSerializer(this.keyType);
    }

    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);
        updateMetric(kv);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write key value " + kv.toString(keyType, valueType));
        }
    }

    public void updateMetric(KeyValue kv) {
        if (minKey == null) {
            minKey = keySerializer.toBinaryRow(kv.key()).copy();
        }

        maxKey = kv.key();
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber());
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber());
    }

    @Override
    protected DataFileMeta createResult(Path path, Metric metric) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing data file " + path);
        }

        if (minKey == null) {
            throw new IllegalStateException(
                    "KvFileWriter does not support creating empty file."
                            + " At least, you should update some metrics.");
        }

        FieldStats[] rowStats = metric.fieldStats();
        int numKeyFields = keyType.getFieldCount();

        FieldStats[] keyFieldStats = Arrays.copyOfRange(rowStats, 0, numKeyFields);
        BinaryTableStats keyStats = keyStatsConverter.toBinary(keyFieldStats);

        FieldStats[] valFieldStats =
                Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length);
        BinaryTableStats valueStats = valueStatsConverter.toBinary(valFieldStats);

        return new DataFileMeta(
                path.getName(),
                FileUtils.getFileSize(path),
                recordCount(),
                minKey,
                keySerializer.toBinaryRow(maxKey).copy(),
                keyStats,
                valueStats,
                minSeqNumber,
                maxSeqNumber,
                schemaId,
                level);
    }
}
