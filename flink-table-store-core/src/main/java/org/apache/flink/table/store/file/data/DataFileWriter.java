/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.data;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.stats.FileStatsExtractor;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.BaseBulkWriter;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/** Writes {@link KeyValue}s into data files. */
public class DataFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileWriter.class);

    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStatsExtractor fileStatsExtractor;
    private final DataFilePathFactory pathFactory;
    private final long suggestedFileSize;

    private DataFileWriter(
            RowType keyType,
            RowType valueType,
            BulkWriter.Factory<RowData> writerFactory,
            FileStatsExtractor fileStatsExtractor,
            DataFilePathFactory pathFactory,
            long suggestedFileSize) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.fileStatsExtractor = fileStatsExtractor;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    @VisibleForTesting
    public DataFilePathFactory pathFactory() {
        return pathFactory;
    }

    /**
     * Write several {@link KeyValue}s into a data file of a given level.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<DataFileMeta> write(CloseableIterator<KeyValue> iterator, int level)
            throws Exception {

        RollingKvWriter rollingKvWriter = createRollingKvWriter(level, suggestedFileSize);
        try (RollingKvWriter writer = rollingKvWriter) {
            writer.write(iterator);

        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing data files. Cleaning up.", e);

            rollingKvWriter.abort();
            throw e;
        } finally {
            iterator.close();
        }

        return rollingKvWriter.result();
    }

    public void delete(DataFileMeta file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file.fileName()));
    }

    private class KvBulkWriterFactory implements BulkWriter.Factory<KeyValue> {

        @Override
        public BulkWriter<KeyValue> create(FSDataOutputStream out) throws IOException {
            KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);

            return new BaseBulkWriter<>(writerFactory.create(out), serializer::toRow);
        }
    }

    private class KvFileWriter extends BaseFileWriter<KeyValue, DataFileMeta> {
        private final int level;
        private final RowDataSerializer keySerializer;

        private FieldStatsCollector keyStatsCollector = null;
        private FieldStatsCollector valueStatsCollector = null;

        private BinaryRowData minKey = null;
        private RowData maxKey = null;
        private long minSeqNumber = Long.MAX_VALUE;
        private long maxSeqNumber = Long.MIN_VALUE;

        public KvFileWriter(BulkWriter.Factory<KeyValue> writerFactory, Path path, int level)
                throws IOException {
            super(writerFactory, path);

            this.level = level;
            this.keySerializer = new RowDataSerializer(keyType);
            if (fileStatsExtractor == null) {
                this.keyStatsCollector = new FieldStatsCollector(keyType);
                this.valueStatsCollector = new FieldStatsCollector(valueType);
            }
        }

        @Override
        public void write(KeyValue kv) throws IOException {
            super.write(kv);

            if (fileStatsExtractor == null) {
                keyStatsCollector.collect(kv.key());
                valueStatsCollector.collect(kv.value());
            }

            updateMinKey(kv);
            updateMaxKey(kv);

            updateMinSeqNumber(kv);
            updateMaxSeqNumber(kv);
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
        protected DataFileMeta createFileMeta(Path path) throws IOException {

            FieldStats[] keyStats;
            FieldStats[] valueStats;
            if (fileStatsExtractor == null) {
                keyStats = keyStatsCollector.extract();
                valueStats = valueStatsCollector.extract();
            } else {
                FieldStats[] rowStats = fileStatsExtractor.extract(path);
                int numKeyFields = keyType.getFieldCount();
                keyStats = Arrays.copyOfRange(rowStats, 0, numKeyFields);
                valueStats = Arrays.copyOfRange(rowStats, numKeyFields + 2, rowStats.length);
            }

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
                    level);
        }
    }

    private static class RollingKvWriter extends RollingFileWriter<KeyValue, DataFileMeta> {

        public RollingKvWriter(Supplier<KvFileWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private Supplier<KvFileWriter> createWriterFactory(int level) {
        return () -> {
            try {
                return new KvFileWriter(new KvBulkWriterFactory(), pathFactory.newPath(), level);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private RollingKvWriter createRollingKvWriter(int level, long targetFileSize) {
        return new RollingKvWriter(createWriterFactory(level), targetFileSize);
    }

    /** Creates {@link DataFileWriter}. */
    public static class Factory {

        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public DataFileWriter create(BinaryRowData partition, int bucket) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            return new DataFileWriter(
                    keyType,
                    valueType,
                    fileFormat.createWriterFactory(recordType),
                    fileFormat.createStatsExtractor(recordType).orElse(null),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    suggestedFileSize);
        }
    }
}
