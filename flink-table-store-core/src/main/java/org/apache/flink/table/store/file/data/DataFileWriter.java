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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.MetricFileWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.store.format.FieldStats;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Writes {@link KeyValue}s into data files. */
public class DataFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileWriter.class);

    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStatsExtractor fileStatsExtractor;
    private final FieldStatsArraySerializer keyStatsConverter;
    private final FieldStatsArraySerializer valueStatsConverter;
    private final DataFilePathFactory pathFactory;
    private final long suggestedFileSize;

    private DataFileWriter(
            long schemaId,
            RowType keyType,
            RowType valueType,
            BulkWriter.Factory<RowData> writerFactory,
            @Nullable FileStatsExtractor fileStatsExtractor,
            DataFilePathFactory pathFactory,
            long suggestedFileSize) {
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.fileStatsExtractor = fileStatsExtractor;
        this.keyStatsConverter = new FieldStatsArraySerializer(keyType);
        this.valueStatsConverter = new FieldStatsArraySerializer(valueType);

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

    /** Write raw {@link KeyValue} iterator into a changelog file. */
    public Path writeLevel0Changelog(CloseableIterator<KeyValue> iterator) throws Exception {
        FileWriter.Factory<KeyValue, Metric> writerFactory = createFileWriterFactory();
        Path changelogPath = pathFactory.newChangelogPath();
        doWrite(writerFactory.create(changelogPath), iterator);
        return changelogPath;
    }

    /**
     * Write several {@link KeyValue}s into a data file of level 0.
     *
     * @return empty if iterator is empty
     */
    public Optional<DataFileMeta> writeLevel0(CloseableIterator<KeyValue> iterator)
            throws Exception {
        List<DataFileMeta> files = write(iterator, 0);
        if (files.size() > 1) {
            throw new RuntimeException("Produce illegal multiple Level 0 files: " + files);
        }
        return files.size() == 0 ? Optional.empty() : Optional.of(files.get(0));
    }

    /**
     * Write several {@link KeyValue}s into data files of a given level.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<DataFileMeta> write(CloseableIterator<KeyValue> iterator, int level)
            throws Exception {
        // Don't roll file for level 0
        long suggestedFileSize = level == 0 ? Long.MAX_VALUE : this.suggestedFileSize;
        return doWrite(createRollingKvWriter(level, suggestedFileSize), iterator);
    }

    private <R> R doWrite(FileWriter<KeyValue, R> fileWriter, CloseableIterator<KeyValue> iterator)
            throws Exception {
        try (FileWriter<KeyValue, R> writer = fileWriter) {
            writer.write(iterator);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing data files. Cleaning up.", e);
            fileWriter.abort();
            throw e;
        } finally {
            iterator.close();
        }
        return fileWriter.result();
    }

    public void delete(DataFileMeta file) {
        delete(file.fileName());
    }

    public void delete(String file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file));
    }

    private class KvFileWriter extends BaseFileWriter<KeyValue, DataFileMeta> {
        private final int level;
        private final RowDataSerializer keySerializer;

        private BinaryRowData minKey = null;
        private RowData maxKey = null;
        private long minSeqNumber = Long.MAX_VALUE;
        private long maxSeqNumber = Long.MIN_VALUE;

        public KvFileWriter(
                FileWriter.Factory<KeyValue, Metric> writerFactory, Path path, int level)
                throws IOException {
            super(writerFactory, path);

            this.level = level;
            this.keySerializer = new RowDataSerializer(keyType);
        }

        @Override
        public void write(KeyValue kv) throws IOException {
            super.write(kv);

            updateMinKey(kv);
            updateMaxKey(kv);

            updateMinSeqNumber(kv);
            updateMaxSeqNumber(kv);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Write key value " + kv.toString(keyType, valueType));
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
        protected DataFileMeta createResult(Path path, Metric metric) throws IOException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing data file " + path);
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

    private static class RollingKvWriter extends RollingFileWriter<KeyValue, DataFileMeta> {

        public RollingKvWriter(Supplier<KvFileWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private Supplier<KvFileWriter> createWriterFactory(int level) {
        return () -> {
            try {
                return new KvFileWriter(createFileWriterFactory(), pathFactory.newPath(), level);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private FileWriter.Factory<KeyValue, Metric> createFileWriterFactory() {
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        return MetricFileWriter.createFactory(
                writerFactory,
                kvSerializer::toRow,
                KeyValue.schema(keyType, valueType),
                fileStatsExtractor);
    }

    private RollingKvWriter createRollingKvWriter(int level, long targetFileSize) {
        return new RollingKvWriter(createWriterFactory(level), targetFileSize);
    }

    /** Creates {@link DataFileWriter}. */
    public static class Factory {

        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public DataFileWriter create(BinaryRowData partition, int bucket) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            return new DataFileWriter(
                    schemaId,
                    keyType,
                    valueType,
                    fileFormat.createWriterFactory(recordType),
                    fileFormat.createStatsExtractor(recordType).orElse(null),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    suggestedFileSize);
        }
    }
}
