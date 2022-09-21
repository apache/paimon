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
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.io.KeyValueDataFileWriter;
import org.apache.flink.table.store.file.io.RollingFileWriter;
import org.apache.flink.table.store.file.io.SingleFileWriter;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Writes {@link KeyValue}s into data files. */
public class DataFileWriter {

    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStatsExtractor fileStatsExtractor;
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
        Path changelogPath = pathFactory.newChangelogPath();
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        SingleFileWriter<KeyValue, Void> writer =
                new SingleFileWriter<KeyValue, Void>(
                        writerFactory, changelogPath, kvSerializer::toRow) {
                    @Override
                    public Void result() throws IOException {
                        return null;
                    }
                };
        writer.write(iterator);
        writer.close();
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
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                new RollingFileWriter<>(() -> createDataFileWriter(level), suggestedFileSize);
        writer.write(iterator);
        writer.close();
        return writer.result();
    }

    private KeyValueDataFileWriter createDataFileWriter(int level) {
        Path path = pathFactory.newPath();
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        return new KeyValueDataFileWriter(
                writerFactory,
                path,
                kvSerializer::toRow,
                keyType,
                valueType,
                fileStatsExtractor,
                schemaId,
                level);
    }

    public void delete(DataFileMeta file) {
        delete(file.fileName());
    }

    public void delete(String file) {
        FileUtils.deleteOrWarn(pathFactory.toPath(file));
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
