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

package org.apache.flink.table.store.file.io;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStatsExtractor fileStatsExtractor;
    private final DataFilePathFactory pathFactory;
    private final long suggestedFileSize;

    private KeyValueFileWriterFactory(
            long schemaId,
            RowType keyType,
            RowType valueType,
            BulkWriter.Factory<RowData> writerFactory,
            FileStatsExtractor fileStatsExtractor,
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
    public DataFilePathFactory pathFactory() {
        return pathFactory;
    }

    public KeyValueDataFileWriter createLevel0Writer() {
        return createLeveledWriterImpl(0);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createLeveledWriter(int level) {
        return new RollingFileWriter<>(() -> createLeveledWriterImpl(level), suggestedFileSize);
    }

    private KeyValueDataFileWriter createLeveledWriterImpl(int level) {
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

    public SingleFileWriter<KeyValue, Void> createExtraFileWriter() throws IOException {
        Path changelogPath = pathFactory.newChangelogPath();
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        return new SingleFileWriter<KeyValue, Void>(
                writerFactory, changelogPath, kvSerializer::toRow) {
            @Override
            public Void result() throws IOException {
                return null;
            }
        };
    }

    public void deleteFile(String filename) {
        FileUtils.deleteOrWarn(pathFactory.toPath(filename));
    }

    public static Builder builder(
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            long suggestedFileSize) {
        return new Builder(
                schemaId, keyType, valueType, fileFormat, pathFactory, suggestedFileSize);
    }

    /** Builder of {@link KeyValueFileWriterFactory}. */
    public static class Builder {

        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        private Builder(
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

        public KeyValueFileWriterFactory build(BinaryRowData partition, int bucket) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            return new KeyValueFileWriterFactory(
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
