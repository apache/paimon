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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.TableStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.util.Map;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final FormatWriterFactory writerFactory;
    @Nullable private final TableStatsExtractor tableStatsExtractor;
    private final DataFilePathFactory pathFactory;
    private final long suggestedFileSize;
    private final Map<Integer, String> levelCompressions;
    private final String fileCompression;
    private final CoreOptions options;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FormatWriterFactory writerFactory,
            @Nullable TableStatsExtractor tableStatsExtractor,
            DataFilePathFactory pathFactory,
            long suggestedFileSize,
            Map<Integer, String> levelCompressions,
            String fileCompression,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.tableStatsExtractor = tableStatsExtractor;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
        this.levelCompressions = levelCompressions;
        this.fileCompression = fileCompression;
        this.options = options;
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

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(int level) {
        return new RollingFileWriter<>(
                () -> createDataFileWriter(pathFactory.newPath(), level, getCompression(level)),
                suggestedFileSize);
    }

    private String getCompression(int level) {
        if (null == levelCompressions) {
            return fileCompression;
        } else {
            return levelCompressions.getOrDefault(level, fileCompression);
        }
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {
        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                pathFactory.newChangelogPath(), level, getCompression(level)),
                suggestedFileSize);
    }

    private KeyValueDataFileWriter createDataFileWriter(Path path, int level, String compression) {
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        return new KeyValueDataFileWriter(
                fileIO,
                writerFactory,
                path,
                kvSerializer::toRow,
                keyType,
                valueType,
                tableStatsExtractor,
                schemaId,
                level,
                compression,
                options);
    }

    public void deleteFile(String filename) {
        fileIO.deleteQuietly(pathFactory.toPath(filename));
    }

    public static Builder builder(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory,
            long suggestedFileSize) {
        return new Builder(
                fileIO, schemaId, keyType, valueType, fileFormat, pathFactory, suggestedFileSize);
    }

    /** Builder of {@link KeyValueFileWriterFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.fileIO = fileIO;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public KeyValueFileWriterFactory build(
                BinaryRow partition,
                int bucket,
                Map<Integer, String> levelCompressions,
                String fileCompression,
                CoreOptions options) {
            RowType recordType = KeyValue.schema(keyType, valueType);
            return new KeyValueFileWriterFactory(
                    fileIO,
                    schemaId,
                    keyType,
                    valueType,
                    fileFormat.createWriterFactory(recordType),
                    fileFormat
                            .createStatsExtractor(
                                    recordType,
                                    StatsCollectorFactories.createStatsFactories(
                                            options, recordType.getFieldNames()))
                            .orElse(null),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    suggestedFileSize,
                    levelCompressions,
                    fileCompression,
                    options);
        }
    }
}
