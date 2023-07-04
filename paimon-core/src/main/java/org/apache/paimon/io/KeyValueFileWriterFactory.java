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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final RowType recordType;
    private final Map<String, FormatWriterFactory> writerFactoryMap;
    @Nullable private final TableStatsExtractor tableStatsExtractor;
    private final Map<String, DataFilePathFactory> pathFactoryMap;
    private final long suggestedFileSize;
    private final Map<Integer, String> levelCompressions;
    private final String fileCompression;
    private final Map<Integer, String> levelFormats;
    private final FileFormat fileFormat;
    private final CoreOptions options;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            RowType recordType,
            FileFormat fileFormat,
            Map<String, FormatWriterFactory> writerFactoryMap,
            @Nullable TableStatsExtractor tableStatsExtractor,
            Map<String, DataFilePathFactory> pathFactoryMap,
            long suggestedFileSize,
            Map<Integer, String> levelCompressions,
            String fileCompression,
            Map<Integer, String> levelFormats,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.recordType = recordType;
        this.fileFormat = fileFormat;
        this.writerFactoryMap = writerFactoryMap;
        this.tableStatsExtractor = tableStatsExtractor;
        this.pathFactoryMap = pathFactoryMap;
        this.suggestedFileSize = suggestedFileSize;
        this.levelCompressions = levelCompressions;
        this.fileCompression = fileCompression;
        this.levelFormats = levelFormats;
        this.options = options;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public DataFilePathFactory pathFactory(String format) {
        return pathFactoryMap.get(format);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(int level) {
        String fileFormat = getFileFormat(level);
        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                pathFactoryMap.get(fileFormat).newPath(),
                                level,
                                getCompression(level)),
                suggestedFileSize);
    }

    private String getCompression(int level) {
        if (null == levelCompressions) {
            return fileCompression;
        } else {
            return levelCompressions.getOrDefault(level, fileCompression);
        }
    }

    private String getFileFormat(int level) {
        if (null == levelFormats) {
            return fileFormat.getFormatIdentifier();
        } else {
            return levelFormats.getOrDefault(level, fileFormat.getFormatIdentifier());
        }
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {

        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                pathFactoryMap.get(getFileFormat(level)).newChangelogPath(),
                                level,
                                getCompression(level)),
                suggestedFileSize);
    }

    private KeyValueDataFileWriter createDataFileWriter(Path path, int level, String compression) {
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        String fileFormat = getFileFormat(level);
        return new KeyValueDataFileWriter(
                fileIO,
                writerFactoryMap.get(fileFormat),
                path,
                kvSerializer::toRow,
                keyType,
                valueType,
                getTableStatsExtractor(fileFormat),
                schemaId,
                level,
                compression,
                options);
    }

    private TableStatsExtractor getTableStatsExtractor(String fileFormat) {
        return null == fileFormat
                ? tableStatsExtractor
                : FileFormat.fromIdentifier(fileFormat, options.toConfiguration())
                        .createStatsExtractor(
                                recordType,
                                StatsCollectorFactories.createStatsFactories(
                                        options, recordType.getFieldNames()))
                        .orElse(null);
    }

    public void deleteFile(String filename, int level) {
        fileIO.deleteQuietly(pathFactoryMap.get(getFileFormat(level)).toPath(filename));
    }

    public static Builder builder(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            Map<String, FileStorePathFactory> format2PathFactory,
            long suggestedFileSize) {
        return new Builder(
                fileIO,
                schemaId,
                keyType,
                valueType,
                fileFormat,
                format2PathFactory,
                suggestedFileSize);
    }

    /** Builder of {@link KeyValueFileWriterFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final Map<String, FileStorePathFactory> format2PathFactory;
        private final long suggestedFileSize;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                Map<String, FileStorePathFactory> format2PathFactory,
                long suggestedFileSize) {
            this.fileIO = fileIO;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.format2PathFactory = format2PathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public KeyValueFileWriterFactory build(
                BinaryRow partition,
                int bucket,
                Map<Integer, String> levelCompressions,
                String fileCompression,
                Map<Integer, String> levelFormats,
                CoreOptions options) {
            RowType recordType = KeyValue.schema(keyType, valueType);

            Map<String, FormatWriterFactory> writerFactoryMap = new HashMap<>();
            writerFactoryMap.put(
                    fileFormat.getFormatIdentifier(), fileFormat.createWriterFactory(recordType));
            if (null != levelFormats) {
                for (String fileFormat : levelFormats.values()) {
                    writerFactoryMap.putIfAbsent(
                            fileFormat,
                            FileFormat.fromIdentifier(fileFormat, options.toConfiguration())
                                    .createWriterFactory(recordType));
                }
            }

            Map<String, DataFilePathFactory> dataFilePathFactoryMap =
                    format2PathFactory.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            e ->
                                                    e.getValue()
                                                            .createDataFilePathFactory(
                                                                    partition, bucket)));

            return new KeyValueFileWriterFactory(
                    fileIO,
                    schemaId,
                    keyType,
                    valueType,
                    recordType,
                    fileFormat,
                    writerFactoryMap,
                    fileFormat
                            .createStatsExtractor(
                                    recordType,
                                    StatsCollectorFactories.createStatsFactories(
                                            options, recordType.getFieldNames()))
                            .orElse(null),
                    dataFilePathFactoryMap,
                    suggestedFileSize,
                    levelCompressions,
                    fileCompression,
                    levelFormats,
                    options);
        }
    }
}
