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

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.ColumnStatisticsCollectSkipper;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileStatsExtractor;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.function.Function;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final FormatWriterFactory writerFactory;

    private final DataFilePathFactory pathFactory;
    private final long suggestedFileSize;
    private final Map<Integer, String> levelCompressions;
    private final String fileCompression;

    private ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper;

    private Function<ColumnStatisticsCollectSkipper, FileStatsExtractor> fileStatsExtractorSupplier;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FormatWriterFactory writerFactory,
            @Nullable
                    Function<ColumnStatisticsCollectSkipper, FileStatsExtractor>
                            fileStatsExtractorSupplier,
            DataFilePathFactory pathFactory,
            long suggestedFileSize,
            Map<Integer, String> levelCompressions,
            String fileCompression,
            @Nullable ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.writerFactory = writerFactory;
        this.fileStatsExtractorSupplier = fileStatsExtractorSupplier;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
        this.levelCompressions = levelCompressions;
        this.fileCompression = fileCompression;
        this.columnStatisticsCollectSkipper = columnStatisticsCollectSkipper;
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
                fileStatsExtractorSupplier,
                schemaId,
                level,
                compression,
                columnStatisticsCollectSkipper);
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
            long suggestedFileSize,
            ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper) {
        return new Builder(
                fileIO,
                schemaId,
                keyType,
                valueType,
                fileFormat,
                pathFactory,
                suggestedFileSize,
                columnStatisticsCollectSkipper);
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

        private final ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize,
                @Nullable ColumnStatisticsCollectSkipper columnStatisticsCollectSkipper) {
            this.fileIO = fileIO;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
            this.columnStatisticsCollectSkipper = columnStatisticsCollectSkipper;
        }

        public KeyValueFileWriterFactory build(
                BinaryRow partition,
                int bucket,
                Map<Integer, String> levelCompressions,
                String fileCompression) {
            RowType recordType = KeyValue.schema(keyType, valueType);

            return new KeyValueFileWriterFactory(
                    fileIO,
                    schemaId,
                    keyType,
                    valueType,
                    fileFormat.createWriterFactory(recordType),
                    fileFormat.createStatsExtractorSupplier(recordType).orElse(null),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    suggestedFileSize,
                    levelCompressions,
                    fileCompression,
                    columnStatisticsCollectSkipper);
        }
    }
}
