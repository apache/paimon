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
import org.apache.paimon.statistics.FieldStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final WriteFormatContext formatContext;
    private final long suggestedFileSize;
    private final CoreOptions options;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            WriteFormatContext formatContext,
            long suggestedFileSize,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.formatContext = formatContext;
        this.suggestedFileSize = suggestedFileSize;
        this.options = options;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public DataFilePathFactory pathFactory(int level) {
        return formatContext.pathFactory(level);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(int level) {
        return new RollingFileWriter<>(
                () -> {
                    KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
                    return new KeyValueDataFileWriter(
                            fileIO,
                            formatContext.writerFactory(level),
                            formatContext.pathFactory(level).newPath(),
                            kvSerializer::toRow,
                            keyType,
                            valueType,
                            formatContext.extractor(level),
                            schemaId,
                            level,
                            formatContext.compression(level),
                            options);
                },
                suggestedFileSize);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {
        return new RollingFileWriter<>(
                () -> {
                    KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
                    return new KeyValueDataFileWriter(
                            fileIO,
                            formatContext.changelogWriterFactory(),
                            formatContext.changelogPathFactory().newChangelogPath(),
                            kvSerializer::toRow,
                            keyType,
                            valueType,
                            formatContext.changelogExtractor(),
                            schemaId,
                            level,
                            formatContext.changelogCompression(),
                            options);
                },
                suggestedFileSize);
    }

    public void deleteFile(String filename, int level) {
        fileIO.deleteQuietly(formatContext.pathFactory(level).toPath(filename));
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
                BinaryRow partition, int bucket, CoreOptions options) {
            RowType fileRowType = KeyValue.schema(keyType, valueType);
            WriteFormatContext context =
                    new WriteFormatContext(
                            partition,
                            bucket,
                            fileRowType,
                            fileFormat,
                            format2PathFactory,
                            options);
            return new KeyValueFileWriterFactory(
                    fileIO, schemaId, keyType, valueType, context, suggestedFileSize, options);
        }
    }

    private static class WriteFormatContext {

        // data files format
        private Function<Integer, String> level2Format;
        private Function<Integer, String> level2Compress;

        private Map<String, Optional<TableStatsExtractor>> format2Extractor;
        private Map<String, DataFilePathFactory> format2PathFactory;
        private Map<String, FormatWriterFactory> format2WriterFactory;

        // changelog format
        private FileFormat changelogFormat;
        private FormatWriterFactory changelogFormatWriterFactory;
        private DataFilePathFactory changelogFormatPathFactory;
        private Optional<TableStatsExtractor> changelogFormatStatsExtractor;
        private String changelogCompression;

        private WriteFormatContext(
                BinaryRow partition,
                int bucket,
                RowType rowType,
                FileFormat defaultFormat,
                Map<String, FileStorePathFactory> parentFactories,
                CoreOptions options) {
            FieldStatsCollector.Factory[] statsCollectorFactories =
                    StatsCollectorFactories.createStatsFactories(options, rowType.getFieldNames());
            // data file format
            initDataFileFormat(
                    partition,
                    bucket,
                    rowType,
                    defaultFormat,
                    parentFactories,
                    options,
                    statsCollectorFactories);
            // changelog file format
            initChangelogFormat(
                    partition, bucket, rowType, parentFactories, options, statsCollectorFactories);
        }

        private void initDataFileFormat(
                BinaryRow partition,
                int bucket,
                RowType rowType,
                FileFormat defaultFormat,
                Map<String, FileStorePathFactory> parentFactories,
                CoreOptions options,
                FieldStatsCollector.Factory[] statsCollectorFactories) {
            Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
            this.level2Format =
                    level ->
                            fileFormatPerLevel.getOrDefault(
                                    level, defaultFormat.getFormatIdentifier());
            String defaultCompress = options.fileCompression();
            Map<Integer, String> fileCompressionPerLevel = options.fileCompressionPerLevel();
            this.level2Compress =
                    level -> fileCompressionPerLevel.getOrDefault(level, defaultCompress);

            this.format2Extractor = new HashMap<>();
            this.format2PathFactory = new HashMap<>();
            this.format2WriterFactory = new HashMap<>();

            for (String format : parentFactories.keySet()) {
                format2PathFactory.put(
                        format,
                        parentFactories.get(format).createDataFilePathFactory(partition, bucket));

                FileFormat fileFormat = FileFormat.getFileFormat(options.toConfiguration(), format);
                format2Extractor.put(
                        format, fileFormat.createStatsExtractor(rowType, statsCollectorFactories));
                format2WriterFactory.put(format, fileFormat.createWriterFactory(rowType));
            }
        }

        private void initChangelogFormat(
                BinaryRow partition,
                int bucket,
                RowType rowType,
                Map<String, FileStorePathFactory> parentFactories,
                CoreOptions options,
                FieldStatsCollector.Factory[] statsCollectorFactories) {
            this.changelogFormat = options.changelogFormat();
            this.changelogFormatPathFactory =
                    parentFactories
                            .get(changelogFormat)
                            .createDataFilePathFactory(partition, bucket);
            this.changelogFormatStatsExtractor =
                    this.changelogFormat.createStatsExtractor(rowType, statsCollectorFactories);
            this.changelogFormatWriterFactory = this.changelogFormat.createWriterFactory(rowType);
            // compress config
            this.changelogCompression = options.changelogCompression() != null ? options.changelogCompression() : options.fileCompression();
        }

        @Nullable
        private TableStatsExtractor extractor(int level) {
            return format2Extractor.get(level2Format.apply(level)).orElse(null);
        }

        private DataFilePathFactory pathFactory(int level) {
            return format2PathFactory.get(level2Format.apply(level));
        }

        private FormatWriterFactory writerFactory(int level) {
            return format2WriterFactory.get(level2Format.apply(level));
        }

        private String compression(int level) {
            return level2Compress.apply(level);
        }

        @Nullable
        private TableStatsExtractor changelogExtractor() {
            return changelogFormatStatsExtractor.orElse(null);
        }

        private DataFilePathFactory changelogPathFactory() {
            return changelogFormatPathFactory;
        }

        private FormatWriterFactory changelogWriterFactory() {
            return this.changelogFormatWriterFactory;
        }

        private String changelogCompression() {
            return this.changelogCompression;
        }
    }
}
