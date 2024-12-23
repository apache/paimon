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
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.KeyValueThinSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** A factory to create {@link FileWriter}s for writing {@link KeyValue} files. */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final WriteFormatContext formatContext;
    private final long suggestedFileSize;
    private final CoreOptions options;
    private final FileIndexOptions fileIndexOptions;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            WriteFormatContext formatContext,
            long suggestedFileSize,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = formatContext.keyType;
        this.valueType = formatContext.valueType;
        this.formatContext = formatContext;
        this.suggestedFileSize = suggestedFileSize;
        this.options = options;
        this.fileIndexOptions = options.indexColumnsOptions();
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

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(
            int level, FileSource fileSource) {
        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                formatContext.pathFactory(level).newPath(), level, fileSource),
                suggestedFileSize);
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {
        return new RollingFileWriter<>(
                () ->
                        createDataFileWriter(
                                formatContext.pathFactory(level).newChangelogPath(),
                                level,
                                FileSource.APPEND),
                suggestedFileSize);
    }

    private KeyValueDataFileWriter createDataFileWriter(
            Path path, int level, FileSource fileSource) {
        return formatContext.thinModeEnabled()
                ? new KeyValueThinDataFileWriterImpl(
                        fileIO,
                        formatContext.writerFactory(level),
                        path,
                        new KeyValueThinSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        formatContext.extractor(level),
                        schemaId,
                        level,
                        formatContext.compression(level),
                        options,
                        fileSource,
                        fileIndexOptions)
                : new KeyValueDataFileWriterImpl(
                        fileIO,
                        formatContext.writerFactory(level),
                        path,
                        new KeyValueSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        formatContext.extractor(level),
                        schemaId,
                        level,
                        formatContext.compression(level),
                        options,
                        fileSource,
                        fileIndexOptions);
    }

    public void deleteFile(String filename, int level, String externalPath) {
        fileIO.deleteQuietly(formatContext.pathFactory(level).toPath(filename, externalPath));
    }

    public void copyFile(
            String sourceFileName, String targetFileName, int level, String externalPath)
            throws IOException {
        Path sourcePath = formatContext.pathFactory(level).toPath(sourceFileName, externalPath);
        Path targetPath = formatContext.pathFactory(level).toPath(targetFileName, externalPath);
        fileIO.copyFile(sourcePath, targetPath, true);
    }

    public FileIO getFileIO() {
        return fileIO;
    }

    public Path newChangelogPath(int level) {
        return formatContext.pathFactory(level).newChangelogPath();
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
            WriteFormatContext context =
                    new WriteFormatContext(
                            partition,
                            bucket,
                            keyType,
                            valueType,
                            fileFormat,
                            format2PathFactory,
                            options);
            return new KeyValueFileWriterFactory(
                    fileIO, schemaId, context, suggestedFileSize, options);
        }
    }

    private static class WriteFormatContext {

        private final Function<Integer, String> level2Format;
        private final Function<Integer, String> level2Compress;

        private final Map<String, Optional<SimpleStatsExtractor>> format2Extractor;
        private final Map<String, DataFilePathFactory> format2PathFactory;
        private final Map<String, FormatWriterFactory> format2WriterFactory;

        private final RowType keyType;
        private final RowType valueType;
        private final boolean thinModeEnabled;

        private WriteFormatContext(
                BinaryRow partition,
                int bucket,
                RowType keyType,
                RowType valueType,
                FileFormat defaultFormat,
                Map<String, FileStorePathFactory> parentFactories,
                CoreOptions options) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.thinModeEnabled =
                    options.dataFileThinMode() && supportsThinMode(keyType, valueType);
            RowType writeRowType =
                    KeyValue.schema(thinModeEnabled ? RowType.of() : keyType, valueType);
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
            SimpleColStatsCollector.Factory[] statsCollectorFactories =
                    StatsCollectorFactories.createStatsFactories(
                            options,
                            writeRowType.getFieldNames(),
                            thinModeEnabled ? keyType.getFieldNames() : Collections.emptyList());
            for (String format : parentFactories.keySet()) {
                format2PathFactory.put(
                        format,
                        parentFactories.get(format).createDataFilePathFactory(partition, bucket));

                FileFormat fileFormat =
                        FileFormat.fromIdentifier(format, options.toConfiguration());
                // In avro format, minValue, maxValue, and nullCount are not counted, set
                // StatsExtractor is Optional.empty() and will use SimpleStatsExtractor to collect
                // stats
                format2Extractor.put(
                        format,
                        format.equals("avro")
                                ? Optional.empty()
                                : fileFormat.createStatsExtractor(
                                        writeRowType, statsCollectorFactories));
                format2WriterFactory.put(format, fileFormat.createWriterFactory(writeRowType));
            }
        }

        private boolean supportsThinMode(RowType keyType, RowType valueType) {
            Set<Integer> keyFieldIds =
                    valueType.getFields().stream().map(DataField::id).collect(Collectors.toSet());

            for (DataField field : keyType.getFields()) {
                if (!SpecialFields.isKeyField(field.name())) {
                    return false;
                }
                if (!keyFieldIds.contains(field.id() - SpecialFields.KEY_FIELD_ID_START)) {
                    return false;
                }
            }
            return true;
        }

        private boolean thinModeEnabled() {
            return thinModeEnabled;
        }

        @Nullable
        private SimpleStatsExtractor extractor(int level) {
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
    }
}
