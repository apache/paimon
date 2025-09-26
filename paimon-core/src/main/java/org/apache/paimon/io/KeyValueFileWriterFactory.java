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
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
    private final FileWriterContextFactory formatContext;
    private final long suggestedFileSize;
    private final CoreOptions options;
    private final FileIndexOptions fileIndexOptions;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            FileWriterContextFactory formatContext,
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
        return formatContext.pathFactory(new WriteFormatKey(level, false));
    }

    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(
            int level, FileSource fileSource) {
        WriteFormatKey key = new WriteFormatKey(level, false);
        return new RollingFileWriterImpl<>(
                () -> {
                    DataFilePathFactory pathFactory = formatContext.pathFactory(key);
                    return createDataFileWriter(
                            pathFactory.newPath(), key, fileSource, pathFactory.isExternalPath());
                },
                suggestedFileSize);
    }

    public RollingFileWriterImpl<KeyValue, DataFileMeta> createRollingChangelogFileWriter(
            int level) {
        WriteFormatKey key = new WriteFormatKey(level, true);
        return new RollingFileWriterImpl<>(
                () -> {
                    DataFilePathFactory pathFactory = formatContext.pathFactory(key);
                    return createDataFileWriter(
                            pathFactory.newChangelogPath(),
                            key,
                            FileSource.APPEND,
                            pathFactory.isExternalPath());
                },
                suggestedFileSize);
    }

    private KeyValueDataFileWriter createDataFileWriter(
            Path path, WriteFormatKey key, FileSource fileSource, boolean isExternalPath) {
        return formatContext.thinModeEnabled
                ? new KeyValueThinDataFileWriterImpl(
                        fileIO,
                        formatContext.fileWriterContext(key),
                        path,
                        new KeyValueThinSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        schemaId,
                        key.level,
                        options,
                        fileSource,
                        fileIndexOptions,
                        isExternalPath)
                : new KeyValueDataFileWriterImpl(
                        fileIO,
                        formatContext.fileWriterContext(key),
                        path,
                        new KeyValueSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        schemaId,
                        key.level,
                        options,
                        fileSource,
                        fileIndexOptions,
                        isExternalPath);
    }

    public void deleteFile(DataFileMeta file) {
        // this path factory is only for path generation, so we don't care about the true or false
        // in WriteFormatKey
        fileIO.deleteQuietly(
                formatContext.pathFactory(new WriteFormatKey(file.level(), false)).toPath(file));
    }

    public FileIO getFileIO() {
        return fileIO;
    }

    public String newChangelogFileName(int level) {
        return formatContext.pathFactory(new WriteFormatKey(level, true)).newChangelogFileName();
    }

    public static Builder builder(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            Function<String, FileStorePathFactory> format2PathFactory,
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
        private final Function<String, FileStorePathFactory> format2PathFactory;
        private final long suggestedFileSize;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                Function<String, FileStorePathFactory> format2PathFactory,
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
            FileWriterContextFactory context =
                    new FileWriterContextFactory(
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

    private static class FileWriterContextFactory {

        private final Function<WriteFormatKey, String> key2Format;
        private final Function<WriteFormatKey, String> key2Compress;
        private final Function<WriteFormatKey, String> key2Stats;

        private final Map<Pair<String, String>, Optional<SimpleStatsExtractor>>
                formatStats2Extractor;
        private final Map<String, SimpleColStatsCollector.Factory[]> statsMode2AvroStats;
        private final Map<String, DataFilePathFactory> format2PathFactory;
        private final Map<String, FileFormat> formatFactory;
        private final Map<String, FormatWriterFactory> format2WriterFactory;

        private final BinaryRow partition;
        private final int bucket;
        private final RowType keyType;
        private final RowType valueType;
        private final RowType writeRowType;
        private final Function<String, FileStorePathFactory> parentFactories;
        private final CoreOptions options;
        private final boolean thinModeEnabled;

        private FileWriterContextFactory(
                BinaryRow partition,
                int bucket,
                RowType keyType,
                RowType valueType,
                FileFormat defaultFileFormat,
                Function<String, FileStorePathFactory> parentFactories,
                CoreOptions options) {
            this.partition = partition;
            this.bucket = bucket;
            this.keyType = keyType;
            this.valueType = valueType;
            this.parentFactories = parentFactories;
            this.options = options;
            this.thinModeEnabled =
                    options.dataFileThinMode() && supportsThinMode(keyType, valueType);
            this.writeRowType =
                    KeyValue.schema(thinModeEnabled ? RowType.of() : keyType, valueType);

            Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
            String defaultFormat = defaultFileFormat.getFormatIdentifier();
            @Nullable String changelogFormat = options.changelogFileFormat();
            this.key2Format =
                    key -> {
                        if (key.isChangelog && changelogFormat != null) {
                            return changelogFormat;
                        }
                        return fileFormatPerLevel.getOrDefault(key.level, defaultFormat);
                    };

            String defaultCompress = options.fileCompression();
            @Nullable String changelogCompression = options.changelogFileCompression();
            Map<Integer, String> fileCompressionPerLevel = options.fileCompressionPerLevel();
            this.key2Compress =
                    key -> {
                        if (key.isChangelog && changelogCompression != null) {
                            return changelogCompression;
                        }
                        return fileCompressionPerLevel.getOrDefault(key.level, defaultCompress);
                    };

            String statsMode = options.statsMode();
            Map<Integer, String> statsModePerLevel = options.statsModePerLevel();
            @Nullable String changelogStatsMode = options.changelogFileStatsMode();
            this.key2Stats =
                    key -> {
                        if (key.isChangelog && changelogStatsMode != null) {
                            return changelogStatsMode;
                        }
                        return statsModePerLevel.getOrDefault(key.level, statsMode);
                    };

            this.formatStats2Extractor = new HashMap<>();
            this.statsMode2AvroStats = new HashMap<>();
            this.format2PathFactory = new HashMap<>();
            this.format2WriterFactory = new HashMap<>();
            this.formatFactory = new HashMap<>();
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

        private FileWriterContext fileWriterContext(WriteFormatKey key) {
            return new FileWriterContext(
                    writerFactory(key), statsProducer(key), key2Compress.apply(key));
        }

        private SimpleStatsProducer statsProducer(WriteFormatKey key) {
            String format = key2Format.apply(key);
            String statsMode = key2Stats.apply(key);
            if (format.equals("avro")) {
                // In avro format, minValue, maxValue, and nullCount are not counted, so use
                // SimpleStatsExtractor to collect stats
                SimpleColStatsCollector.Factory[] factories =
                        statsMode2AvroStats.computeIfAbsent(
                                statsMode,
                                k ->
                                        StatsCollectorFactories.createStatsFactoriesForAvro(
                                                statsMode, options, writeRowType.getFieldNames()));
                SimpleStatsCollector collector = new SimpleStatsCollector(writeRowType, factories);
                return SimpleStatsProducer.fromCollector(collector);
            }

            Optional<SimpleStatsExtractor> extractor =
                    formatStats2Extractor.computeIfAbsent(
                            Pair.of(format, statsMode),
                            k -> createSimpleStatsExtractor(format, statsMode));
            return SimpleStatsProducer.fromExtractor(extractor.orElse(null));
        }

        private Optional<SimpleStatsExtractor> createSimpleStatsExtractor(
                String format, String statsMode) {
            SimpleColStatsCollector.Factory[] statsFactories =
                    StatsCollectorFactories.createStatsFactories(
                            statsMode,
                            options,
                            writeRowType.getFieldNames(),
                            thinModeEnabled ? keyType.getFieldNames() : Collections.emptyList());
            boolean isDisabled =
                    Arrays.stream(SimpleColStatsCollector.create(statsFactories))
                            .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
            if (isDisabled) {
                return Optional.empty();
            }
            return fileFormat(format).createStatsExtractor(writeRowType, statsFactories);
        }

        private DataFilePathFactory pathFactory(WriteFormatKey key) {
            String format = key2Format.apply(key);
            return format2PathFactory.computeIfAbsent(
                    format,
                    k ->
                            parentFactories
                                    .apply(format)
                                    .createDataFilePathFactory(partition, bucket));
        }

        private FormatWriterFactory writerFactory(WriteFormatKey key) {
            return format2WriterFactory.computeIfAbsent(
                    key2Format.apply(key),
                    format -> fileFormat(format).createWriterFactory(writeRowType));
        }

        private FileFormat fileFormat(String format) {
            return formatFactory.computeIfAbsent(
                    format, k -> FileFormat.fromIdentifier(format, options.toConfiguration()));
        }
    }

    private static class WriteFormatKey {

        private final int level;
        private final boolean isChangelog;

        private WriteFormatKey(int level, boolean isChangelog) {
            this.level = level;
            this.isChangelog = isChangelog;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriteFormatKey formatKey = (WriteFormatKey) o;
            return level == formatKey.level && isChangelog == formatKey.isChangelog;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, isChangelog);
        }
    }
}
