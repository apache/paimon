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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.VersionedObjectSerializer;
import org.apache.flink.table.store.file.writer.BaseFileWriter;
import org.apache.flink.table.store.file.writer.FileWriter;
import org.apache.flink.table.store.file.writer.Metric;
import org.apache.flink.table.store.file.writer.MetricFileWriter;
import org.apache.flink.table.store.file.writer.RollingFileWriter;
import org.apache.flink.table.store.format.FieldStatsCollector;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FileStatsExtractor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFile.class);

    private final SchemaManager schemaManager;
    private final long schemaId;
    private final RowType partitionType;
    private final ManifestEntrySerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final FileStorePathFactory pathFactory;
    private final long suggestedFileSize;
    private final FileWriter.Factory<ManifestEntry, Metric> fileWriterFactory;

    private ManifestFile(
            SchemaManager schemaManager,
            long schemaId,
            RowType partitionType,
            RowType entryType,
            ManifestEntrySerializer serializer,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            BulkWriter.Factory<RowData> writerFactory,
            FileStatsExtractor fileStatsExtractor,
            FileStorePathFactory pathFactory,
            long suggestedFileSize) {
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.partitionType = partitionType;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;

        // Initialize the metric file writer factory to write manifest entry and generate metrics.
        this.fileWriterFactory =
                MetricFileWriter.createFactory(
                        writerFactory, serializer::toRow, entryType, fileStatsExtractor);
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    public List<ManifestEntry> read(String fileName) {
        try {
            return FileUtils.readListFromFile(
                    pathFactory.toManifestFilePath(fileName), serializer, readerFactory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest file " + fileName, e);
        }
    }

    public Iterable<ManifestEntry> readManifestFiles(List<String> manifestFiles) {
        Queue<String> files = new LinkedList<>(manifestFiles);
        return Iterables.concat(
                (Iterable<Iterable<ManifestEntry>>)
                        () ->
                                new Iterator<Iterable<ManifestEntry>>() {
                                    @Override
                                    public boolean hasNext() {
                                        return files.size() > 0;
                                    }

                                    @Override
                                    public Iterable<ManifestEntry> next() {
                                        return read(files.poll());
                                    }
                                });
    }

    /**
     * Write several {@link ManifestEntry}s into manifest files.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {

        ManifestRollingWriter rollingWriter = createManifestRollingWriter(suggestedFileSize);
        try (ManifestRollingWriter writer = rollingWriter) {
            writer.write(entries);

        } catch (Exception e) {
            LOG.warn("Exception occurs when writing manifest files. Cleaning up.", e);

            rollingWriter.abort();
            throw new RuntimeException(e);
        }

        return rollingWriter.result();
    }

    public void delete(String fileName) {
        FileUtils.deleteOrWarn(pathFactory.toManifestFilePath(fileName));
    }

    private class ManifestEntryWriter extends BaseFileWriter<ManifestEntry, ManifestFileMeta> {

        private final FieldStatsCollector partitionStatsCollector;
        private final FieldStatsArraySerializer partitionStatsSerializer;
        private long numAddedFiles = 0;
        private long numDeletedFiles = 0;

        ManifestEntryWriter(FileWriter.Factory<ManifestEntry, Metric> writerFactory, Path path)
                throws IOException {
            super(writerFactory, path);

            this.partitionStatsCollector = new FieldStatsCollector(partitionType);
            this.partitionStatsSerializer = new FieldStatsArraySerializer(partitionType);
        }

        @Override
        public void write(ManifestEntry entry) throws IOException {
            super.write(entry);

            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind());
            }

            partitionStatsCollector.collect(entry.partition());
        }

        @Override
        protected ManifestFileMeta createResult(Path path, Metric ignore) throws IOException {
            // The input metric will be ignored because it includes all the column's stats, rather
            // than the partition stats.
            return new ManifestFileMeta(
                    path.getName(),
                    path.getFileSystem().getFileStatus(path).getLen(),
                    numAddedFiles,
                    numDeletedFiles,
                    partitionStatsSerializer.toBinary(partitionStatsCollector.extract()),
                    schemaId);
        }
    }

    private static class ManifestRollingWriter
            extends RollingFileWriter<ManifestEntry, ManifestFileMeta> {

        public ManifestRollingWriter(
                Supplier<ManifestEntryWriter> writerFactory, long targetFileSize) {
            super(writerFactory, targetFileSize);
        }
    }

    private Supplier<ManifestEntryWriter> createWriterFactory() {
        return () -> {
            try {
                return new ManifestEntryWriter(fileWriterFactory, pathFactory.newManifestFile());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private ManifestRollingWriter createManifestRollingWriter(long targetFileSize) {
        return new ManifestRollingWriter(createWriterFactory(), targetFileSize);
    }

    /**
     * Creator of {@link ManifestFile}. It reueses {@link BulkFormat} and {@link BulkWriter.Factory}
     * from {@link FileFormat}.
     */
    public static class Factory {

        private final SchemaManager schemaManager;
        private final long schemaId;
        private final RowType partitionType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                SchemaManager schemaManager,
                long schemaId,
                RowType partitionType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.schemaManager = schemaManager;
            this.schemaId = schemaId;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public ManifestFile create() {
            RowType entryType = VersionedObjectSerializer.versionType(ManifestEntry.schema());
            return new ManifestFile(
                    schemaManager,
                    schemaId,
                    partitionType,
                    entryType,
                    new ManifestEntrySerializer(),
                    fileFormat.createReaderFactory(entryType),
                    fileFormat.createWriterFactory(entryType),
                    fileFormat.createStatsExtractor(entryType).orElse(null),
                    pathFactory,
                    suggestedFileSize);
        }
    }
}
