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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.io.RollingFileWriterImpl;
import org.apache.paimon.io.SingleFileWriter;
import org.apache.paimon.manifest.ProjectedManifestEntry.Projection;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile extends ObjectsFile<ManifestEntry> {

    private static final Projection EXPIRE_FILE_PROJECTION = createExpireFileProjection();

    private final SchemaManager schemaManager;
    private final RowType partitionType;
    private final AvroFileFormat avroFileFormat;
    private final FormatWriterFactory writerFactory;
    private final long suggestedFileSize;

    private ManifestFile(
            FileIO fileIO,
            SchemaManager schemaManager,
            RowType partitionType,
            AvroFileFormat avroFileFormat,
            ManifestEntrySerializer serializer,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            long suggestedFileSize,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                serializer,
                ManifestEntry.MANIFEST_ROW_TYPE,
                (path, ignoredFileSize) ->
                        createManifestIterator(
                                fileIO,
                                path,
                                avroFileFormat,
                                ManifestEntry.MANIFEST_ROW_TYPE,
                                null,
                                null),
                writerFactory,
                compression,
                pathFactory,
                cache);
        this.schemaManager = schemaManager;
        this.partitionType = partitionType;
        this.avroFileFormat = avroFileFormat;
        this.writerFactory = writerFactory;
        this.suggestedFileSize = suggestedFileSize;
    }

    @Override
    protected ManifestEntryCache createCache(
            @Nullable SegmentsCache<Path> cache, RowType formatType) {
        return new ManifestEntryCache(
                cache, serializer, formatType, super::fileSize, this::createIterator);
    }

    @Override
    public ManifestFile withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        super.withCacheMetrics(cacheMetrics);
        return this;
    }

    public List<ManifestEntry> read(
            String fileName,
            @Nullable Long fileSize,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readTFilter) {
        return read(
                fileName,
                fileSize,
                partitionFilter,
                bucketFilter,
                readFilter,
                readTFilter,
                Function.identity());
    }

    public <T> List<T> read(
            String fileName,
            @Nullable Long fileSize,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readTFilter,
            Function<ManifestEntry, T> convertor) {
        try {
            Path path = pathFactory.toPath(fileName);
            if (cache != null) {
                ManifestEntryFilters filters =
                        new ManifestEntryFilters(
                                partitionFilter, bucketFilter, readFilter, readTFilter);
                return cache.read(path, fileSize, filters, convertor);
            }

            CloseableIterator<InternalRow> iterator =
                    createManifestIterator(
                            fileIO,
                            path,
                            avroFileFormat,
                            ManifestEntry.MANIFEST_ROW_TYPE,
                            partitionFilter,
                            bucketFilter);
            return readFromIterator(iterator, serializer, readFilter, readTFilter, convertor);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Scans projected manifest entries without materializing {@link PojoManifestEntry}s.
     *
     * <p>Every call to {@code next()} creates a {@link ProjectedManifestEntry} wrapper around the
     * reader's reusable backing row. The returned entry must be consumed before the iterator
     * advances and cannot be retained after the iterator advances or closes. The caller must close
     * the iterator.
     *
     * <p>This method intentionally bypasses the manifest cache because cached entries are
     * materialized with the complete manifest schema.
     */
    public CloseableIterator<ProjectedManifestEntry> scan(String fileName, Projection projection) {
        try {
            CloseableIterator<InternalRow> rows =
                    createManifestIterator(
                            fileIO,
                            pathFactory.toPath(fileName),
                            avroFileFormat,
                            projection.projectedType(),
                            null,
                            null);
            return new CloseableIterator<ProjectedManifestEntry>() {

                @Override
                public boolean hasNext() {
                    return rows.hasNext();
                }

                @Override
                public ProjectedManifestEntry next() {
                    return projection.createEntry().replace(rows.next());
                }

                @Override
                public void close() throws Exception {
                    rows.close();
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read manifest file " + fileName, e);
        }
    }

    private static CloseableIterator<InternalRow> createManifestIterator(
            FileIO fileIO,
            Path path,
            AvroFileFormat avroFileFormat,
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter)
            throws IOException {
        try {
            ManifestAvroReader reader =
                    new ManifestAvroReader(fileIO.newInputStream(path), avroFileFormat);
            return reader.read(projectedType, partitionFilter, bucketFilter);
        } catch (IOException e) {
            FileUtils.checkExists(fileIO, path);
            throw e;
        }
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    public List<ExpireFileEntry> readExpireFileEntries(String fileName) {
        List<ExpireFileEntry> result = new ArrayList<>();
        try (CloseableIterator<ProjectedManifestEntry> entries =
                scan(fileName, EXPIRE_FILE_PROJECTION)) {
            while (entries.hasNext()) {
                result.add(ExpireFileEntry.from(entries.next()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to scan expiring entries from manifest file '%s'.", fileName),
                    e);
        }
        return result;
    }

    private static Projection createExpireFileProjection() {
        RowType manifestType = ManifestEntry.MANIFEST_ROW_TYPE;
        return Projection.create(
                new RowType(
                        false,
                        Arrays.asList(
                                manifestType.getField(ManifestEntry.KIND),
                                manifestType.getField(ManifestEntry.PARTITION),
                                manifestType.getField(ManifestEntry.BUCKET),
                                manifestType.getField(ManifestEntry.TOTAL_BUCKETS),
                                manifestType
                                        .getField(ManifestEntry.FILE)
                                        .newType(
                                                DataFileMeta.SCHEMA.project(
                                                        DataFileMeta.FILE_NAME,
                                                        DataFileMeta.ROW_COUNT,
                                                        DataFileMeta.MIN_KEY,
                                                        DataFileMeta.MAX_KEY,
                                                        DataFileMeta.LEVEL,
                                                        DataFileMeta.EXTRA_FILES,
                                                        DataFileMeta.EMBEDDED_FILE_INDEX,
                                                        DataFileMeta.FILE_SOURCE,
                                                        DataFileMeta.EXTERNAL_PATH,
                                                        DataFileMeta.FIRST_ROW_ID)))));
    }

    /**
     * Write several {@link ManifestEntry}s into manifest files.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = createRollingWriter();
        try {
            writer.write(entries);
            writer.close();
            return writer.result();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public RollingFileWriter<ManifestEntry, ManifestFileMeta> createRollingWriter() {
        return new RollingFileWriterImpl<>(
                () -> new ManifestEntryWriter(writerFactory, pathFactory.newPath(), compression),
                suggestedFileSize,
                Long.MAX_VALUE);
    }

    public ManifestEntryWriter createManifestEntryWriter(Path manifestPath) {
        return new ManifestEntryWriter(writerFactory, manifestPath, compression);
    }

    /** Writer for {@link ManifestEntry}. */
    public class ManifestEntryWriter extends SingleFileWriter<ManifestEntry, ManifestFileMeta> {

        private final SimpleStatsCollector partitionStatsCollector;
        private final SimpleStatsConverter partitionStatsSerializer;

        private long numAddedFiles = 0;
        private long numDeletedFiles = 0;
        private long schemaId = Long.MIN_VALUE;
        private int minBucket = Integer.MAX_VALUE;
        private int maxBucket = Integer.MIN_VALUE;
        private int minLevel = Integer.MAX_VALUE;
        private int maxLevel = Integer.MIN_VALUE;
        private @Nullable RowIdStats rowIdStats = new RowIdStats();

        ManifestEntryWriter(FormatWriterFactory factory, Path path, String fileCompression) {
            super(
                    ManifestFile.this.fileIO,
                    factory,
                    path,
                    serializer::toRow,
                    fileCompression,
                    false);
            this.partitionStatsCollector = new SimpleStatsCollector(partitionType);
            this.partitionStatsSerializer = new SimpleStatsConverter(partitionType);
        }

        @Override
        public void write(ManifestEntry entry) throws IOException {
            if (entry instanceof ProjectedManifestEntry) {
                writeRow(((ProjectedManifestEntry) entry).fullRow());
            } else {
                super.write(entry);
            }

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
            schemaId = Math.max(schemaId, entry.file().schemaId());
            minBucket = Math.min(minBucket, entry.bucket());
            maxBucket = Math.max(maxBucket, entry.bucket());
            minLevel = Math.min(minLevel, entry.level());
            maxLevel = Math.max(maxLevel, entry.level());
            if (rowIdStats != null) {
                Long firstRowId = entry.file().firstRowId();
                if (firstRowId == null) {
                    rowIdStats = null;
                } else {
                    rowIdStats.collect(firstRowId, entry.file().rowCount());
                }
            }

            partitionStatsCollector.collect(entry.partition());
        }

        @Override
        public ManifestFileMeta result() throws IOException {
            return new ManifestFileMeta(
                    path.getName(),
                    outputBytes(),
                    numAddedFiles,
                    numDeletedFiles,
                    partitionStatsSerializer.toBinaryAllMode(partitionStatsCollector.extract()),
                    numAddedFiles + numDeletedFiles > 0
                            ? schemaId
                            : schemaManager.latest().get().id(),
                    minBucket,
                    maxBucket,
                    minLevel,
                    maxLevel,
                    rowIdStats == null ? null : rowIdStats.minRowId,
                    rowIdStats == null ? null : rowIdStats.maxRowId);
        }
    }

    private static class RowIdStats {

        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;

        private void collect(long firstRowId, long rowCount) {
            minRowId = Math.min(minRowId, firstRowId);
            maxRowId = Math.max(maxRowId, firstRowId + rowCount - 1);
        }
    }

    /** Creator of {@link ManifestFile}. */
    public static class Factory {

        private final FileIO fileIO;
        private final SchemaManager schemaManager;
        private final RowType partitionType;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;
        @Nullable private final SegmentsCache<Path> cache;

        public Factory(
                FileIO fileIO,
                SchemaManager schemaManager,
                RowType partitionType,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                long suggestedFileSize,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
            this.cache = cache;
        }

        public boolean isCacheEnabled() {
            return cache != null;
        }

        public ManifestFile create() {
            RowType entryType = ManifestEntry.MANIFEST_ROW_TYPE;
            return new ManifestFile(
                    fileIO,
                    schemaManager,
                    partitionType,
                    (AvroFileFormat) fileFormat,
                    new ManifestEntrySerializer(),
                    fileFormat.createWriterFactory(entryType),
                    compression,
                    pathFactory.manifestFileFactory(),
                    suggestedFileSize,
                    cache);
        }
    }
}
