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
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ProjectedManifestEntry.Projection;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
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
    private final long suggestedFileSize;

    private ManifestFile(
            FileIO fileIO,
            SchemaManager schemaManager,
            RowType partitionType,
            AvroFileFormat avroFileFormat,
            ManifestEntrySerializer serializer,
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
                                fileIO, path, ManifestEntry.MANIFEST_ROW_TYPE, null, null),
                avroFileFormat.createWriterFactory(ManifestEntry.MANIFEST_ROW_TYPE),
                compression,
                pathFactory,
                cache);
        this.schemaManager = schemaManager;
        this.partitionType = partitionType;
        this.avroFileFormat = avroFileFormat;
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
     * <p>Every returned {@link ProjectedManifestEntry} has independent backing data and can be
     * retained after the iterator advances or closes. The caller must close the iterator.
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
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter)
            throws IOException {
        try {
            ManifestAvroReader reader = new ManifestAvroReader(fileIO.newInputStream(path));
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
        ManifestAvroWriter writer = createAvroWriter();
        try {
            writer.write(entries);
            writer.close();
            return writer.result();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Creates a rolling Avro manifest writer. */
    public ManifestAvroWriter createAvroWriter() {
        return new ManifestAvroWriter(
                fileIO,
                schemaManager,
                partitionType,
                avroFileFormat,
                serializer,
                compression,
                pathFactory,
                suggestedFileSize);
    }

    /** Creates an Avro manifest writer for one explicit path. */
    public ManifestAvroWriter createAvroWriter(Path manifestPath) {
        return new ManifestAvroWriter(
                fileIO,
                schemaManager,
                partitionType,
                avroFileFormat,
                serializer,
                compression,
                singlePathFactory(manifestPath),
                Long.MAX_VALUE);
    }

    private PathFactory singlePathFactory(Path manifestPath) {
        return new PathFactory() {

            private boolean created;

            @Override
            public Path newPath() {
                if (created) {
                    throw new IllegalStateException(
                            "Cannot create more than one fixed-path manifest file.");
                }
                created = true;
                return manifestPath;
            }

            @Override
            public Path toPath(String fileName) {
                return pathFactory.toPath(fileName);
            }
        };
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
            return new ManifestFile(
                    fileIO,
                    schemaManager,
                    partitionType,
                    (AvroFileFormat) fileFormat,
                    new ManifestEntrySerializer(),
                    compression,
                    pathFactory.manifestFileFactory(),
                    suggestedFileSize,
                    cache);
        }
    }
}
