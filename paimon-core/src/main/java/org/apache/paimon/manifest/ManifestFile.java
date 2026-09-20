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

import org.apache.paimon.CoreOptions;
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
import org.apache.paimon.utils.RowRangeIndex;
import org.apache.paimon.utils.SegmentsCache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

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
    private final CoreOptions options;
    @Nullable private final SegmentsCache<Path> sidecarCache;
    @Nullable private CacheMetrics cacheMetrics;

    private ManifestFile(
            FileIO fileIO,
            SchemaManager schemaManager,
            RowType partitionType,
            AvroFileFormat avroFileFormat,
            ManifestEntrySerializer serializer,
            String compression,
            PathFactory pathFactory,
            long suggestedFileSize,
            @Nullable SegmentsCache<Path> cache,
            @Nullable SegmentsCache<Path> sidecarCache,
            CoreOptions options) {
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
        this.options = options;
        this.sidecarCache = sidecarCache == null ? cache : sidecarCache;
    }

    @Override
    protected ManifestEntryCache createCache(
            @Nullable SegmentsCache<Path> cache, RowType formatType) {
        return new ManifestEntryCache(
                cache,
                serializer,
                formatType,
                super::fileSize,
                this::createIterator,
                (path, fileSize, partitionFilter, bucketFilter) ->
                        createManifestIterator(
                                fileIO,
                                path,
                                ManifestEntry.MANIFEST_ROW_TYPE,
                                partitionFilter,
                                bucketFilter));
    }

    @Override
    public ManifestFile withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        super.withCacheMetrics(cacheMetrics);
        this.cacheMetrics = cacheMetrics;
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
        return read(
                fileName,
                fileSize,
                partitionFilter,
                bucketFilter,
                readFilter,
                readTFilter,
                convertor,
                null);
    }

    public <T> List<T> read(
            String fileName,
            @Nullable Long fileSize,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            Filter<InternalRow> readFilter,
            Filter<ManifestEntry> readTFilter,
            Function<ManifestEntry, T> convertor,
            @Nullable ManifestSidecar.Selection selected) {
        if (selected != null && selected.blocks().isEmpty()) {
            return java.util.Collections.emptyList();
        }
        try {
            Path path = pathFactory.toPath(fileName);
            // Sidecar selections use the block cache, even when every block is selected.
            if (cache != null && selected == null) {
                ManifestEntryFilters filters =
                        new ManifestEntryFilters(
                                partitionFilter, bucketFilter, readFilter, readTFilter);
                return cache.read(path, fileSize, filters, convertor);
            }

            CacheMetrics metrics = cacheMetrics;
            ManifestSidecar.CacheStatus cacheStatus =
                    selected != null && cache != null && metrics != null
                            ? new ManifestSidecar.CacheStatus()
                            : null;
            try {
                CloseableIterator<InternalRow> iterator =
                        createManifestIterator(
                                fileIO,
                                path,
                                ManifestEntry.MANIFEST_ROW_TYPE,
                                partitionFilter,
                                bucketFilter,
                                selected,
                                cache == null ? null : cache.segmentsCache(),
                                cacheStatus);
                return readFromIterator(iterator, serializer, readFilter, readTFilter, convertor);
            } finally {
                if (cacheStatus != null) {
                    if (cacheStatus.hit()) {
                        metrics.increaseHitObject();
                    } else {
                        metrics.increaseMissedObject();
                    }
                }
            }
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
        return scan(fileName, projection, null, null);
    }

    /**
     * Scans projected manifest entries and prunes partitions and buckets before materializing the
     * nested data file row.
     */
    public CloseableIterator<ProjectedManifestEntry> scan(
            String fileName,
            Projection projection,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter) {
        try {
            CloseableIterator<InternalRow> rows =
                    createManifestIterator(
                            fileIO,
                            pathFactory.toPath(fileName),
                            projection.projectedType(),
                            partitionFilter,
                            bucketFilter);
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
        return createManifestIterator(
                fileIO, path, projectedType, partitionFilter, bucketFilter, null, null, null);
    }

    private static CloseableIterator<InternalRow> createManifestIterator(
            FileIO fileIO,
            Path path,
            RowType projectedType,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter,
            @Nullable ManifestSidecar.Selection selected,
            @Nullable SegmentsCache<Object> cache,
            @Nullable ManifestSidecar.CacheStatus cacheStatus)
            throws IOException {
        try {
            ManifestAvroReader reader =
                    new ManifestAvroReader(
                            ManifestSidecar.openManifest(
                                    fileIO, path, selected, cache, cacheStatus));
            return reader.read(projectedType, partitionFilter, bucketFilter);
        } catch (IOException e) {
            FileUtils.checkExists(fileIO, path);
            throw e;
        }
    }

    /** Opens a low-allocation reader over raw Avro manifest blocks. */
    public ManifestAvroReader scanAvroBlocks(String fileName, @Nullable Long fileSize) {
        try {
            return new ManifestAvroReader(fileIO.newInputStream(pathFactory.toPath(fileName)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read manifest file " + fileName, e);
        }
    }

    /** Opens a low-allocation reader for the encoded manifest fields needed by run merge. */
    public ManifestAvroReader scanForRunMerge(String fileName, @Nullable Long fileSize) {
        return scanAvroBlocks(fileName, fileSize);
    }

    @VisibleForTesting
    public long suggestedFileSize() {
        return suggestedFileSize;
    }

    public List<ExpireFileEntry> readExpireFileEntries(String fileName) {
        return readExpireFileEntries(fileName, null, entry -> true);
    }

    /**
     * Reads only expiring entries accepted by the supplied filters.
     *
     * <p>The bucket filter is evaluated by the Avro reader before nested data-file metadata is
     * decoded. The entry filter then runs on a reusable projected view, before an {@link
     * ExpireFileEntry} is materialized. The entry filter must not retain its argument.
     */
    public List<ExpireFileEntry> readExpireFileEntries(
            String fileName,
            @Nullable BucketFilter bucketFilter,
            Predicate<ProjectedManifestEntry> entryFilter) {
        List<ExpireFileEntry> result = new ArrayList<>();
        ProjectedManifestEntry entry = EXPIRE_FILE_PROJECTION.createEntry();
        try (ManifestAvroReader reader = scanAvroBlocks(fileName, null)) {
            while (reader.hasNext()) {
                ManifestAvroReader.RowIterator rows =
                        reader.next()
                                .toRows(EXPIRE_FILE_PROJECTION.projectedType(), null, bucketFilter);
                while (rows.hasNext()) {
                    entry.replace(rows.next());
                    if (entryFilter.test(entry)) {
                        result.add(ExpireFileEntry.from(entry));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to scan expiring entries from manifest file '%s'.", fileName),
                    e);
        } finally {
            entry.clear();
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
                suggestedFileSize,
                options);
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
                Long.MAX_VALUE,
                options);
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

    @Nullable
    public ManifestSidecar.Selection selectBlocks(
            ManifestFileMeta manifest, @Nullable RowRangeIndex query) {
        return selectBlocks(manifest, query, null, null);
    }

    @Nullable
    public ManifestSidecar.Selection selectBlocks(
            ManifestFileMeta manifest,
            @Nullable RowRangeIndex query,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable BucketFilter bucketFilter) {
        boolean hasFilter = query != null || partitionFilter != null || bucketFilter != null;
        return !options.manifestSidecarEnabled() || (!hasFilter && cache == null)
                ? null
                : ManifestSidecar.read(
                        fileIO,
                        pathFactory.toPath(manifest.fileName()),
                        manifest,
                        query,
                        partitionFilter,
                        partitionType,
                        bucketFilter == null ? null : bucketFilter::mayContain,
                        sidecarCache);
    }

    /** Deletes an unreferenced manifest and its explicitly referenced extra files. */
    public void delete(ManifestFileMeta manifest) {
        delete(manifest.fileName());
        if (manifest.extraFiles() != null) {
            manifest.extraFiles().forEach(this::delete);
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
        private final CoreOptions options;
        @Nullable private final SegmentsCache<Path> cache;
        @Nullable private final SegmentsCache<Path> sidecarCache;

        public Factory(
                FileIO fileIO,
                SchemaManager schemaManager,
                RowType partitionType,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                long suggestedFileSize,
                @Nullable SegmentsCache<Path> cache,
                @Nullable SegmentsCache<Path> sidecarCache,
                CoreOptions options) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
            this.cache = cache;
            this.sidecarCache = sidecarCache;
            this.options = options;
        }

        /** Returns whether a manifest of this size is eligible for the configured cache. */
        public boolean isCacheable(long fileSize) {
            return cache != null && fileSize <= cache.maxElementSize();
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
                    cache,
                    sidecarCache,
                    options);
        }
    }
}
