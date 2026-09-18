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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/** Index manifest file. */
public class IndexManifestFile extends ObjectsFile<IndexManifestEntry> {

    private final AvroFileFormat avroFileFormat;

    private IndexManifestFile(
            FileIO fileIO,
            AvroFileFormat avroFileFormat,
            RowType schema,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                new IndexManifestEntrySerializer(),
                schema,
                (path, ignoredFileSize) -> createIndexManifestIterator(fileIO, path, schema),
                avroFileFormat.createWriterFactory(schema),
                compression,
                pathFactory,
                cache);
        this.avroFileFormat = avroFileFormat;
    }

    public Path indexManifestFilePath(String fileName) {
        return pathFactory.toPath(fileName);
    }

    /**
     * Scans projected index manifest entries without materializing {@link IndexManifestEntry}s.
     *
     * <p>The returned iterator reuses the same mutable {@link BinaryIndexManifestEntry} for all
     * records. An entry is only valid until the next call to {@link CloseableIterator#hasNext()},
     * {@link CloseableIterator#next()}, or {@link CloseableIterator#close()}, and must not be
     * retained. The caller must close the iterator.
     *
     * <p>This method intentionally bypasses the manifest cache because cached entries are
     * materialized with the complete index manifest schema.
     */
    public CloseableIterator<BinaryIndexManifestEntry> scan(
            String fileName, BinaryIndexManifestEntry.Projection projection) {
        BinaryIndexManifestEntry entry = projection.createEntry();
        try {
            IndexManifestAvroReader reader =
                    new IndexManifestAvroReader(
                            fileIO.newInputStream(pathFactory.toPath(fileName)));
            CloseableIterator<InternalRow> rows = reader.read(projection.projectedType(), true);
            return new CloseableIterator<BinaryIndexManifestEntry>() {
                @Override
                public boolean hasNext() {
                    entry.clear();
                    return rows.hasNext();
                }

                @Override
                public BinaryIndexManifestEntry next() {
                    entry.clear();
                    InternalRow row = rows.next();
                    return row == null ? null : entry.replace(row);
                }

                @Override
                public void close() throws Exception {
                    entry.clear();
                    rows.close();
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read index manifest " + fileName, e);
        }
    }

    /** Opens a block-aware reader over one index manifest. */
    IndexManifestAvroReader scanAvroBlocks(String fileName) {
        try {
            return new IndexManifestAvroReader(fileIO.newInputStream(pathFactory.toPath(fileName)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read index manifest " + fileName, e);
        }
    }

    /** Creates a one-file index manifest Avro writer. */
    IndexManifestAvroWriter createAvroWriter() {
        return new IndexManifestAvroWriter(
                fileIO, avroFileFormat, serializer, compression, pathFactory);
    }

    /** Returns whether this complete index manifest is already materialized in the cache. */
    boolean isCached(String fileName) {
        return cache != null
                && cache.segmentsCache().getIfPresents(pathFactory.toPath(fileName)) != null;
    }

    private static CloseableIterator<InternalRow> createIndexManifestIterator(
            FileIO fileIO, Path path, RowType projectedType) throws IOException {
        try {
            return new IndexManifestAvroReader(fileIO.newInputStream(path)).read(projectedType);
        } catch (IOException e) {
            org.apache.paimon.utils.FileUtils.checkExists(fileIO, path);
            throw e;
        }
    }

    /** Write new index files to index manifest. */
    @Nullable
    public String writeIndexFiles(
            @Nullable String previousIndexManifest,
            List<IndexManifestEntry> newIndexFiles,
            BucketMode bucketMode) {
        if (newIndexFiles.isEmpty()) {
            return previousIndexManifest;
        }
        IndexManifestFileHandler handler = new IndexManifestFileHandler(this, bucketMode);
        return handler.write(previousIndexManifest, newIndexFiles);
    }

    /** Creator of {@link IndexManifestFile}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        @Nullable private final SegmentsCache<Path> cache;

        public Factory(
                FileIO fileIO,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.cache = cache;
        }

        public IndexManifestFile create() {
            RowType schema = IndexManifestEntry.MANIFEST_ROW_TYPE;
            return new IndexManifestFile(
                    fileIO,
                    (AvroFileFormat) fileFormat,
                    schema,
                    compression,
                    pathFactory.indexManifestFileFactory(),
                    cache);
        }
    }
}
