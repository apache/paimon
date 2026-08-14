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
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Index manifest file. */
public class IndexManifestFile extends ObjectsFile<IndexManifestEntry> {

    private final FileFormat fileFormat;
    private final RowType manifestType;

    private IndexManifestFile(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType schema,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                new IndexManifestEntrySerializer(),
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
        this.fileFormat = fileFormat;
        this.manifestType = schema;
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
            CloseableIterator<InternalRow> rows =
                    FileUtils.createFormatReader(
                                    fileIO,
                                    fileFormat.createReaderFactory(
                                            manifestType,
                                            projection.projectedType(),
                                            Collections.emptyList()),
                                    pathFactory.toPath(fileName),
                                    null)
                            .toCloseableIterator();
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
                    fileFormat,
                    schema,
                    fileFormat.createReaderFactory(schema, schema, new ArrayList<>()),
                    fileFormat.createWriterFactory(schema),
                    compression,
                    pathFactory.indexManifestFileFactory(),
                    cache);
        }
    }
}
