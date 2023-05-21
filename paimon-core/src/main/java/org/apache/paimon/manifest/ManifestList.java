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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.List;

/**
 * This file includes several {@link ManifestFileMeta}, representing all data of the whole table at
 * the corresponding snapshot.
 */
public class ManifestList extends ObjectsFile<ManifestFileMeta> {

    private final FormatWriterFactory writerFactory;

    private ManifestList(
            FileIO fileIO,
            ManifestFileMetaSerializer serializer,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory,
            @Nullable SegmentsCache<String> cache) {
        super(fileIO, serializer, readerFactory, pathFactory, cache);
        this.writerFactory = writerFactory;
    }

    /**
     * Write several {@link ManifestFileMeta}s into a manifest list.
     *
     * <p>NOTE: This method is atomic.
     */
    public String write(List<ManifestFileMeta> metas) {
        Path path = pathFactory.newPath();
        try {
            try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
                FormatWriter writer =
                        writerFactory.create(out, CoreOptions.FILE_COMPRESSION.defaultValue());
                try {
                    for (ManifestFileMeta record : metas) {
                        writer.addElement(serializer.toRow(record));
                    }
                } finally {
                    writer.flush();
                    writer.finish();
                }
            }
            return path.getName();
        } catch (Throwable e) {
            fileIO.deleteQuietly(path);
            throw new RuntimeException(
                    "Exception occurs when writing manifest list " + path + ". Clean up.", e);
        }
    }

    /** Creator of {@link ManifestList}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;
        @Nullable private final SegmentsCache<String> cache;

        public Factory(
                FileIO fileIO,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                @Nullable SegmentsCache<String> cache) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
            this.cache = cache;
        }

        public ManifestList create() {
            RowType metaType = VersionedObjectSerializer.versionType(ManifestFileMeta.schema());
            return new ManifestList(
                    fileIO,
                    new ManifestFileMetaSerializer(),
                    fileFormat.createReaderFactory(metaType),
                    fileFormat.createWriterFactory(metaType),
                    pathFactory.manifestListFactory(),
                    cache);
        }
    }
}
