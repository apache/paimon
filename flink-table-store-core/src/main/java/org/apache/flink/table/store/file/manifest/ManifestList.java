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

import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.VersionedObjectSerializer;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.format.FormatReaderFactory;
import org.apache.flink.table.store.format.FormatWriter;
import org.apache.flink.table.store.format.FormatWriterFactory;
import org.apache.flink.table.store.fs.FileIO;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.PositionOutputStream;
import org.apache.flink.table.store.types.RowType;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several {@link ManifestFileMeta}, representing all data of the whole table at
 * the corresponding snapshot.
 */
public class ManifestList {

    private final FileIO fileIO;
    private final ManifestFileMetaSerializer serializer;
    private final FormatReaderFactory readerFactory;
    private final FormatWriterFactory writerFactory;
    private final FileStorePathFactory pathFactory;

    private ManifestList(
            FileIO fileIO,
            ManifestFileMetaSerializer serializer,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            FileStorePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
    }

    public List<ManifestFileMeta> read(String fileName) {
        try {
            return FileUtils.readListFromFile(
                    fileIO, pathFactory.toManifestListPath(fileName), serializer, readerFactory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest list " + fileName, e);
        }
    }

    /**
     * Write several {@link ManifestFileMeta}s into a manifest list.
     *
     * <p>NOTE: This method is atomic.
     */
    public String write(List<ManifestFileMeta> metas) {
        Path path = pathFactory.newManifestList();
        try {
            return write(metas, path);
        } catch (Throwable e) {
            fileIO.deleteQuietly(path);
            throw new RuntimeException(
                    "Exception occurs when writing manifest list " + path + ". Clean up.", e);
        }
    }

    private String write(List<ManifestFileMeta> metas, Path path) throws IOException {
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            // Initialize the bulk writer to accept the ManifestFileMeta.
            FormatWriter writer = writerFactory.create(out);
            try {
                for (ManifestFileMeta manifest : metas) {
                    writer.addElement(serializer.toRow(manifest));
                }
            } finally {
                writer.flush();
                writer.finish();
            }
        }
        return path.getName();
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toManifestListPath(fileName));
    }

    /** Creator of {@link ManifestList}. */
    public static class Factory {

        private final FileIO fileIO;
        private final RowType partitionType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        public Factory(
                FileIO fileIO,
                RowType partitionType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.fileIO = fileIO;
            this.partitionType = partitionType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
        }

        public ManifestList create() {
            RowType metaType = VersionedObjectSerializer.versionType(ManifestFileMeta.schema());
            return new ManifestList(
                    fileIO,
                    new ManifestFileMetaSerializer(),
                    fileFormat.createReaderFactory(metaType),
                    fileFormat.createWriterFactory(metaType),
                    pathFactory);
        }
    }
}
