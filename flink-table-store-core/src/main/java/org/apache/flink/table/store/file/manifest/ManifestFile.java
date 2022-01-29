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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile {

    private final RowType partitionType;
    private final ManifestEntrySerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStorePathFactory pathFactory;

    private ManifestFile(
            RowType partitionType,
            ManifestEntrySerializer serializer,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            BulkWriter.Factory<RowData> writerFactory,
            FileStorePathFactory pathFactory) {
        this.partitionType = partitionType;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
    }

    public List<ManifestEntry> read(String fileName) {
        try {
            return FileUtils.readListFromFile(
                    pathFactory.toManifestFilePath(fileName), serializer, readerFactory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read manifest file " + fileName, e);
        }
    }

    /**
     * Write several {@link ManifestEntry}s into a manifest file.
     *
     * <p>NOTE: This method is atomic.
     */
    public ManifestFileMeta write(List<ManifestEntry> entries) {
        Preconditions.checkArgument(
                entries.size() > 0, "Manifest entries to write must not be empty.");

        Path path = pathFactory.newManifestFile();
        try {
            return write(entries, path);
        } catch (Throwable e) {
            FileUtils.deleteOrWarn(path);
            throw new RuntimeException(
                    "Exception occurs when writing manifest file " + path + ". Clean up.", e);
        }
    }

    private ManifestFileMeta write(List<ManifestEntry> entries, Path path) throws IOException {
        FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = writerFactory.create(out);
        long numAddedFiles = 0;
        long numDeletedFiles = 0;
        FieldStatsCollector statsCollector = new FieldStatsCollector(partitionType);

        for (ManifestEntry entry : entries) {
            writer.addElement(serializer.toRow(entry));
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
            }
            statsCollector.collect(entry.partition());
        }
        writer.finish();
        out.close();

        return new ManifestFileMeta(
                path.getName(),
                path.getFileSystem().getFileStatus(path).getLen(),
                numAddedFiles,
                numDeletedFiles,
                statsCollector.extract());
    }

    public void delete(String fileName) {
        FileUtils.deleteOrWarn(pathFactory.toManifestFilePath(fileName));
    }

    /**
     * Creator of {@link ManifestFile}. It reueses {@link BulkFormat} and {@link BulkWriter.Factory}
     * from {@link FileFormat}.
     */
    public static class Factory {

        private final RowType partitionType;
        private final RowType keyType;
        private final RowType rowType;
        private final BulkFormat<RowData, FileSourceSplit> readerFactory;
        private final BulkWriter.Factory<RowData> writerFactory;
        private final FileStorePathFactory pathFactory;

        public Factory(
                RowType partitionType,
                RowType keyType,
                RowType rowType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.partitionType = partitionType;
            this.keyType = keyType;
            this.rowType = rowType;
            RowType entryType = ManifestEntry.schema(partitionType, keyType, rowType);
            this.readerFactory = fileFormat.createReaderFactory(entryType);
            this.writerFactory = fileFormat.createWriterFactory(entryType);
            this.pathFactory = pathFactory;
        }

        public ManifestFile create() {
            return new ManifestFile(
                    partitionType,
                    new ManifestEntrySerializer(partitionType, keyType, rowType),
                    readerFactory,
                    writerFactory,
                    pathFactory);
        }
    }
}
