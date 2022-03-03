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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.stats.FieldStatsCollector;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RollingFile;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This file includes several {@link ManifestEntry}s, representing the additional changes since last
 * snapshot.
 */
public class ManifestFile {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFile.class);

    private final RowType partitionType;
    private final ManifestEntrySerializer serializer;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileStorePathFactory pathFactory;
    private final long suggestedFileSize;

    private ManifestFile(
            RowType partitionType,
            ManifestEntrySerializer serializer,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            BulkWriter.Factory<RowData> writerFactory,
            FileStorePathFactory pathFactory,
            long suggestedFileSize) {
        this.partitionType = partitionType;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.pathFactory = pathFactory;
        this.suggestedFileSize = suggestedFileSize;
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

    /**
     * Write several {@link ManifestEntry}s into manifest files.
     *
     * <p>NOTE: This method is atomic.
     */
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {
        ManifestRollingFile rollingFile = new ManifestRollingFile();
        List<ManifestFileMeta> result = new ArrayList<>();
        List<Path> filesToCleanUp = new ArrayList<>();
        try {
            rollingFile.write(entries.iterator(), result, filesToCleanUp);
        } catch (Throwable e) {
            LOG.warn("Exception occurs when writing manifest files. Cleaning up.", e);
            for (Path path : filesToCleanUp) {
                FileUtils.deleteOrWarn(path);
            }
            throw new RuntimeException(e);
        }
        return result;
    }

    public void delete(String fileName) {
        FileUtils.deleteOrWarn(pathFactory.toManifestFilePath(fileName));
    }

    private class ManifestRollingFile extends RollingFile<ManifestEntry, ManifestFileMeta> {

        private long numAddedFiles;
        private long numDeletedFiles;
        private FieldStatsCollector statsCollector;

        private ManifestRollingFile() {
            super(suggestedFileSize);
            resetMeta();
        }

        @Override
        protected Path newPath() {
            return pathFactory.newManifestFile();
        }

        @Override
        protected BulkWriter<RowData> newWriter(FSDataOutputStream out) throws IOException {
            return writerFactory.create(out);
        }

        @Override
        protected RowData toRowData(ManifestEntry entry) {
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
            }
            statsCollector.collect(entry.partition());

            return serializer.toRow(entry);
        }

        @Override
        protected ManifestFileMeta collectFile(Path path) throws IOException {
            ManifestFileMeta result =
                    new ManifestFileMeta(
                            path.getName(),
                            path.getFileSystem().getFileStatus(path).getLen(),
                            numAddedFiles,
                            numDeletedFiles,
                            statsCollector.extract());
            resetMeta();
            return result;
        }

        private void resetMeta() {
            numAddedFiles = 0;
            numDeletedFiles = 0;
            statsCollector = new FieldStatsCollector(partitionType);
        }
    }

    /**
     * Creator of {@link ManifestFile}. It reueses {@link BulkFormat} and {@link BulkWriter.Factory}
     * from {@link FileFormat}.
     */
    public static class Factory {

        private final RowType partitionType;
        private final RowType keyType;
        private final RowType valueType;
        private final BulkFormat<RowData, FileSourceSplit> readerFactory;
        private final BulkWriter.Factory<RowData> writerFactory;
        private final FileStorePathFactory pathFactory;
        private final long suggestedFileSize;

        public Factory(
                RowType partitionType,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory,
                long suggestedFileSize) {
            this.partitionType = partitionType;
            this.keyType = keyType;
            this.valueType = valueType;
            RowType entryType = ManifestEntry.schema(partitionType, keyType, valueType);
            this.readerFactory = fileFormat.createReaderFactory(entryType);
            this.writerFactory = fileFormat.createWriterFactory(entryType);
            this.pathFactory = pathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public ManifestFile create() {
            return new ManifestFile(
                    partitionType,
                    new ManifestEntrySerializer(partitionType, keyType, valueType),
                    readerFactory,
                    writerFactory,
                    pathFactory,
                    suggestedFileSize);
        }
    }
}
