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

package org.apache.flink.table.store.file.data;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.format.FileFormat;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** Reads {@link RowData} from data files. */
public class AppendOnlyReader {

    private final SchemaManager schemaManager;
    private final long schemaId;

    // TODO introduce Map<SchemaId, readerFactory>
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final DataFilePathFactory pathFactory;

    private AppendOnlyReader(
            SchemaManager schemaManager,
            long schemaId,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            DataFilePathFactory pathFactory) {
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.readerFactory = readerFactory;
        this.pathFactory = pathFactory;
    }

    public RecordReader<RowData> read(String fileName) throws IOException {
        System.out.println("read " + fileName);
        return new AppendOnlyRecordReader(pathFactory.toPath(fileName));
    }

    private class AppendOnlyRecordReader implements RecordReader<RowData> {

        private final BulkFormat.Reader<RowData> reader;

        private AppendOnlyRecordReader(Path path) throws IOException {
            long fileSize = FileUtils.getFileSize(path);
            FileSourceSplit split = new FileSourceSplit("ignore", path, 0, fileSize, 0, fileSize);
            this.reader = readerFactory.createReader(FileUtils.DEFAULT_READER_CONFIG, split);
        }

        @Nullable
        @Override
        public RecordIterator<RowData> readBatch() throws IOException {
            BulkFormat.RecordIterator<RowData> iterator = reader.readBatch();
            return iterator == null ? null : new AppendOnlyRecordIterator(iterator);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class AppendOnlyRecordIterator implements RecordReader.RecordIterator<RowData> {

        private final BulkFormat.RecordIterator<RowData> iterator;

        private AppendOnlyRecordIterator(BulkFormat.RecordIterator<RowData> iterator) {
            this.iterator = iterator;
        }

        @Override
        public RowData next() throws IOException {
            RecordAndPosition<RowData> result = iterator.next();

            // TODO schema evolution
            return result == null ? null : result.getRecord();
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }

    /** Creates {@link AppendOnlyReader}. */
    public static class Factory {

        private final SchemaManager schemaManager;
        private final long schemaId;
        private final RowType rowType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        private int[][] projection;

        public Factory(
                SchemaManager schemaManager,
                long schemaId,
                RowType rowType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.schemaManager = schemaManager;
            this.schemaId = schemaId;
            this.rowType = rowType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;

            this.projection = Projection.range(0, rowType.getFieldCount()).toNestedIndexes();
        }

        public Factory withProjection(int[][] projection) {
            this.projection = projection;
            return this;
        }

        public AppendOnlyReader create(BinaryRowData partition, int bucket) {
            return new AppendOnlyReader(
                    schemaManager,
                    schemaId,
                    fileFormat.createReaderFactory(rowType, projection),
                    pathFactory.createDataFilePathFactory(partition, bucket));
        }
    }
}
