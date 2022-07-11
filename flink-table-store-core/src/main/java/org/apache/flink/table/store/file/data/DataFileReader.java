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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.KeyValueSerializer;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Reads {@link KeyValue}s from data files.
 *
 * <p>NOTE: If the key exists, the data is sorted according to the key and the key projection will
 * cause the orderliness of the data to fail.
 */
public class DataFileReader {

    private final SchemaManager schemaManager;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;

    // TODO introduce Map<SchemaId, readerFactory>
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final DataFilePathFactory pathFactory;

    private DataFileReader(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            DataFilePathFactory pathFactory) {
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.readerFactory = readerFactory;
        this.pathFactory = pathFactory;
    }

    public RecordReader<KeyValue> read(String fileName) throws IOException {
        return new DataFileRecordReader(pathFactory.toPath(fileName));
    }

    private class DataFileRecordReader implements RecordReader<KeyValue> {

        private final BulkFormat.Reader<RowData> reader;
        private final KeyValueSerializer serializer;

        private DataFileRecordReader(Path path) throws IOException {
            long fileSize = FileUtils.getFileSize(path);
            FileSourceSplit split = new FileSourceSplit("ignore", path, 0, fileSize);
            this.reader = readerFactory.createReader(FileUtils.DEFAULT_READER_CONFIG, split);
            this.serializer =
                    new KeyValueSerializer(
                            keyType, valueType, schemaManager.schemaIfExists(schemaId));
        }

        @Nullable
        @Override
        public RecordIterator<KeyValue> readBatch() throws IOException {
            BulkFormat.RecordIterator<RowData> iterator = reader.readBatch();
            return iterator == null ? null : new DataFileRecordIterator(iterator, serializer);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static class DataFileRecordIterator implements RecordReader.RecordIterator<KeyValue> {

        private final BulkFormat.RecordIterator<RowData> iterator;
        private final KeyValueSerializer serializer;

        private DataFileRecordIterator(
                BulkFormat.RecordIterator<RowData> iterator, KeyValueSerializer serializer) {
            this.iterator = iterator;
            this.serializer = serializer;
        }

        @Override
        public KeyValue next() throws IOException {
            RecordAndPosition<RowData> result = iterator.next();

            // TODO schema evolution
            return result == null ? null : serializer.fromRow(result.getRecord());
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }

    /** Creates {@link DataFileReader}. */
    public static class Factory {

        private final SchemaManager schemaManager;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        private final int[][] fullKeyProjection;
        private int[][] keyProjection;
        private int[][] valueProjection;
        private RowType projectedKeyType;
        private RowType projectedValueType;

        public Factory(
                SchemaManager schemaManager,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                FileStorePathFactory pathFactory) {
            this.schemaManager = schemaManager;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;

            this.fullKeyProjection = Projection.range(0, keyType.getFieldCount()).toNestedIndexes();
            this.keyProjection = fullKeyProjection;
            this.valueProjection = Projection.range(0, valueType.getFieldCount()).toNestedIndexes();
            applyProjection();
        }

        public Factory withKeyProjection(int[][] projection) {
            keyProjection = projection;
            applyProjection();
            return this;
        }

        public Factory withValueProjection(int[][] projection) {
            valueProjection = projection;
            applyProjection();
            return this;
        }

        public DataFileReader create(BinaryRowData partition, int bucket) {
            return create(partition, bucket, true);
        }

        public DataFileReader create(BinaryRowData partition, int bucket, boolean projectKeys) {
            int[][] keyProjection = projectKeys ? this.keyProjection : fullKeyProjection;
            RowType projectedKeyType = projectKeys ? this.projectedKeyType : keyType;

            RowType recordType = KeyValue.schema(keyType, valueType);
            int[][] projection =
                    KeyValue.project(keyProjection, valueProjection, keyType.getFieldCount());
            return new DataFileReader(
                    schemaManager,
                    schemaId,
                    projectedKeyType,
                    projectedValueType,
                    fileFormat.createReaderFactory(recordType, projection),
                    pathFactory.createDataFilePathFactory(partition, bucket));
        }

        private void applyProjection() {
            projectedKeyType = (RowType) Projection.of(keyProjection).project(keyType);
            projectedValueType = (RowType) Projection.of(valueProjection).project(valueType);
        }
    }
}
