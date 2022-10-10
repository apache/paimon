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

package org.apache.flink.table.store.file.io;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Factory to create {@link RecordReader}s for reading {@link KeyValue} files. */
public class KeyValueFileReaderFactory {

    private final SchemaManager schemaManager;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;

    // TODO introduce Map<SchemaId, readerFactory>
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final DataFilePathFactory pathFactory;

    private KeyValueFileReaderFactory(
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

    public RecordReader<KeyValue> createRecordReader(String fileName, int level)
            throws IOException {
        return new KeyValueDataFileRecordReader(
                readerFactory, pathFactory.toPath(fileName), keyType, valueType, level);
    }

    public static Builder builder(
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            FileStorePathFactory pathFactory) {
        return new Builder(schemaManager, schemaId, keyType, valueType, fileFormat, pathFactory);
    }

    /** Builder for {@link KeyValueFileReaderFactory}. */
    public static class Builder {

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

        private Builder(
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

        public Builder withKeyProjection(int[][] projection) {
            keyProjection = projection;
            applyProjection();
            return this;
        }

        public Builder withValueProjection(int[][] projection) {
            valueProjection = projection;
            applyProjection();
            return this;
        }

        public KeyValueFileReaderFactory build(BinaryRowData partition, int bucket) {
            return build(partition, bucket, true, Collections.emptyList());
        }

        public KeyValueFileReaderFactory build(
                BinaryRowData partition,
                int bucket,
                boolean projectKeys,
                @Nullable List<Predicate> filters) {
            int[][] keyProjection = projectKeys ? this.keyProjection : fullKeyProjection;
            RowType projectedKeyType = projectKeys ? this.projectedKeyType : keyType;

            RowType recordType = KeyValue.schema(keyType, valueType);
            int[][] projection =
                    KeyValue.project(keyProjection, valueProjection, keyType.getFieldCount());
            return new KeyValueFileReaderFactory(
                    schemaManager,
                    schemaId,
                    projectedKeyType,
                    projectedValueType,
                    fileFormat.createReaderFactory(recordType, projection, filters),
                    pathFactory.createDataFilePathFactory(partition, bucket));
        }

        private void applyProjection() {
            projectedKeyType = (RowType) Projection.of(keyProjection).project(keyType);
            projectedValueType = (RowType) Projection.of(valueProjection).project(valueType);
        }
    }
}
