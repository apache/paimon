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

package org.apache.paimon.io;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Factory to create {@link RecordReader}s for reading {@link KeyValue} files. */
public class KeyValueFileReaderFactory {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;

    private final BulkFormatMapping.BulkFormatMappingBuilder bulkFormatMappingBuilder;
    private final Map<FormatKey, BulkFormatMapping> bulkFormatMappings;
    private final DataFilePathFactory pathFactory;

    private KeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            BulkFormatMapping.BulkFormatMappingBuilder bulkFormatMappingBuilder,
            DataFilePathFactory pathFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schemaId = schemaId;
        this.keyType = keyType;
        this.valueType = valueType;
        this.bulkFormatMappingBuilder = bulkFormatMappingBuilder;
        this.pathFactory = pathFactory;
        this.bulkFormatMappings = new HashMap<>();
    }

    public RecordReader<KeyValue> createRecordReader(long schemaId, String fileName, int level)
            throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(fileName);
        BulkFormatMapping bulkFormatMapping =
                bulkFormatMappings.computeIfAbsent(
                        new FormatKey(schemaId, formatIdentifier),
                        key -> {
                            TableSchema tableSchema = schemaManager.schema(this.schemaId);
                            TableSchema dataSchema = schemaManager.schema(key.schemaId);
                            return bulkFormatMappingBuilder.build(
                                    formatIdentifier, tableSchema, dataSchema);
                        });
        return new KeyValueDataFileRecordReader(
                fileIO,
                bulkFormatMapping.getReaderFactory(),
                pathFactory.toPath(fileName),
                keyType,
                valueType,
                level,
                bulkFormatMapping.getIndexMapping(),
                bulkFormatMapping.getCastMapping());
    }

    public static Builder builder(
            FileIO fileIO,
            SchemaManager schemaManager,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor) {
        return new Builder(
                fileIO,
                schemaManager,
                schemaId,
                keyType,
                valueType,
                formatDiscover,
                pathFactory,
                extractor);
    }

    /** Builder for {@link KeyValueFileReaderFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final SchemaManager schemaManager;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormatDiscover formatDiscover;
        private final FileStorePathFactory pathFactory;
        private final KeyValueFieldsExtractor extractor;

        private final int[][] fullKeyProjection;
        private int[][] keyProjection;
        private int[][] valueProjection;
        private RowType projectedKeyType;
        private RowType projectedValueType;

        private Builder(
                FileIO fileIO,
                SchemaManager schemaManager,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormatDiscover formatDiscover,
                FileStorePathFactory pathFactory,
                KeyValueFieldsExtractor extractor) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.formatDiscover = formatDiscover;
            this.pathFactory = pathFactory;
            this.extractor = extractor;

            this.fullKeyProjection = Projection.range(0, keyType.getFieldCount()).toNestedIndexes();
            this.keyProjection = fullKeyProjection;
            this.valueProjection = Projection.range(0, valueType.getFieldCount()).toNestedIndexes();
            applyProjection();
        }

        public Builder copyWithoutProjection() {
            return new Builder(
                    fileIO,
                    schemaManager,
                    schemaId,
                    keyType,
                    valueType,
                    formatDiscover,
                    pathFactory,
                    extractor);
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

        public RowType projectedValueType() {
            return projectedValueType;
        }

        public KeyValueFileReaderFactory build(BinaryRow partition, int bucket) {
            return build(partition, bucket, true, Collections.emptyList());
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition,
                int bucket,
                boolean projectKeys,
                @Nullable List<Predicate> filters) {
            int[][] keyProjection = projectKeys ? this.keyProjection : fullKeyProjection;
            RowType projectedKeyType = projectKeys ? this.projectedKeyType : keyType;

            return new KeyValueFileReaderFactory(
                    fileIO,
                    schemaManager,
                    schemaId,
                    projectedKeyType,
                    projectedValueType,
                    BulkFormatMapping.newBuilder(
                            formatDiscover, extractor, keyProjection, valueProjection, filters),
                    pathFactory.createDataFilePathFactory(partition, bucket));
        }

        private void applyProjection() {
            projectedKeyType = Projection.of(keyProjection).project(keyType);
            projectedValueType = Projection.of(valueProjection).project(valueType);
        }
    }
}
