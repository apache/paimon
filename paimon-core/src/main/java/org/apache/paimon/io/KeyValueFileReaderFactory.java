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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.AsyncRecordReader;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.BulkFormatMapping.BulkFormatMappingBuilder;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/** Factory to create {@link RecordReader}s for reading {@link KeyValue} files. */
public class KeyValueFileReaderFactory implements FileReaderFactory<KeyValue> {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final TableSchema schema;
    private final RowType keyType;
    private final RowType valueType;

    private final BulkFormatMappingBuilder bulkFormatMappingBuilder;
    private final DataFilePathFactory pathFactory;
    private final long asyncThreshold;

    private final Map<FormatKey, BulkFormatMapping> bulkFormatMappings;
    private final BinaryRow partition;
    private final DeletionVector.Factory dvFactory;

    private KeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            BulkFormatMappingBuilder bulkFormatMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.keyType = keyType;
        this.valueType = valueType;
        this.bulkFormatMappingBuilder = bulkFormatMappingBuilder;
        this.pathFactory = pathFactory;
        this.asyncThreshold = asyncThreshold;
        this.partition = partition;
        this.bulkFormatMappings = new HashMap<>();
        this.dvFactory = dvFactory;
    }

    @Override
    public RecordReader<KeyValue> createRecordReader(DataFileMeta file) throws IOException {
        return createRecordReader(file.schemaId(), file.fileName(), file.fileSize(), file.level());
    }

    public RecordReader<KeyValue> createRecordReader(
            long schemaId, String fileName, long fileSize, int level) throws IOException {
        if (fileSize >= asyncThreshold && fileName.endsWith(".orc")) {
            return new AsyncRecordReader<>(
                    () -> createRecordReader(schemaId, fileName, level, false, 2, fileSize));
        }
        return createRecordReader(schemaId, fileName, level, true, null, fileSize);
    }

    private FileRecordReader<KeyValue> createRecordReader(
            long schemaId,
            String fileName,
            int level,
            boolean reuseFormat,
            @Nullable Integer orcPoolSize,
            long fileSize)
            throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(fileName);

        Supplier<BulkFormatMapping> formatSupplier =
                () ->
                        bulkFormatMappingBuilder.build(
                                formatIdentifier,
                                schema,
                                schemaId == schema.id() ? schema : schemaManager.schema(schemaId));

        BulkFormatMapping bulkFormatMapping =
                reuseFormat
                        ? bulkFormatMappings.computeIfAbsent(
                                new FormatKey(schemaId, formatIdentifier),
                                key -> formatSupplier.get())
                        : formatSupplier.get();
        Path filePath = pathFactory.toPath(fileName);

        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        bulkFormatMapping.getReaderFactory(),
                        orcPoolSize == null
                                ? new FormatReaderContext(fileIO, filePath, fileSize)
                                : new OrcFormatReaderContext(
                                        fileIO, filePath, fileSize, orcPoolSize),
                        bulkFormatMapping.getColumnMapping(),
                        bulkFormatMapping.getCastMapping(),
                        PartitionUtils.create(bulkFormatMapping.getPartitionPair(), partition));

        Optional<DeletionVector> deletionVector = dvFactory.create(fileName);
        if (deletionVector.isPresent() && !deletionVector.get().isEmpty()) {
            fileRecordReader =
                    new ApplyDeletionVectorReader(fileRecordReader, deletionVector.get());
        }

        return new KeyValueDataFileRecordReader(fileRecordReader, keyType, valueType, level);
    }

    public static Builder builder(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor,
            CoreOptions options) {
        return new Builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatDiscover,
                pathFactory,
                extractor,
                options);
    }

    /** Builder for {@link KeyValueFileReaderFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final SchemaManager schemaManager;
        private final TableSchema schema;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormatDiscover formatDiscover;
        private final FileStorePathFactory pathFactory;
        private final KeyValueFieldsExtractor extractor;
        private final CoreOptions options;

        private RowType readKeyType;
        private RowType readValueType;

        private Builder(
                FileIO fileIO,
                SchemaManager schemaManager,
                TableSchema schema,
                RowType keyType,
                RowType valueType,
                FileFormatDiscover formatDiscover,
                FileStorePathFactory pathFactory,
                KeyValueFieldsExtractor extractor,
                CoreOptions options) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.schema = schema;
            this.keyType = keyType;
            this.valueType = valueType;
            this.formatDiscover = formatDiscover;
            this.pathFactory = pathFactory;
            this.extractor = extractor;
            this.options = options;

            this.readKeyType = keyType;
            this.readValueType = valueType;
        }

        public Builder copyWithoutProjection() {
            return new Builder(
                    fileIO,
                    schemaManager,
                    schema,
                    keyType,
                    valueType,
                    formatDiscover,
                    pathFactory,
                    extractor,
                    options);
        }

        public Builder withReadKeyType(RowType readKeyType) {
            this.readKeyType = readKeyType;
            return this;
        }

        public Builder withReadValueType(RowType readValueType) {
            this.readValueType = readValueType;
            return this;
        }

        public RowType keyType() {
            return keyType;
        }

        public RowType readValueType() {
            return readValueType;
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition, int bucket, DeletionVector.Factory dvFactory) {
            return build(partition, bucket, dvFactory, true, Collections.emptyList());
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition,
                int bucket,
                DeletionVector.Factory dvFactory,
                boolean projectKeys,
                @Nullable List<Predicate> filters) {
            RowType finalReadKeyType = projectKeys ? this.readKeyType : keyType;
            Function<TableSchema, List<DataField>> fieldsExtractor =
                    schema -> {
                        List<DataField> dataKeyFields = extractor.keyFields(schema);
                        List<DataField> dataValueFields = extractor.valueFields(schema);
                        return KeyValue.createKeyValueFields(dataKeyFields, dataValueFields);
                    };
            List<DataField> readTableFields =
                    KeyValue.createKeyValueFields(
                            finalReadKeyType.getFields(), readValueType.getFields());

            return new KeyValueFileReaderFactory(
                    fileIO,
                    schemaManager,
                    schema,
                    finalReadKeyType,
                    readValueType,
                    new BulkFormatMappingBuilder(
                            formatDiscover, readTableFields, fieldsExtractor, filters),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    options.fileReaderAsyncThreshold().getBytes(),
                    partition,
                    dvFactory);
        }

        public FileIO fileIO() {
            return fileIO;
        }
    }
}
