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

package org.apache.paimon.format.parquet;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.shredding.ShreddingReadPlan;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.reader.VectorizedParquetRecordReader;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.shredding.ShreddingFormatReader;
import org.apache.paimon.format.shredding.ShreddingReadPlanFactories;
import org.apache.paimon.format.shredding.ShreddingReadPlanFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.data.columnar.ColumnVectorUtils.createParquetWritableColumnVector;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.PAIMON_SCHEMA;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.buildFieldsList;

/**
 * Parquet {@link FormatReaderFactory} that reads data from the file to {@link
 * VectorizedColumnBatch} in vectorized mode.
 */
public class ParquetReaderFactory implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderFactory.class);
    private final Options conf;
    private final RowType readType;
    private final int batchSize;
    private final boolean caseSensitive;
    @Nullable private final List<Predicate> predicates;

    /**
     * Cache: fileSchema -> requestedSchema.
     *
     * <p>Within one factory instance the readType is fixed, so the result of {@code
     * clipParquetSchema(fileSchema)} is deterministic for a given {@code fileSchema}. Most
     * Paimon-written files sharing the same schema version will have identical file schemas, so the
     * cache will almost always have at most one entry. Keying by the actual {@code fileSchema}
     * (rather than assuming all files have the same schema) keeps correctness for edge cases such
     * as externally-migrated Parquet files whose on-disk schema may vary.
     */
    private final Map<MessageType, RequestedSchema> requestedSchemaCache =
            new ConcurrentHashMap<>();

    public ParquetReaderFactory(
            Options conf, RowType readType, int batchSize, @Nullable List<Predicate> predicates) {
        this.conf = conf;
        this.readType = readType;
        this.batchSize = batchSize;
        this.caseSensitive = conf.getOptional(CatalogOptions.CASE_SENSITIVE).orElse(true);
        this.predicates = predicates;
    }

    @VisibleForTesting
    Map<MessageType, RequestedSchema> requestedSchemaCache() {
        return requestedSchemaCache;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        ParquetInputFile inputFile =
                ParquetInputFile.fromPath(context.fileIO(), context.filePath(), context.fileSize());
        ParquetReadOptions.Builder readOptionsBuilder =
                ParquetUtil.getParquetReadOptionsBuilder(conf).withRange(0, context.fileSize());
        ParquetInputStream inputStream = inputFile.newStream();
        ParquetMetadata footer =
                ParquetFileReader.readFooter(
                        inputFile, readOptionsBuilder.build(), inputStream, true);

        MessageType fileSchema = footer.getFileMetaData().getSchema();
        FilterCompat.Filter filter = ParquetFilters.convert(predicates, fileSchema, caseSensitive);
        ParquetReadOptions readOptions = readOptionsBuilder.withRecordFilter(filter).build();
        ParquetFileReader reader =
                new ParquetFileReader(
                        inputFile, footer, readOptions, inputStream, context.selection());

        ShreddingReadPlan readPlan =
                ShreddingReadPlanFactories.createReadPlan(
                        readType,
                        Collections.emptyMap(),
                        fileSchema,
                        shreddingReadPlanFactories(readType));
        DataField[] physicalReadFields = readFields(readPlan.physicalRowType());
        RequestedSchema requestedSchema =
                readPlan.isIdentity()
                        ? getOrCreateRequestedSchema(fileSchema)
                        : createRequestedSchema(fileSchema, physicalReadFields);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Create reader of the parquet file {}, the fileSchema is {}, the requestedSchema is {}.",
                    context.filePath(),
                    fileSchema,
                    requestedSchema.messageType);
        }

        int actualBatchSize = computeBatchSize(reader, requestedSchema.messageType);
        Preconditions.checkArgument(
                actualBatchSize > 0,
                "Parquet read batch size should be positive: %s",
                actualBatchSize);
        reader.setRequestedSchema(requestedSchema.messageType);
        WritableColumnVector[] writableVectors =
                createWritableVectors(actualBatchSize, physicalReadFields);

        VectorizedParquetRecordReader parquetReader =
                new VectorizedParquetRecordReader(
                        context.filePath(),
                        reader,
                        fileSchema,
                        requestedSchema.fields,
                        writableVectors,
                        actualBatchSize,
                        context.fileIO());
        return readPlan.isIdentity()
                ? parquetReader
                : new ShreddingFormatReader(parquetReader, readPlan);
    }

    private RequestedSchema getOrCreateRequestedSchema(MessageType fileSchema) {
        // clipParquetSchema and buildFieldsList are pure functions of (readFields, fileSchema).
        // Cache the result keyed by fileSchema so that files sharing the same on-disk schema
        // within this factory instance avoid redundant computation. Keying by fileSchema (rather
        // than a simple "compute once" flag) correctly handles edge cases where different files
        // read by the same factory instance may have different on-disk schemas, e.g. externally
        // migrated Parquet files.
        return requestedSchemaCache.computeIfAbsent(fileSchema, this::createRequestedSchema);
    }

    private List<ShreddingReadPlanFactory> shreddingReadPlanFactories(RowType readType) {
        return Collections.<ShreddingReadPlanFactory>singletonList(
                new VariantShreddingReadPlanFactory(readType, caseSensitive));
    }

    private RequestedSchema createRequestedSchema(MessageType fileSchema) {
        return createRequestedSchema(fileSchema, readFields(readType));
    }

    private static DataField[] readFields(RowType readType) {
        return readType.getFields().toArray(new DataField[0]);
    }

    private RequestedSchema createRequestedSchema(MessageType fileSchema, DataField[] readFields) {
        MessageType rs = clipParquetSchema(fileSchema, readFields);
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(rs);
        List<ParquetField> f = buildFieldsList(readFields, columnIO, rs);
        return new RequestedSchema(rs, f);
    }

    /** Clips `parquetSchema` according to `fieldNames`. */
    private MessageType clipParquetSchema(GroupType parquetSchema, DataField[] readFields) {
        Type[] types = new Type[readFields.length];
        for (int i = 0; i < readFields.length; ++i) {
            String fieldName = readFields[i].name();
            Type matched = matchParquetField(parquetSchema, fieldName);
            if (matched != null) {
                types[i] = clipParquetType(readFields[i].type(), matched);
            } else {
                LOG.warn(
                        "{} does not exist in {}, will fill the field with null.",
                        fieldName,
                        parquetSchema);
                types[i] = ParquetSchemaConverter.convertToParquetType(readFields[i]);
            }
        }

        return Types.buildMessage().addFields(types).named(PAIMON_SCHEMA);
    }

    /**
     * Resolves a field of {@code group} by {@code fieldName}, returning {@code null} when no field
     * matches. In case-sensitive mode only an exact-name match is accepted. In case-insensitive
     * mode a name that matches more than one parquet field (differing only by case) is ambiguous
     * and fails, mirroring Spark's case-insensitive Parquet resolution.
     */
    @Nullable
    private Type matchParquetField(GroupType group, String fieldName) {
        if (caseSensitive) {
            return group.containsField(fieldName) ? group.getType(fieldName) : null;
        }
        Type matched = null;
        for (Type field : group.getFields()) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                if (matched != null) {
                    throw new RuntimeException(
                            String.format(
                                    "Found duplicate field(s) \"%s\": [%s, %s] in case-insensitive mode",
                                    fieldName, matched.getName(), field.getName()));
                }
                matched = field;
            }
        }
        return matched;
    }

    /** Clips `parquetType` by `readType`. */
    private Type clipParquetType(DataType readType, Type parquetType) {
        Type variantType = VariantShreddingReadPlanFactory.clipParquetType(readType, parquetType);
        if (variantType != null) {
            return variantType;
        }

        switch (readType.getTypeRoot()) {
            case ROW:
                RowType rowType = (RowType) readType;
                GroupType rowGroup = (GroupType) parquetType;
                List<Type> rowGroupFields = new ArrayList<>();
                for (DataField field : rowType.getFields()) {
                    String fieldName = field.name();
                    Type type = matchParquetField(rowGroup, fieldName);
                    if (type != null) {
                        rowGroupFields.add(clipParquetType(field.type(), type));
                    } else {
                        // todo: support nested field missing
                        throw new RuntimeException("field " + fieldName + " is missing");
                    }
                }
                return rowGroup.withNewFields(rowGroupFields);
            case MAP:
                MapType mapType = (MapType) readType;
                GroupType mapGroup = (GroupType) parquetType;
                int mapSubFields = mapGroup.getFieldCount();
                Preconditions.checkArgument(
                        mapSubFields == 1,
                        "Parquet map group type should only have one middle level REPEATED field.");
                Pair<Type, Type> keyValueType = parquetMapKeyValueType(mapGroup);
                return ConversionPatterns.mapType(
                        mapGroup.getRepetition(),
                        mapGroup.getName(),
                        mapGroup.getType(0).getName(),
                        clipParquetType(mapType.getKeyType(), keyValueType.getLeft()),
                        clipParquetType(mapType.getValueType(), keyValueType.getRight()));
            case ARRAY:
            case VECTOR:
                DataType elementReadType =
                        readType instanceof ArrayType
                                ? ((ArrayType) readType).getElementType()
                                : ((VectorType) readType).getElementType();
                GroupType arrayGroup = (GroupType) parquetType;
                int listSubFields = arrayGroup.getFieldCount();
                Preconditions.checkArgument(
                        listSubFields == 1,
                        "Parquet list group type should only have one middle level REPEATED field.");
                // There are two representations for array type in parquet.
                // See link:
                // https://impala.apache.org/docs/build/html/topics/impala_parquet_array_resolution.html.
                int level = arrayGroup.getType(0) instanceof GroupType ? 3 : 2;
                Type elementType =
                        clipParquetType(elementReadType, parquetListElementType(arrayGroup));

                if (level == 3) {
                    // In case that the name in middle level is not "list".
                    Type groupMiddle =
                            new GroupType(
                                    Type.Repetition.REPEATED,
                                    arrayGroup.getType(0).getName(),
                                    elementType);
                    return new GroupType(
                            arrayGroup.getRepetition(),
                            arrayGroup.getName(),
                            OriginalType.LIST,
                            groupMiddle);
                } else {
                    return new GroupType(
                            arrayGroup.getRepetition(),
                            arrayGroup.getName(),
                            OriginalType.LIST,
                            elementType);
                }
            default:
                return parquetType;
        }
    }

    /**
     * Compute the batch size to use for the given file. Subclasses can override this to implement
     * dynamic per-file batch sizing based on footer metadata. The default implementation returns
     * the static {@code batchSize} passed to the constructor.
     */
    protected int computeBatchSize(ParquetFileReader reader, MessageType requestedSchema) {
        return batchSize;
    }

    private WritableColumnVector[] createWritableVectors(int batchSize, DataField[] readFields) {
        WritableColumnVector[] columns = new WritableColumnVector[readFields.length];
        for (int i = 0; i < readFields.length; i++) {
            columns[i] = createParquetWritableColumnVector(batchSize, readFields[i].type());
        }
        return columns;
    }

    public static Map<String, Map<String, String>> readFieldMetadata(ParquetFileReader reader) {
        String encodedSchema =
                reader.getFooter()
                        .getFileMetaData()
                        .getKeyValueMetaData()
                        .get(FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY);
        byte[] schemaMetadata =
                encodedSchema == null
                        ? null
                        : FormatMetadataUtils.decodeMetadata(
                                        Collections.singletonMap(
                                                FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY,
                                                encodedSchema))
                                .get(FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY);
        return FormatMetadataUtils.readFieldMetadata(schemaMetadata);
    }

    private static class RequestedSchema {

        private final MessageType messageType;
        private final List<ParquetField> fields;

        private RequestedSchema(MessageType messageType, List<ParquetField> fields) {
            this.messageType = messageType;
            this.fields = fields;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RequestedSchema that = (RequestedSchema) o;
            return Objects.equals(messageType, that.messageType)
                    && Objects.equals(fields, that.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(messageType, fields);
        }
    }
}
