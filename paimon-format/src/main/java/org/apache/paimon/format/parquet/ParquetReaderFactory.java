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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.data.variant.VariantPathSegment;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.reader.VectorizedParquetRecordReader;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.data.variant.VariantMetadataUtils.path;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.PAIMON_SCHEMA;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.buildFieldsList;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.createWritableColumnVector;

/**
 * Parquet {@link FormatReaderFactory} that reads data from the file to {@link
 * VectorizedColumnBatch} in vectorized mode.
 */
public class ParquetReaderFactory implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderFactory.class);

    private final Options conf;
    private final DataField[] readFields;
    private final int batchSize;
    @Nullable private final FilterCompat.Filter filter;

    /**
     * Cache: fileSchema -> (requestedSchema, parquetFields).
     *
     * <p>Within one factory instance the readType is fixed, so the result of {@code
     * clipParquetSchema(fileSchema)} is deterministic for a given {@code fileSchema}. Most
     * Paimon-written files sharing the same schema version will have identical file schemas, so the
     * cache will almost always have at most one entry. Keying by the actual {@code fileSchema}
     * (rather than assuming all files have the same schema) keeps correctness for edge cases such
     * as externally-migrated Parquet files whose on-disk schema may vary.
     */
    private final Map<MessageType, Pair<MessageType, List<ParquetField>>> schemaCache =
            new ConcurrentHashMap<>();

    public ParquetReaderFactory(
            Options conf, RowType readType, int batchSize, @Nullable FilterCompat.Filter filter) {
        this.conf = conf;
        this.readFields = readType.getFields().toArray(new DataField[0]);
        this.batchSize = batchSize;
        this.filter = filter;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        ParquetReadOptions.Builder builder =
                ParquetUtil.getParquetReadOptionsBuilder(conf)
                        .withRecordFilter(filter)
                        .withRange(0, context.fileSize());

        ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(
                                context.fileIO(), context.filePath(), context.fileSize()),
                        builder.build(),
                        context.selection());
        MessageType fileSchema = reader.getFileMetaData().getSchema();

        // clipParquetSchema and buildFieldsList are pure functions of (readFields, fileSchema).
        // Cache the result keyed by fileSchema so that files sharing the same on-disk schema
        // within this factory instance avoid redundant computation. Keying by fileSchema (rather
        // than a simple "compute once" flag) correctly handles edge cases where different files
        // read by the same factory instance may have different on-disk schemas, e.g. externally
        // migrated Parquet files.
        Pair<MessageType, List<ParquetField>> cached =
                schemaCache.computeIfAbsent(
                        fileSchema,
                        fs -> {
                            MessageType rs = clipParquetSchema(fs);
                            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(rs);
                            List<ParquetField> f = buildFieldsList(readFields, columnIO, rs);
                            return Pair.of(rs, f);
                        });
        MessageType requestedSchema = cached.getLeft();
        List<ParquetField> fields = cached.getRight();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Create reader of the parquet file {}, the fileSchema is {}, the requestedSchema is {}.",
                    context.filePath(),
                    fileSchema,
                    requestedSchema);
        }

        reader.setRequestedSchema(requestedSchema);
        WritableColumnVector[] writableVectors = createWritableVectors();

        return new VectorizedParquetRecordReader(
                context.filePath(),
                reader,
                fileSchema,
                fields,
                writableVectors,
                batchSize,
                context.fileIO());
    }

    /** Clips `parquetSchema` according to `fieldNames`. */
    private MessageType clipParquetSchema(GroupType parquetSchema) {
        Type[] types = new Type[readFields.length];
        for (int i = 0; i < readFields.length; ++i) {
            String fieldName = readFields[i].name();
            if (!parquetSchema.containsField(fieldName)) {
                LOG.warn(
                        "{} does not exist in {}, will fill the field with null.",
                        fieldName,
                        parquetSchema);
                types[i] = ParquetSchemaConverter.convertToParquetType(readFields[i]);
            } else {
                Type parquetType = parquetSchema.getType(fieldName);
                types[i] = clipParquetType(readFields[i].type(), parquetType);
            }
        }

        return Types.buildMessage().addFields(types).named(PAIMON_SCHEMA);
    }

    /** Clips `parquetType` by `readType`. */
    private Type clipParquetType(DataType readType, Type parquetType) {
        switch (readType.getTypeRoot()) {
            case ROW:
                RowType rowType = (RowType) readType;
                if (VariantMetadataUtils.isVariantRowType(rowType)) {
                    return clipVariantType(rowType, parquetType.asGroupType());
                }
                GroupType rowGroup = (GroupType) parquetType;
                List<Type> rowGroupFields = new ArrayList<>();
                for (DataField field : rowType.getFields()) {
                    String fieldName = field.name();
                    if (rowGroup.containsField(fieldName)) {
                        Type type = rowGroup.getType(fieldName);
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
                ArrayType arrayType = (ArrayType) readType;
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
                        clipParquetType(
                                arrayType.getElementType(), parquetListElementType(arrayGroup));

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

    private Type clipVariantType(RowType variantRowType, GroupType parquetType) {
        // If there is no typed_value field, return the original parquetType.
        if (!parquetType.containsField(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME)) {
            return parquetType;
        }

        boolean canClip = true;
        Set<String> fieldsToRead = new HashSet<>();
        for (DataField field : variantRowType.getFields()) {
            String path = path(field.description());
            VariantPathSegment[] pathSegments = VariantPathSegment.parse(path);
            if (pathSegments.length < 1) {
                canClip = false;
                break;
            }

            // todo: support nested column pruning
            VariantPathSegment pathSegment = pathSegments[0];
            if (pathSegment instanceof VariantPathSegment.ObjectExtraction) {
                fieldsToRead.add(((VariantPathSegment.ObjectExtraction) pathSegment).getKey());
            } else {
                canClip = false;
                break;
            }
        }

        if (!canClip) {
            return parquetType;
        }

        List<Type> typedFieldsToRead = new ArrayList<>();
        GroupType typedValue =
                parquetType.getType(PaimonShreddingUtils.TYPED_VALUE_FIELD_NAME).asGroupType();
        for (Type field : typedValue.getFields()) {
            if (fieldsToRead.contains(field.getName())) {
                typedFieldsToRead.add(field);
                fieldsToRead.remove(field.getName());
            }
        }

        List<Type> rowGroupFields = new ArrayList<>();
        rowGroupFields.add(parquetType.getType(PaimonShreddingUtils.METADATA_FIELD_NAME));
        // If there are fields to read not in the `typed_value`, add the `value` field.
        if (!fieldsToRead.isEmpty()) {
            rowGroupFields.add(parquetType.getType(PaimonShreddingUtils.VARIANT_VALUE_FIELD_NAME));
        }
        if (!typedFieldsToRead.isEmpty()) {
            rowGroupFields.add(typedValue.withNewFields(typedFieldsToRead));
        }
        return parquetType.withNewFields(rowGroupFields);
    }

    private WritableColumnVector[] createWritableVectors() {
        WritableColumnVector[] columns = new WritableColumnVector[readFields.length];
        for (int i = 0; i < readFields.length; i++) {
            columns[i] = createWritableColumnVector(batchSize, readFields[i].type());
        }
        return columns;
    }
}
