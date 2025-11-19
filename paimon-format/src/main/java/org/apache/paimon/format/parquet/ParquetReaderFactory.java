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
import org.apache.paimon.data.variant.VariantAccessInfo;
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
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
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
import java.util.List;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.PAIMON_SCHEMA;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetListElementType;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.parquetMapKeyValueType;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.buildFieldsList;
import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.createWritableColumnVector;
import static org.apache.parquet.hadoop.UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY;

/**
 * Parquet {@link FormatReaderFactory} that reads data from the file to {@link
 * VectorizedColumnBatch} in vectorized mode.
 */
public class ParquetReaderFactory implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderFactory.class);

    private static final String ALLOCATION_SIZE = "parquet.read.allocation.size";

    private final Options conf;
    private final DataField[] readFields;
    private final int batchSize;
    @Nullable private final FilterCompat.Filter filter;
    @Nullable private final VariantAccessInfo[] variantAccess;

    public ParquetReaderFactory(
            Options conf, RowType readType, int batchSize, @Nullable FilterCompat.Filter filter) {
        this(conf, readType, batchSize, filter, null);
    }

    public ParquetReaderFactory(
            Options conf,
            RowType readType,
            int batchSize,
            @Nullable FilterCompat.Filter filter,
            @Nullable VariantAccessInfo[] variantAccess) {
        this.conf = conf;
        this.readFields = readType.getFields().toArray(new DataField[0]);
        this.batchSize = batchSize;
        this.filter = filter;
        this.variantAccess = variantAccess;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(FormatReaderFactory.Context context)
            throws IOException {
        ParquetReadOptions.Builder builder =
                ParquetReadOptions.builder(new PlainParquetConfiguration())
                        .withRange(0, context.fileSize());
        setReadOptions(builder);

        ParquetFileReader reader =
                new ParquetFileReader(
                        ParquetInputFile.fromPath(
                                context.fileIO(), context.filePath(), context.fileSize()),
                        builder.build(),
                        context.selection());
        MessageType fileSchema = reader.getFileMetaData().getSchema();
        MessageType requestedSchema = clipParquetSchema(fileSchema);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Create reader of the parquet file {}, the fileSchema is {}, the requestedSchema is {}.",
                    context.filePath(),
                    fileSchema,
                    requestedSchema);
        }

        reader.setRequestedSchema(requestedSchema);
        RowType[] shreddingSchemas =
                VariantUtils.extractShreddingSchemasFromParquetSchema(readFields, fileSchema);
        List<List<VariantAccessInfo.VariantField>> variantFields =
                VariantUtils.buildVariantFields(readFields, variantAccess);
        WritableColumnVector[] writableVectors = createWritableVectors(variantFields);

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(requestedSchema);
        List<ParquetField> fields =
                buildFieldsList(readFields, columnIO, shreddingSchemas, variantFields);

        return new VectorizedParquetRecordReader(
                context.filePath(), reader, fileSchema, fields, writableVectors, batchSize);
    }

    private void setReadOptions(ParquetReadOptions.Builder builder) {
        builder.useSignedStringMinMax(
                conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
        builder.useDictionaryFilter(
                conf.getBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, true));
        builder.useStatsFilter(conf.getBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, true));
        builder.useRecordFilter(conf.getBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, true));
        builder.useColumnIndexFilter(
                conf.getBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true));
        builder.usePageChecksumVerification(
                conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false));
        builder.useBloomFilter(conf.getBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true));
        builder.withMaxAllocationInBytes(conf.getInteger(ALLOCATION_SIZE, 8388608));
        String badRecordThresh = conf.getString(BAD_RECORD_THRESHOLD_CONF_KEY, null);
        if (badRecordThresh != null) {
            builder.set(BAD_RECORD_THRESHOLD_CONF_KEY, badRecordThresh);
        }
        builder.withRecordFilter(filter);
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
                                arrayType.getElementType(),
                                parquetListElementType(arrayGroup, level));

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

    private WritableColumnVector[] createWritableVectors(
            List<List<VariantAccessInfo.VariantField>> variantFields) {
        WritableColumnVector[] columns = new WritableColumnVector[readFields.length];
        for (int i = 0; i < readFields.length; i++) {
            columns[i] =
                    createWritableColumnVector(
                            batchSize, readFields[i].type(), variantFields.get(i));
        }
        return columns;
    }
}
