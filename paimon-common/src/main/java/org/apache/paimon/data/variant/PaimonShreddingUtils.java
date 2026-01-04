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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.RowToColumnConverter;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.variant.VariantPathSegment.ArrayExtraction;
import org.apache.paimon.data.variant.VariantPathSegment.ObjectExtraction;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.data.variant.GenericVariantUtil.Type.ARRAY;
import static org.apache.paimon.data.variant.GenericVariantUtil.Type.OBJECT;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;

/** Utils for paimon shredding. */
public class PaimonShreddingUtils {

    public static final String METADATA_FIELD_NAME = Variant.METADATA;
    public static final String VARIANT_VALUE_FIELD_NAME = Variant.VALUE;
    public static final String TYPED_VALUE_FIELD_NAME = "typed_value";

    /** Paimon shredded row. */
    static class PaimonShreddedRow implements ShreddingUtils.ShreddedRow {

        private final DataGetters row;

        public PaimonShreddedRow(DataGetters row) {
            this.row = row;
        }

        @Override
        public boolean isNullAt(int ordinal) {
            return row.isNullAt(ordinal);
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return row.getBoolean(ordinal);
        }

        @Override
        public byte getByte(int ordinal) {
            return row.getByte(ordinal);
        }

        @Override
        public short getShort(int ordinal) {
            return row.getShort(ordinal);
        }

        @Override
        public int getInt(int ordinal) {
            return row.getInt(ordinal);
        }

        @Override
        public long getLong(int ordinal) {
            return row.getLong(ordinal);
        }

        @Override
        public float getFloat(int ordinal) {
            return row.getFloat(ordinal);
        }

        @Override
        public double getDouble(int ordinal) {
            return row.getDouble(ordinal);
        }

        @Override
        public BigDecimal getDecimal(int ordinal, int precision, int scale) {
            return row.getDecimal(ordinal, precision, scale).toBigDecimal();
        }

        @Override
        public String getString(int ordinal) {
            return row.getString(ordinal).toString();
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return row.getBinary(ordinal);
        }

        @Override
        public UUID getUuid(int ordinal) {
            // Paimon currently does not shred UUID.
            throw new UnsupportedOperationException();
        }

        @Override
        public ShreddingUtils.ShreddedRow getStruct(int ordinal, int numFields) {
            return new PaimonShreddedRow(row.getRow(ordinal, numFields));
        }

        @Override
        public ShreddingUtils.ShreddedRow getArray(int ordinal) {
            return new PaimonShreddedRow(row.getArray(ordinal));
        }

        @Override
        public int numElements() {
            return ((InternalArray) row).size();
        }
    }

    /** The search result of a `VariantPathSegment` in a `VariantSchema`. */
    public static class SchemaPathSegment {

        private final VariantPathSegment rawPath;

        // Whether this path segment is an object or array extraction.
        private final boolean isObject;
        // `schema.typedIdx`, if the path exists in the schema (for object extraction, the schema
        // should contain an object `typed_value` containing the requested field; similar for array
        // extraction). Negative otherwise.
        private final int typedIdx;

        // For object extraction, it is the index of the desired field in `schema.objectSchema`. If
        // the
        // requested field doesn't exist, both `extractionIdx/typedIdx` are set to negative.
        // For array extraction, it is the array index. The information is already stored in
        // `rawPath`,
        // but accessing a raw int should be more efficient than `rawPath`, which is an `Either`.
        private final int extractionIdx;

        public SchemaPathSegment(
                VariantPathSegment rawPath, boolean isObject, int typedIdx, int extractionIdx) {
            this.rawPath = rawPath;
            this.isObject = isObject;
            this.typedIdx = typedIdx;
            this.extractionIdx = extractionIdx;
        }

        public VariantPathSegment rawPath() {
            return rawPath;
        }

        public boolean isObject() {
            return isObject;
        }

        public int typedIdx() {
            return typedIdx;
        }

        public int extractionIdx() {
            return extractionIdx;
        }
    }

    /**
     * Represent a single field in a variant struct, that is a single requested field that the scan
     * should produce by extracting from the variant column.
     */
    public static class FieldToExtract {

        private final SchemaPathSegment[] path;
        private final BaseVariantReader reader;

        public FieldToExtract(SchemaPathSegment[] path, BaseVariantReader reader) {
            this.path = path;
            this.reader = reader;
        }

        public SchemaPathSegment[] path() {
            return path;
        }

        public BaseVariantReader reader() {
            return reader;
        }
    }

    public static RowType variantShreddingSchema(RowType rowType) {
        return variantShreddingSchema(rowType, true, false);
    }

    /**
     * Given an expected schema of a Variant value, returns a suitable schema for shredding, by
     * inserting appropriate intermediate value/typed_value fields at each level. For example, to
     * represent the JSON {"a": 1, "b": "hello"}, the schema struct&lt;a: int, b: string&gt; could
     * be passed into this function, and it would return the shredding schema: struct&lt; metadata:
     * binary, value: binary, typed_value: struct&lt; a: struct&lt;typed_value: int, value:
     * binary&gt;, b: struct&lt;typed_value: string, value: binary&gt;&gt;&gt;
     */
    private static RowType variantShreddingSchema(
            DataType dataType, boolean isTopLevel, boolean isObjectField) {
        RowType.Builder builder = RowType.builder();
        if (isTopLevel) {
            builder.field(METADATA_FIELD_NAME, DataTypes.BYTES().copy(false));
        }
        switch (dataType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                ArrayType shreddedArrayType =
                        new ArrayType(
                                arrayType.isNullable(),
                                variantShreddingSchema(arrayType.getElementType(), false, false));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedArrayType);
                break;
            case ROW:
                // The field name level is always non-nullable: Variant null values are represented
                // in the "value" column as "00", and missing values are represented by setting both
                // "value" and "typed_value" to null.
                RowType rowType = (RowType) dataType;
                RowType shreddedRowType =
                        rowType.copy(
                                rowType.getFields().stream()
                                        .map(
                                                field ->
                                                        field.newType(
                                                                variantShreddingSchema(
                                                                                field.type(),
                                                                                false,
                                                                                true)
                                                                        .notNull()))
                                        .collect(Collectors.toList()));
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, shreddedRowType);
                break;
            case VARIANT:
                // For Variant, we don't need a typed column. If there is no typed column, value is
                // required
                // for array elements or top-level fields, but optional for objects (where a null
                // represents
                // a missing field).
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES().copy(isObjectField));
                break;
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                builder.field(VARIANT_VALUE_FIELD_NAME, DataTypes.BYTES());
                builder.field(TYPED_VALUE_FIELD_NAME, dataType);
                break;
            default:
                throw invalidVariantShreddingSchema(dataType);
        }
        return builder.build();
    }

    public static VariantSchema buildVariantSchema(RowType rowType) {
        return buildVariantSchema(rowType, true);
    }

    private static VariantSchema buildVariantSchema(RowType rowType, boolean topLevel) {
        int typedIdx = -1;
        int variantIdx = -1;
        int topLevelMetadataIdx = -1;
        VariantSchema.ScalarType scalarSchema = null;
        VariantSchema.ObjectField[] objectSchema = null;
        VariantSchema arraySchema = null;

        // The struct must not be empty or contain duplicate field names. The latter is enforced in
        // the loop below (`if (typedIdx != -1)` and other similar checks).
        if (rowType.getFields().isEmpty()) {
            throw invalidVariantShreddingSchema(rowType);
        }

        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            DataType dataType = field.type();
            switch (field.name()) {
                case TYPED_VALUE_FIELD_NAME:
                    if (typedIdx != -1) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    typedIdx = i;
                    switch (field.type().getTypeRoot()) {
                        case ROW:
                            RowType r = (RowType) dataType;
                            List<DataField> rFields = r.getFields();
                            // The struct must not be empty or contain duplicate field names.
                            if (fields.isEmpty()
                                    || fields.stream().distinct().count() != fields.size()) {
                                throw invalidVariantShreddingSchema(rowType);
                            }
                            objectSchema = new VariantSchema.ObjectField[rFields.size()];
                            for (int index = 0; index < rFields.size(); index++) {
                                if (field.type() instanceof RowType) {
                                    DataField f = rFields.get(index);
                                    objectSchema[index] =
                                            new VariantSchema.ObjectField(
                                                    f.name(),
                                                    buildVariantSchema((RowType) f.type(), false));
                                } else {
                                    throw invalidVariantShreddingSchema(rowType);
                                }
                            }
                            break;
                        case ARRAY:
                            ArrayType arrayType = (ArrayType) dataType;
                            if (arrayType.getElementType() instanceof RowType) {
                                arraySchema =
                                        buildVariantSchema(
                                                (RowType) arrayType.getElementType(), false);
                            } else {
                                throw invalidVariantShreddingSchema(rowType);
                            }
                            break;
                        case BOOLEAN:
                            scalarSchema = new VariantSchema.BooleanType();
                            break;
                        case TINYINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.BYTE);
                            break;
                        case SMALLINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(
                                            VariantSchema.IntegralSize.SHORT);
                            break;
                        case INTEGER:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.INT);
                            break;
                        case BIGINT:
                            scalarSchema =
                                    new VariantSchema.IntegralType(VariantSchema.IntegralSize.LONG);
                            break;
                        case FLOAT:
                            scalarSchema = new VariantSchema.FloatType();
                            break;
                        case DOUBLE:
                            scalarSchema = new VariantSchema.DoubleType();
                            break;
                        case VARCHAR:
                            scalarSchema = new VariantSchema.StringType();
                            break;
                        case BINARY:
                            scalarSchema = new VariantSchema.BinaryType();
                            break;
                        case DATE:
                            scalarSchema = new VariantSchema.DateType();
                            break;
                        case DECIMAL:
                            DecimalType d = (DecimalType) dataType;
                            scalarSchema =
                                    new VariantSchema.DecimalType(d.getPrecision(), d.getScale());
                            break;
                        default:
                            throw invalidVariantShreddingSchema(rowType);
                    }
                    break;

                case VARIANT_VALUE_FIELD_NAME:
                    if (variantIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    variantIdx = i;
                    break;

                case METADATA_FIELD_NAME:
                    if (topLevelMetadataIdx != -1 || !(field.type() instanceof VarBinaryType)) {
                        throw invalidVariantShreddingSchema(rowType);
                    }
                    topLevelMetadataIdx = i;
                    break;

                default:
                    throw invalidVariantShreddingSchema(rowType);
            }

            if (topLevel && (topLevelMetadataIdx == -1)) {
                topLevelMetadataIdx = i;
            }
        }

        if (topLevel != (topLevelMetadataIdx >= 0)) {
            throw invalidVariantShreddingSchema(rowType);
        }

        return new VariantSchema(
                typedIdx,
                variantIdx,
                topLevelMetadataIdx,
                fields.size(),
                scalarSchema,
                objectSchema,
                arraySchema);
    }

    public static DataType scalarSchemaToPaimonType(VariantSchema.ScalarType scala) {
        if (scala instanceof VariantSchema.StringType) {
            return DataTypes.STRING();
        } else if (scala instanceof VariantSchema.IntegralType) {
            VariantSchema.IntegralType it = (VariantSchema.IntegralType) scala;
            switch (it.size) {
                case BYTE:
                    return DataTypes.TINYINT();
                case SHORT:
                    return DataTypes.SMALLINT();
                case INT:
                    return DataTypes.INT();
                case LONG:
                    return DataTypes.BIGINT();
                default:
                    throw new UnsupportedOperationException();
            }
        } else if (scala instanceof VariantSchema.FloatType) {
            return DataTypes.FLOAT();
        } else if (scala instanceof VariantSchema.DoubleType) {
            return DataTypes.DOUBLE();
        } else if (scala instanceof VariantSchema.BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (scala instanceof VariantSchema.BinaryType) {
            return DataTypes.BYTES();
        } else if (scala instanceof VariantSchema.DecimalType) {
            VariantSchema.DecimalType dt = (VariantSchema.DecimalType) scala;
            return DataTypes.DECIMAL(dt.precision, dt.scale);
        } else if (scala instanceof VariantSchema.DateType) {
            return DataTypes.DATE();
        } else if (scala instanceof VariantSchema.TimestampType) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
        } else if (scala instanceof VariantSchema.TimestampNTZType) {
            return DataTypes.TIMESTAMP();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private static RuntimeException invalidVariantShreddingSchema(DataType dataType) {
        return new RuntimeException("Invalid variant shredding schema: " + dataType);
    }

    /** Paimon shredded result. */
    public static class PaimonShreddedResult implements VariantShreddingWriter.ShreddedResult {

        private final VariantSchema schema;
        // Result is stored as an InternalRow.
        private final GenericRow row;

        public PaimonShreddedResult(VariantSchema schema) {
            this.schema = schema;
            this.row = new GenericRow(schema.numFields);
        }

        @Override
        public void addArray(VariantShreddingWriter.ShreddedResult[] array) {
            GenericArray arrayResult =
                    new GenericArray(
                            java.util.Arrays.stream(array)
                                    .map(result -> ((PaimonShreddedResult) result).row)
                                    .toArray(InternalRow[]::new));
            row.setField(schema.typedIdx, arrayResult);
        }

        @Override
        public void addObject(VariantShreddingWriter.ShreddedResult[] values) {
            GenericRow innerRow = new GenericRow(schema.objectSchema.length);
            for (int i = 0; i < values.length; i++) {
                innerRow.setField(i, ((PaimonShreddedResult) values[i]).row);
            }
            row.setField(schema.typedIdx, innerRow);
        }

        @Override
        public void addVariantValue(byte[] result) {
            row.setField(schema.variantIdx, result);
        }

        @Override
        public void addScalar(Object result) {
            Object paimonValue;
            if (schema.scalarSchema instanceof VariantSchema.StringType) {
                paimonValue = BinaryString.fromString((String) result);
            } else if (schema.scalarSchema instanceof VariantSchema.DecimalType) {
                VariantSchema.DecimalType dt = (VariantSchema.DecimalType) schema.scalarSchema;
                paimonValue = Decimal.fromBigDecimal((BigDecimal) result, dt.precision, dt.scale);
            } else {
                paimonValue = result;
            }
            row.setField(schema.typedIdx, paimonValue);
        }

        @Override
        public void addMetadata(byte[] result) {
            row.setField(schema.topLevelMetadataIdx, result);
        }
    }

    /** Paimon shredded result builder. */
    public static class PaimonShreddedResultBuilder
            implements VariantShreddingWriter.ShreddedResultBuilder {
        @Override
        public VariantShreddingWriter.ShreddedResult createEmpty(VariantSchema schema) {
            return new PaimonShreddedResult(schema);
        }

        // Consider allowing this to be set via config?
        @Override
        public boolean allowNumericScaleChanges() {
            return true;
        }
    }

    /** Converts an input variant into shredded components. Returns the shredded result. */
    public static InternalRow castShredded(GenericVariant variant, VariantSchema variantSchema) {
        return ((PaimonShreddedResult)
                        VariantShreddingWriter.castShredded(
                                variant, variantSchema, new PaimonShreddedResultBuilder()))
                .row;
    }

    /** Assemble a variant (binary format) from a variant value. */
    public static Variant assembleVariant(InternalRow row, VariantSchema schema) {
        return ShreddingUtils.rebuild(new PaimonShreddedRow(row), schema);
    }

    /** Assemble a variant struct, in which each field is extracted from the variant value. */
    public static InternalRow assembleVariantStruct(
            InternalRow inputRow, VariantSchema schema, FieldToExtract[] fields) {
        if (inputRow.isNullAt(schema.topLevelMetadataIdx)) {
            throw malformedVariant();
        }
        byte[] topLevelMetadata = inputRow.getBinary(schema.topLevelMetadataIdx);
        int numFields = fields.length;
        GenericRow resultRow = new GenericRow(numFields);
        int fieldIdx = 0;
        while (fieldIdx < numFields) {
            resultRow.setField(
                    fieldIdx,
                    extractField(
                            inputRow,
                            topLevelMetadata,
                            schema,
                            fields[fieldIdx].path(),
                            fields[fieldIdx].reader()));
            fieldIdx += 1;
        }
        return resultRow;
    }

    /**
     * Return a list of fields to extract. `targetType` must be either variant or variant struct. If
     * it is variant, return null because the target is the full variant and there is no field to
     * extract. If it is variant struct, return a list of fields matching the variant struct fields.
     */
    public static FieldToExtract[] getFieldsToExtract(
            List<VariantAccessInfo.VariantField> variantFields, VariantSchema variantSchema) {
        if (variantFields != null) {
            return variantFields.stream()
                    .map(
                            field ->
                                    buildFieldsToExtract(
                                            field.dataField().type(),
                                            field.path(),
                                            field.castArgs(),
                                            variantSchema))
                    .toArray(FieldToExtract[]::new);
        }
        return null;
    }

    /**
     * According to the dataType, variant extraction path and variantSchema, build the
     * FieldToExtract.
     */
    public static FieldToExtract buildFieldsToExtract(
            DataType dataType, String path, VariantCastArgs castArgs, VariantSchema inputSchema) {
        VariantPathSegment[] rawPath = VariantPathSegment.parse(path);
        SchemaPathSegment[] schemaPath = new SchemaPathSegment[rawPath.length];
        VariantSchema schema = inputSchema;
        // Search `rawPath` in `schema` to produce `schemaPath`. If a raw path segment cannot be
        // found at a certain level of the file type, then `typedIdx` will be -1 starting from
        // this position, and the final `schema` will be null.
        for (int i = 0; i < rawPath.length; i++) {
            VariantPathSegment extraction = rawPath[i];
            boolean isObject = extraction instanceof ObjectExtraction;
            int typedIdx = -1;
            int extractionIdx = -1;

            if (extraction instanceof ObjectExtraction) {
                ObjectExtraction objExtr = (ObjectExtraction) extraction;
                if (schema != null && schema.objectSchemaMap != null) {
                    Integer fieldIdx = schema.objectSchemaMap.get(objExtr.getKey());
                    if (fieldIdx != null) {
                        typedIdx = schema.typedIdx;
                        extractionIdx = fieldIdx;
                        schema = schema.objectSchema[fieldIdx].schema();
                    } else {
                        schema = null;
                    }
                } else {
                    schema = null;
                }
            } else if (extraction instanceof ArrayExtraction) {
                ArrayExtraction arrExtr = (ArrayExtraction) extraction;
                if (schema != null && schema.arraySchema != null) {
                    typedIdx = schema.typedIdx;
                    extractionIdx = arrExtr.getIndex();
                    schema = schema.arraySchema;
                } else {
                    schema = null;
                }
            } else {
                schema = null;
            }
            schemaPath[i] = new SchemaPathSegment(extraction, isObject, typedIdx, extractionIdx);
        }

        BaseVariantReader reader =
                BaseVariantReader.create(
                        schema,
                        dataType,
                        castArgs,
                        (schemaPath.length == 0) && inputSchema.isUnshredded());
        return new FieldToExtract(schemaPath, reader);
    }

    /**
     * Extract a single variant struct field from a variant value. It steps into `inputRow`
     * according to the variant extraction path, and read the extracted value as the target type.
     */
    private static Object extractField(
            InternalRow inputRow,
            byte[] topLevelMetadata,
            VariantSchema inputSchema,
            SchemaPathSegment[] pathList,
            BaseVariantReader reader) {
        int pathIdx = 0;
        int pathLen = pathList.length;
        InternalRow row = inputRow;
        VariantSchema schema = inputSchema;
        while (pathIdx < pathLen) {
            SchemaPathSegment path = pathList[pathIdx];

            if (path.typedIdx() < 0) {
                // The extraction doesn't exist in `typed_value`. Try to extract the remaining part
                // of the
                // path in `value`.
                int variantIdx = schema.variantIdx;
                if (variantIdx < 0 || row.isNullAt(variantIdx)) {
                    return null;
                }
                GenericVariant v = new GenericVariant(row.getBinary(variantIdx), topLevelMetadata);
                while (pathIdx < pathLen) {
                    VariantPathSegment rowPath = pathList[pathIdx].rawPath();
                    if (rowPath instanceof ObjectExtraction && v.getType() == OBJECT) {
                        v = v.getFieldByKey(((ObjectExtraction) rowPath).getKey());
                    } else if (rowPath instanceof ArrayExtraction && v.getType() == ARRAY) {
                        v = v.getElementAtIndex(((ArrayExtraction) rowPath).getIndex());
                    } else {
                        v = null;
                    }
                    if (v == null) {
                        return null;
                    }
                    pathIdx += 1;
                }
                return VariantGet.cast(v, reader.targetType(), reader.castArgs());
            }

            if (row.isNullAt(path.typedIdx())) {
                return null;
            }

            if (path.isObject()) {
                InternalRow obj = row.getRow(path.typedIdx(), schema.objectSchema.length);
                // Object field must not be null.
                if (obj.isNullAt(path.extractionIdx())) {
                    throw malformedVariant();
                }
                schema = schema.objectSchema[path.extractionIdx()].schema();
                row = obj.getRow(path.extractionIdx(), schema.numFields);
                // Return null if the field is missing.
                if ((schema.typedIdx < 0 || row.isNullAt(schema.typedIdx))
                        && (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx))) {
                    return null;
                }
            } else {
                InternalArray arr = row.getArray(path.typedIdx());
                // Return null if the extraction index is out of bound.
                if (path.extractionIdx() >= arr.size()) {
                    return null;
                }
                // Array element must not be null.
                if (arr.isNullAt(path.extractionIdx())) {
                    throw malformedVariant();
                }
                schema = schema.arraySchema;
                row = arr.getRow(path.extractionIdx(), schema.numFields);
            }
            pathIdx += 1;
        }
        return reader.read(row, topLevelMetadata);
    }

    /** Assemble a batch of variant (binary format) from a batch of variant values. */
    public static void assembleVariantBatch(
            CastedRowColumnVector input, WritableColumnVector output, VariantSchema variantSchema) {
        int numRows = input.getElementsAppended();
        output.reset();
        output.reserve(numRows);
        WritableBytesVector valueChild = (WritableBytesVector) output.getChildren()[0];
        WritableBytesVector metadataChild = (WritableBytesVector) output.getChildren()[1];
        for (int i = 0; i < numRows; ++i) {
            if (input.isNullAt(i)) {
                output.setNullAt(i);
            } else {
                Variant v = assembleVariant(input.getRow(i), variantSchema);
                byte[] value = v.value();
                byte[] metadata = v.metadata();
                valueChild.putByteArray(i, value, 0, value.length);
                metadataChild.putByteArray(i, metadata, 0, metadata.length);
            }
        }
    }

    /** Assemble a batch of variant struct from a batch of variant values. */
    public static void assembleVariantStructBatch(
            CastedRowColumnVector input,
            WritableColumnVector output,
            VariantSchema variantSchema,
            FieldToExtract[] fields,
            DataType readType) {
        int numRows = input.getElementsAppended();
        output.reset();
        output.reserve(numRows);
        RowToColumnConverter converter =
                new RowToColumnConverter(RowType.of(new DataField(0, "placeholder", readType)));
        WritableColumnVector[] converterVectors = new WritableColumnVector[1];
        converterVectors[0] = output;
        GenericRow converterRow = new GenericRow(1);
        for (int i = 0; i < numRows; ++i) {
            if (input.isNullAt(i)) {
                converterRow.setField(0, null);
            } else {
                converterRow.setField(
                        0, assembleVariantStruct(input.getRow(i), variantSchema, fields));
            }
            converter.convert(converterRow, converterVectors);
        }
    }
}
