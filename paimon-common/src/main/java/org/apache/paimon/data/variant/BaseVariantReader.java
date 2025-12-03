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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VariantType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;

import static org.apache.paimon.data.variant.GenericVariantUtil.Type;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The base class to read variant values into a Paimon type. For convenience, we also allow creating
 * an instance of the base class itself. None of its functions can be used, but it can serve as a
 * container of `targetType` and `castArgs`.
 */
public class BaseVariantReader {

    protected final VariantSchema schema;
    protected final DataType targetType;
    protected final VariantCastArgs castArgs;

    public BaseVariantReader(VariantSchema schema, DataType targetType, VariantCastArgs castArgs) {
        this.schema = schema;
        this.targetType = targetType;
        this.castArgs = castArgs;
    }

    public VariantSchema schema() {
        return schema;
    }

    public DataType targetType() {
        return targetType;
    }

    public VariantCastArgs castArgs() {
        return castArgs;
    }

    /**
     * Read from a row containing a variant value (shredded or unshredded) and return a value of
     * `targetType`. The row schema is described by `schema`. This function throws MALFORMED_VARIANT
     * if the variant is missing. If the variant can be legally missing (the only possible situation
     * is struct fields in object `typed_value`), the caller should check for it and avoid calling
     * this function if the variant is missing.
     */
    public Object read(InternalRow row, byte[] topLevelMetadata) {
        if (schema.typedIdx < 0 || row.isNullAt(schema.typedIdx)) {
            if (schema.variantIdx < 0 || row.isNullAt(schema.variantIdx)) {
                // Both `typed_value` and `value` are null, meaning the variant is missing.
                throw malformedVariant();
            }
            GenericVariant variant =
                    new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
            return VariantGet.cast(variant, targetType, castArgs);
        } else {
            return readFromTyped(row, topLevelMetadata);
        }
    }

    /** Subclasses should override it to produce the read result when `typed_value` is not null. */
    protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
        throw new UnsupportedOperationException();
    }

    /** A util function to rebuild the variant in binary format from a variant value. */
    protected Variant rebuildVariant(InternalRow row, byte[] topLevelMetadata) {
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        ShreddingUtils.rebuild(
                new PaimonShreddingUtils.PaimonShreddedRow(row), topLevelMetadata, schema, builder);
        return builder.result();
    }

    /** A util function to throw error or return null when an invalid cast happens. */
    protected Object invalidCast(InternalRow row, byte[] topLevelMetadata) {
        return VariantGet.invalidCast(rebuildVariant(row, topLevelMetadata), targetType, castArgs);
    }

    /**
     * Create a reader for `targetType`. If `schema` is null, meaning that the extraction path
     * doesn't exist in `typed_value`, it returns an instance of `BaseVariantReader`. As described
     * in the class comment, the reader is only a container of `targetType` and `castArgs` in this
     * case.
     */
    public static BaseVariantReader create(
            @Nullable VariantSchema schema,
            DataType targetType,
            VariantCastArgs castArgs,
            boolean isTopLevelUnshredded) {
        if (schema == null) {
            return new BaseVariantReader(null, targetType, castArgs);
        }

        if (targetType instanceof RowType) {
            return new RowReader(schema, (RowType) targetType, castArgs);
        } else if (targetType instanceof ArrayType) {
            return new ArrayReader(schema, (ArrayType) targetType, castArgs);
        } else if (targetType instanceof MapType
                && ((MapType) targetType).getKeyType().equals(DataTypes.STRING())) {
            if (((MapType) targetType).getKeyType().equals(DataTypes.STRING())) {
                return new MapReader(schema, (MapType) targetType, castArgs);
            } else {
                throw new UnsupportedOperationException();
            }
        } else if (targetType instanceof VariantType) {
            return new VariantReader(
                    schema, (VariantType) targetType, castArgs, isTopLevelUnshredded);
        } else {
            return new ScalarReader(schema, targetType, castArgs);
        }
    }

    /**
     * Read variant values into a Paimon row type. It reads unshredded fields (fields that are not
     * in the typed object) from the `value`, and reads the shredded fields from the object
     * `typed_value`. `value` must not contain any shredded field according to the shredding spec,
     * but this requirement is not enforced. If `value` does contain a shredded field, no error will
     * occur, and the field in object `typed_value` will be the final result.
     */
    private static final class RowReader extends BaseVariantReader {

        // For each field in `targetType`, store the index of the field with the same name in object
        // `typed_value`, or -1 if it doesn't exist in object `typed_value`.
        private final int[] fieldInputIndices;

        // For each field in `targetType`, store the reader from the corresponding field in object
        // `typed_value`, or null if it doesn't exist in object `typed_value`.
        private final BaseVariantReader[] fieldReaders;

        // If all fields in `targetType` can be found in object `typed_value`, then the reader
        // doesn't
        // need to read from `value`.
        private final boolean needUnshreddedObject;

        public RowReader(VariantSchema schema, RowType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);

            List<DataField> targetFields = targetType.getFields();
            this.fieldInputIndices = new int[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                fieldInputIndices[i] =
                        schema.objectSchemaMap != null
                                ? schema.objectSchemaMap.get(targetFields.get(i).name())
                                : -1;
            }

            this.fieldReaders = new BaseVariantReader[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                int inputIdx = fieldInputIndices[i];
                if (inputIdx >= 0) {
                    VariantSchema fieldSchema = schema.objectSchema[inputIdx].schema();
                    fieldReaders[i] =
                            BaseVariantReader.create(
                                    fieldSchema, targetFields.get(i).type(), castArgs, false);
                } else {
                    fieldReaders[i] = null;
                }
            }

            // Check if any field is missing (i.e., index == -1)
            boolean needsUnshredded = false;
            for (int idx : fieldInputIndices) {
                if (idx < 0) {
                    needsUnshredded = true;
                    break;
                }
            }
            this.needUnshreddedObject = needsUnshredded;
        }

        @Override
        public Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.objectSchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            InternalRow obj = row.getRow(schema.typedIdx, schema.objectSchema.length);
            GenericRow result = new GenericRow(fieldInputIndices.length);
            GenericVariant unshreddedObject = null;

            if (needUnshreddedObject
                    && schema.variantIdx >= 0
                    && !row.isNullAt(schema.variantIdx)) {
                unshreddedObject =
                        new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
                if (unshreddedObject.getType() != Type.OBJECT) {
                    throw malformedVariant();
                }
            }

            int numFields = fieldInputIndices.length;
            int i = 0;
            while (i < numFields) {
                int inputIdx = fieldInputIndices[i];
                if (inputIdx >= 0) {
                    // Shredded field must not be null.
                    if (obj.isNullAt(inputIdx)) {
                        throw malformedVariant();
                    }

                    VariantSchema fieldSchema = schema.objectSchema[inputIdx].schema();
                    InternalRow fieldInput = obj.getRow(inputIdx, fieldSchema.numFields);
                    // Only read from the shredded field if it is not missing.
                    if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx))
                            || (fieldSchema.variantIdx >= 0
                                    && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
                        Object fieldValue = fieldReaders[i].read(fieldInput, topLevelMetadata);
                        result.setField(i, fieldValue);
                    }
                } else if (unshreddedObject != null) {
                    DataField field = ((RowType) targetType).getField(i);
                    String fieldName = field.name();
                    DataType fieldType = field.type();
                    GenericVariant unshreddedField = unshreddedObject.getFieldByKey(fieldName);
                    if (unshreddedField != null) {
                        Object castedValue = VariantGet.cast(unshreddedField, fieldType, castArgs);
                        result.setField(i, castedValue);
                    }
                }
                i += 1;
            }
            return result;
        }
    }

    /** Read Parquet variant values into a Paimon array type. */
    private static final class ArrayReader extends BaseVariantReader {

        private final BaseVariantReader elementReader;

        public ArrayReader(VariantSchema schema, ArrayType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            if (schema.arraySchema != null) {
                this.elementReader =
                        BaseVariantReader.create(
                                schema.arraySchema, targetType.getElementType(), castArgs, false);
            } else {
                this.elementReader = null;
            }
        }

        @Override
        protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.arraySchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            int elementNumFields = schema.arraySchema.numFields;
            InternalArray arr = row.getArray(schema.typedIdx);
            int size = arr.size();
            Object[] result = new Object[size];
            int i = 0;
            while (i < size) {
                if (arr.isNullAt(i)) {
                    throw malformedVariant();
                }
                result[i] = elementReader.read(arr.getRow(i, elementNumFields), topLevelMetadata);
                i += 1;
            }
            return new GenericArray(result);
        }
    }

    /**
     * Read variant values into a Paimon map type with string key type. The input must be object for
     * a valid cast. The resulting map contains shredded fields from object `typed_value` and
     * unshredded fields from object `value`. `value` must not contain any shredded field according
     * to the shredding spec. Unlike `StructReader`, this requirement is enforced in `MapReader`. If
     * `value` does contain a shredded field, throw a MALFORMED_VARIANT error. The purpose is to
     * avoid duplicate map keys.
     */
    private static final class MapReader extends BaseVariantReader {

        private final BaseVariantReader[] valueReaders;
        private final BinaryString[] shreddedFieldNames;
        private final MapType targetType;

        public MapReader(VariantSchema schema, MapType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            this.targetType = targetType;

            if (schema.objectSchema != null) {
                int len = schema.objectSchema.length;
                this.valueReaders = new BaseVariantReader[len];
                this.shreddedFieldNames = new BinaryString[len];

                for (int i = 0; i < len; i++) {
                    VariantSchema.ObjectField fieldInfo = schema.objectSchema[i];
                    this.valueReaders[i] =
                            BaseVariantReader.create(
                                    fieldInfo.schema(), targetType.getValueType(), castArgs, false);
                    this.shreddedFieldNames[i] = BinaryString.fromString(fieldInfo.fieldName());
                }
            } else {
                this.valueReaders = null;
                this.shreddedFieldNames = null;
            }
        }

        @Override
        public Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (schema.objectSchema == null) {
                return invalidCast(row, topLevelMetadata);
            }

            InternalRow obj = row.getRow(schema.typedIdx, schema.objectSchema.length);
            int numShreddedFields = (valueReaders != null) ? valueReaders.length : 0;

            GenericVariant unshreddedObject = null;
            if (schema.variantIdx >= 0 && !row.isNullAt(schema.variantIdx)) {
                unshreddedObject =
                        new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
                if (unshreddedObject.getType() != Type.OBJECT) {
                    throw malformedVariant();
                }
            }

            int numUnshreddedFields =
                    (unshreddedObject != null) ? unshreddedObject.objectSize() : 0;
            int totalCapacity = numShreddedFields + numUnshreddedFields;

            HashMap<BinaryString, Object> map = new HashMap<>(totalCapacity);
            int i = 0;
            while (i < numShreddedFields) {
                // Shredded field must not be null.
                if (obj.isNullAt(i)) {
                    throw malformedVariant();
                }
                VariantSchema fieldSchema = schema.objectSchema[i].schema();
                InternalRow fieldInput = obj.getRow(i, fieldSchema.numFields);
                // Only add the shredded field to map if it is not missing.
                if ((fieldSchema.typedIdx >= 0 && !fieldInput.isNullAt(fieldSchema.typedIdx))
                        || (fieldSchema.variantIdx >= 0
                                && !fieldInput.isNullAt(fieldSchema.variantIdx))) {
                    map.put(
                            shreddedFieldNames[i],
                            valueReaders[i].read(fieldInput, topLevelMetadata));
                }
                i += 1;
            }
            i = 0;
            while (i < numUnshreddedFields) {
                GenericVariant.ObjectField field = unshreddedObject.getFieldAtIndex(i);
                if (schema.objectSchemaMap.containsKey(field.key)) {
                    throw malformedVariant();
                }
                map.put(
                        BinaryString.fromString(field.key),
                        VariantGet.cast(field.value, targetType.getValueType(), castArgs));
                i += 1;
            }
            return new GenericMap(map);
        }
    }

    /** Read variant values into a Paimon variant type (the binary format). */
    private static final class VariantReader extends BaseVariantReader {

        // An optional optimization: the user can set it to true if the variant column is
        // unshredded and the extraction path is empty. We are not required to do anything special,
        // but
        // we can avoid rebuilding variant for optimization purpose.
        private final boolean isTopLevelUnshredded;

        public VariantReader(
                VariantSchema schema,
                VariantType targetType,
                VariantCastArgs castArgs,
                boolean isTopLevelUnshredded) {
            super(schema, targetType, castArgs);
            this.isTopLevelUnshredded = isTopLevelUnshredded;
        }

        @Override
        public Object read(InternalRow row, byte[] topLevelMetadata) {
            if (isTopLevelUnshredded) {
                if (row.isNullAt(schema.variantIdx)) {
                    throw malformedVariant();
                }
                return new GenericVariant(row.getBinary(schema.variantIdx), topLevelMetadata);
            }
            return rebuildVariant(row, topLevelMetadata);
        }
    }

    /**
     * Read variant values into a Paimon scalar type. When `typed_value` is not null but not a
     * scalar, all other target types should return an invalid cast, but only the string target type
     * can still build a string from array/object `typed_value`. For scalar `typed_value`, it
     * depends on `ScalarCastHelper` to perform the cast. According to the shredding spec, scalar
     * `typed_value` and `value` must not be non-null at the same time. The requirement is not
     * enforced in this reader. If they are both non-null, no error will occur, and the reader will
     * read from `typed_value`.
     */
    private static final class ScalarReader extends BaseVariantReader {

        private final DataType scalaType;
        @Nullable private final CastExecutor<Object, Object> resolve;
        private final boolean noNeedCast;

        public ScalarReader(VariantSchema schema, DataType targetType, VariantCastArgs castArgs) {
            super(schema, targetType, castArgs);
            if (schema.scalarSchema != null) {
                scalaType = PaimonShreddingUtils.scalarSchemaToPaimonType(schema.scalarSchema);
                noNeedCast = scalaType.equals(targetType);
                resolve =
                        noNeedCast
                                ? null
                                : (CastExecutor<Object, Object>)
                                        CastExecutors.resolve(scalaType, targetType);
            } else {
                scalaType = null;
                noNeedCast = false;
                resolve = null;
            }
        }

        @Override
        protected Object readFromTyped(InternalRow row, byte[] topLevelMetadata) {
            if (!noNeedCast && resolve == null) {
                if (targetType.equals(DataTypes.STRING())) {
                    return BinaryString.fromString(
                            rebuildVariant(row, topLevelMetadata).toJson(castArgs.zoneId()));
                } else {
                    return invalidCast(row, topLevelMetadata);
                }
            }

            int typedValueIdx = schema.typedIdx;

            if (row.isNullAt(typedValueIdx)) {
                return null;
            }

            Object i;
            if (scalaType.equals(DataTypes.STRING())) {
                i = row.getString(typedValueIdx);
            } else if (scalaType instanceof TinyIntType) {
                i = row.getByte(typedValueIdx);
            } else if (scalaType instanceof SmallIntType) {
                i = row.getShort(typedValueIdx);
            } else if (scalaType instanceof IntType) {
                i = row.getInt(typedValueIdx);
            } else if (scalaType instanceof BigIntType) {
                i = row.getLong(typedValueIdx);
            } else if (scalaType instanceof FloatType) {
                i = row.getFloat(typedValueIdx);
            } else if (scalaType instanceof DoubleType) {
                i = row.getDouble(typedValueIdx);
            } else if (scalaType instanceof BooleanType) {
                i = row.getBoolean(typedValueIdx);
            } else if (scalaType.equals(DataTypes.BYTES())) {
                i = row.getBinary(typedValueIdx);
            } else if (scalaType instanceof DecimalType) {
                i =
                        row.getDecimal(
                                typedValueIdx,
                                ((DecimalType) scalaType).getPrecision(),
                                ((DecimalType) scalaType).getScale());
            } else {
                throw new UnsupportedOperationException("Unsupported scalar type: " + scalaType);
            }
            if (noNeedCast) {
                return i;
            }
            try {
                return resolve.cast(i);
            } catch (Exception e) {
                return invalidCast(row, topLevelMetadata);
            }
        }
    }
}
