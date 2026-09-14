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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariantUtil.Type;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

/** Utils for variant get. */
public class VariantGet {

    public static Object cast(GenericVariant v, DataType dataType, VariantCastArgs castArgs) {
        if (dataType instanceof VariantType) {
            GenericVariantBuilder builder = new GenericVariantBuilder(false);
            builder.appendVariant(v);
            GenericVariant result = builder.result();
            return new GenericVariant(result.valueBuffer(), result.metadataBuffer());
        }

        Type variantType = v.getType();
        if (variantType == Type.NULL) {
            return null;
        }

        if (variantType == Type.UUID) {
            // There's no UUID type in Paimon. We only allow it to be cast to string.
            if (dataType.equals(DataTypes.STRING())) {
                return BinaryString.fromString(v.getUuid().toString());
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        }

        if (dataType instanceof RowType) {
            RowType rowType = (RowType) dataType;
            if (variantType == Type.OBJECT) {
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < v.objectSize(); i++) {
                    GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                    int idx = rowType.getFieldIndex(field.key);
                    if (idx != -1) {
                        row.setField(idx, cast(field.value, rowType.getTypeAt(idx), castArgs));
                    }
                }
                return row;
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            DataType valueType = mapType.getValueType();
            if (mapType.getKeyType().equals(DataTypes.STRING())) {
                if (variantType == Type.OBJECT) {
                    int size = v.objectSize();
                    HashMap<BinaryString, Object> map = new HashMap<>();
                    for (int i = 0; i < size; i++) {
                        GenericVariant.ObjectField field = v.getFieldAtIndex(i);
                        map.put(
                                BinaryString.fromString(field.key),
                                cast(field.value, valueType, castArgs));
                    }
                    return new GenericMap(map);
                } else {
                    return invalidCast(v, dataType, castArgs);
                }
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else if (dataType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dataType;
            if (variantType == Type.ARRAY) {
                int size = v.arraySize();
                Object[] array = new Object[size];
                for (int i = 0; i < size; i++) {
                    array[i] = cast(v.getElementAtIndex(i), arrayType.getElementType(), castArgs);
                }
                return new GenericArray(array);
            } else {
                return invalidCast(v, dataType, castArgs);
            }
        } else {
            Object input;
            DataType inputType;
            switch (variantType) {
                case OBJECT:
                case ARRAY:
                    if (dataType.equals(DataTypes.STRING())) {
                        return BinaryString.fromString(v.toJson(castArgs.zoneId()));
                    } else {
                        return invalidCast(v, dataType, castArgs);
                    }
                case BOOLEAN:
                    input = v.getBoolean();
                    inputType = DataTypes.BOOLEAN();
                    break;
                case LONG:
                    input = v.getLong();
                    inputType = DataTypes.BIGINT();
                    break;
                case STRING:
                    input = BinaryString.fromString(v.getString());
                    inputType = DataTypes.STRING();
                    break;
                case DOUBLE:
                    input = v.getDouble();
                    inputType = DataTypes.DOUBLE();
                    break;
                case DECIMAL:
                    Decimal decimal = normalizedDecimal(v.getDecimal());
                    input = decimal;
                    inputType = DataTypes.DECIMAL(decimal.precision(), decimal.scale());
                    break;
                case DATE:
                    input = (int) v.getLong();
                    inputType = DataTypes.DATE();
                    break;
                case TIMESTAMP:
                    input = Timestamp.fromMicros(v.getLong());
                    inputType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
                    break;
                case TIMESTAMP_NTZ:
                    input = Timestamp.fromMicros(v.getLong());
                    inputType = DataTypes.TIMESTAMP();
                    break;
                case FLOAT:
                    input = v.getFloat();
                    inputType = DataTypes.FLOAT();
                    break;
                case BINARY:
                    input = v.getBinary();
                    inputType = DataTypes.BYTES();
                    break;
                default:
                    // todo: support other types
                    throw new IllegalArgumentException("Unsupported type: " + v.getType());
            }

            if (inputType.equals(dataType)) {
                return input;
            }

            CastExecutor<Object, Object> resolve =
                    (CastExecutor<Object, Object>) CastExecutors.resolve(inputType, dataType);
            Object result = castScalar(input, inputType, dataType, resolve);
            return result == null ? invalidCast(v, dataType, castArgs) : result;
        }
    }

    /**
     * Casts a non-null scalar read from a variant to {@code targetType}, returning null when the
     * cast is invalid. The generic cast rules wrap a numeric value that does not fit the target, so
     * an out-of-range value is rejected here first, matching Spark's TRY cast semantics.
     */
    @Nullable
    static Object castScalar(
            Object input,
            DataType inputType,
            DataType targetType,
            @Nullable CastExecutor<Object, Object> executor) {
        if (executor == null || !fitsIntegralTarget(input, inputType, targetType)) {
            return null;
        }
        try {
            return executor.cast(input);
        } catch (Exception e) {
            return null;
        }
    }

    /** Whether a numeric {@code input} lies within the range of an integral {@code targetType}. */
    private static boolean fitsIntegralTarget(
            Object input, DataType inputType, DataType targetType) {
        long min;
        long max;
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                min = Byte.MIN_VALUE;
                max = Byte.MAX_VALUE;
                break;
            case SMALLINT:
                min = Short.MIN_VALUE;
                max = Short.MAX_VALUE;
                break;
            case INTEGER:
                min = Integer.MIN_VALUE;
                max = Integer.MAX_VALUE;
                break;
            case BIGINT:
                min = Long.MIN_VALUE;
                max = Long.MAX_VALUE;
                break;
            default:
                return true;
        }

        switch (inputType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                long value = ((Number) input).longValue();
                return value >= min && value <= max;
            case FLOAT:
            case DOUBLE:
                // The fractional part is truncated by the cast, so any finite value strictly
                // between min - 1 and max + 1 fits. Both bounds are exact doubles; for BIGINT
                // max + 1 is 2^63 and min - 1 rounds to -2^63, which is itself in range.
                double d = ((Number) input).doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    return false;
                }
                return max == Long.MAX_VALUE
                        ? d >= -0x1p63 && d < 0x1p63
                        : d > min - 1.0 && d < max + 1.0;
            case DECIMAL:
                BigDecimal truncated =
                        ((Decimal) input).toBigDecimal().setScale(0, RoundingMode.DOWN);
                return truncated.compareTo(BigDecimal.valueOf(min)) >= 0
                        && truncated.compareTo(BigDecimal.valueOf(max)) <= 0;
            default:
                return true;
        }
    }

    /**
     * The decimal a variant scalar is cast from: trailing zeros stripped, the way {@code toJson}
     * renders it, with a precision and scale that {@code DecimalType} accepts. The shredded reader
     * applies the same normalization to a {@code typed_value} decimal, whose scale comes from the
     * file schema, so that a cast yields the same result for a plain and a shredded file.
     */
    static Decimal normalizedDecimal(BigDecimal decimal) {
        decimal = decimal.stripTrailingZeros();
        if (decimal.scale() < 0) {
            // stripTrailingZeros folds trailing zeros into a negative exponent, and a negative
            // scale is not a Paimon decimal
            decimal = decimal.setScale(0);
        }
        int scale = decimal.scale();
        // precision() counts the digits of the unscaled value, so it is smaller than the scale
        // for a value below 0.1, which DecimalType rejects. The variant writer caps both at
        // MAX_DECIMAL16_PRECISION, so this stays in range.
        int precision = Math.max(decimal.precision(), scale);
        return Decimal.fromBigDecimal(decimal, precision, scale);
    }

    public static Object invalidCast(Variant v, DataType dataType, VariantCastArgs castArgs) {
        if (castArgs.failOnError()) {
            throw new RuntimeException(
                    "Invalid cast " + v.toJson(castArgs.zoneId()) + " to " + dataType);
        } else {
            return null;
        }
    }
}
