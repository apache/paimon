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

package org.apache.parquet.filter2.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.types.VectorType;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Convert {@link Predicate} to {@link FilterCompat.Filter}. */
public class ParquetFilters {

    private ParquetFilters() {}

    public static FilterCompat.Filter convert(
            List<Predicate> predicates, MessageType fileSchema, boolean caseSensitive) {
        ConvertFilterToParquet converter = new ConvertFilterToParquet(fileSchema, caseSensitive);
        FilterPredicate result = null;
        if (predicates != null) {
            for (Predicate predicate : predicates) {
                try {
                    FilterPredicate parquetFilter = predicate.visit(converter);
                    if (result == null) {
                        result = parquetFilter;
                    } else {
                        result = FilterApi.and(result, parquetFilter);
                    }
                } catch (UnsupportedOperationException ignore) {
                }
            }
        }

        return result != null ? FilterCompat.get(result) : FilterCompat.NOOP;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class ConvertFilterToParquet implements FunctionVisitor<FilterPredicate> {

        private final MessageType fileSchema;
        private final boolean caseSensitive;

        private ConvertFilterToParquet(MessageType fileSchema, boolean caseSensitive) {
            this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
            this.caseSensitive = caseSensitive;
        }

        @Override
        public FilterPredicate visitIsNotNull(FieldRef fieldRef) {
            return new Operators.NotEq<>(toParquetColumn(fieldRef), null);
        }

        @Override
        public FilterPredicate visitIsNull(FieldRef fieldRef) {
            return new Operators.Eq<>(toParquetColumn(fieldRef), null);
        }

        @Override
        public FilterPredicate visitIsNaN(FieldRef fieldRef) {
            Operators.Column<?> column = toParquetColumn(fieldRef);
            if (column instanceof DoubleColumn) {
                return FilterApi.userDefined((DoubleColumn) column, new IsNaNDoublePredicate());
            }
            if (column instanceof FloatColumn) {
                return FilterApi.userDefined((FloatColumn) column, new IsNaNFloatPredicate());
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitLessThan(FieldRef fieldRef, Object literal) {
            return new Operators.Lt(toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new Operators.GtEq(
                    toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitNotEqual(FieldRef fieldRef, Object literal) {
            return new Operators.NotEq(
                    toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new Operators.LtEq(
                    toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitEqual(FieldRef fieldRef, Object literal) {
            return new Operators.Eq(toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new Operators.Gt(toParquetColumn(fieldRef), toParquetObject(literal, fieldRef));
        }

        @Override
        public FilterPredicate visitAnd(List<FilterPredicate> children) {
            if (children.size() != 2) {
                throw new RuntimeException("Illegal and children: " + children.size());
            }

            return FilterApi.and(children.get(0), children.get(1));
        }

        @Override
        public FilterPredicate visitOr(List<FilterPredicate> children) {
            if (children.size() != 2) {
                throw new RuntimeException("Illegal or children: " + children.size());
            }

            return FilterApi.or(children.get(0), children.get(1));
        }

        @Override
        public FilterPredicate visitStartsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitEndsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitContains(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitLike(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitIn(FieldRef fieldRef, List<Object> literals) {
            Operators.Column<?> column = toParquetColumn(fieldRef);
            if (column instanceof Operators.LongColumn) {
                return FilterApi.in(
                        (Operators.LongColumn) column, convertSets(literals, Long.class, fieldRef));
            } else if (column instanceof Operators.IntColumn) {
                return FilterApi.in(
                        (Operators.IntColumn) column,
                        convertSets(literals, Integer.class, fieldRef));
            } else if (column instanceof Operators.DoubleColumn) {
                return FilterApi.in(
                        (Operators.DoubleColumn) column,
                        convertSets(literals, Double.class, fieldRef));
            } else if (column instanceof Operators.FloatColumn) {
                return FilterApi.in(
                        (Operators.FloatColumn) column,
                        convertSets(literals, Float.class, fieldRef));
            } else if (column instanceof Operators.BinaryColumn) {
                return FilterApi.in(
                        (Operators.BinaryColumn) column,
                        convertSets(literals, Binary.class, fieldRef));
            }

            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitNotIn(FieldRef fieldRef, List<Object> literals) {
            Operators.Column<?> column = toParquetColumn(fieldRef);
            if (column instanceof Operators.LongColumn) {
                return FilterApi.notIn(
                        (Operators.LongColumn) column, convertSets(literals, Long.class, fieldRef));
            } else if (column instanceof Operators.IntColumn) {
                return FilterApi.notIn(
                        (Operators.IntColumn) column,
                        convertSets(literals, Integer.class, fieldRef));
            } else if (column instanceof Operators.DoubleColumn) {
                return FilterApi.notIn(
                        (Operators.DoubleColumn) column,
                        convertSets(literals, Double.class, fieldRef));
            } else if (column instanceof Operators.FloatColumn) {
                return FilterApi.notIn(
                        (Operators.FloatColumn) column,
                        convertSets(literals, Float.class, fieldRef));
            } else if (column instanceof Operators.BinaryColumn) {
                return FilterApi.notIn(
                        (Operators.BinaryColumn) column,
                        convertSets(literals, Binary.class, fieldRef));
            }

            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitNonFieldLeaf(LeafPredicate predicate) {
            throw new UnsupportedOperationException();
        }

        private <T> Set<T> convertSets(List<Object> values, Class<T> kclass, FieldRef fieldRef) {
            Set<T> converted = new HashSet<>();
            for (Object value : values) {
                Comparable<?> cmp = toParquetObject(value, fieldRef);
                if (kclass.isInstance(cmp)) {
                    converted.add((T) cmp);
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            return converted;
        }

        private Operators.Column<?> toParquetColumn(FieldRef fieldRef) {
            return fieldRef.type()
                    .accept(new ConvertToColumnTypeVisitor(fieldRef, fileSchema, caseSensitive));
        }

        private Comparable<?> toParquetObject(Object value, FieldRef fieldRef) {
            if (value == null) {
                return null;
            }

            org.apache.paimon.types.DataType type = fieldRef.type();
            if (type instanceof DecimalType) {
                DecimalType decimalType = (DecimalType) fieldRef.type();
                Decimal decimal = normalizeDecimal((Decimal) value, decimalType);
                PrimitiveType primitiveType =
                        decimalPrimitiveType(fieldRef, fileSchema, caseSensitive);
                switch (primitiveType.getPrimitiveTypeName()) {
                    case INT32:
                        long intValue = toUnscaledLong(decimal);
                        if (intValue < Integer.MIN_VALUE || intValue > Integer.MAX_VALUE) {
                            throw new UnsupportedOperationException();
                        }
                        return (int) intValue;
                    case INT64:
                        return toUnscaledLong(decimal);
                    case BINARY:
                        return Binary.fromConstantByteArray(decimal.toUnscaledBytes());
                    case FIXED_LEN_BYTE_ARRAY:
                        return decimalToBinary(decimal, primitiveType.getTypeLength());
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            if (value instanceof Number) {
                if (value instanceof Byte) {
                    return ((Byte) value).intValue();
                } else if (value instanceof Short) {
                    return ((Short) value).intValue();
                }
                return (Comparable<?>) value;
            } else if (value instanceof String) {
                return Binary.fromString((String) value);
            } else if (value instanceof BinaryString) {
                return Binary.fromString(value.toString());
            } else if (value instanceof byte[]) {
                return Binary.fromReusedByteArray((byte[]) value);
            } else if (value instanceof Timestamp) {
                Timestamp timestamp = (Timestamp) value;
                timestampPrimitiveType(fieldRef, fileSchema, caseSensitive);
                int precision = getTimestampPrecision(type);
                if (precision <= 3) {
                    // milliseconds
                    return timestamp.getMillisecond();
                } else if (precision <= 6) {
                    // microseconds
                    return timestamp.toMicros();
                }
                // precision > 6 uses INT96, not supported
                throw new UnsupportedOperationException();
            }

            throw new UnsupportedOperationException();
        }

        private Decimal normalizeDecimal(Decimal decimal, DecimalType fieldType) {
            try {
                BigDecimal normalized =
                        decimal.toBigDecimal()
                                .setScale(fieldType.getScale(), RoundingMode.UNNECESSARY);
                Decimal result =
                        Decimal.fromBigDecimal(
                                normalized, fieldType.getPrecision(), fieldType.getScale());
                if (result == null) {
                    throw new UnsupportedOperationException();
                }
                return result;
            } catch (ArithmeticException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        private long toUnscaledLong(Decimal decimal) {
            try {
                return decimal.toUnscaledLong();
            } catch (ArithmeticException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        private Binary decimalToBinary(Decimal decimal, int numBytes) {
            byte[] unscaledBytes = decimal.toUnscaledBytes();
            if (unscaledBytes.length > numBytes) {
                throw new UnsupportedOperationException();
            }
            if (unscaledBytes.length == numBytes) {
                return Binary.fromConstantByteArray(unscaledBytes);
            }

            byte[] paddedBytes = new byte[numBytes];
            Arrays.fill(paddedBytes, unscaledBytes[0] < 0 ? (byte) -1 : (byte) 0);
            System.arraycopy(
                    unscaledBytes,
                    0,
                    paddedBytes,
                    numBytes - unscaledBytes.length,
                    unscaledBytes.length);
            return Binary.fromConstantByteArray(paddedBytes);
        }
    }

    private static PrimitiveType decimalPrimitiveType(
            FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
        PrimitiveType primitiveType = primitiveType(fieldRef, fileSchema, caseSensitive);
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (!(logicalType instanceof DecimalLogicalTypeAnnotation)) {
            throw new UnsupportedOperationException();
        }

        DecimalLogicalTypeAnnotation decimalLogicalType =
                (DecimalLogicalTypeAnnotation) logicalType;
        if (decimalLogicalType.getScale() != ((DecimalType) fieldRef.type()).getScale()) {
            throw new UnsupportedOperationException();
        }
        return primitiveType;
    }

    private static PrimitiveType timestampPrimitiveType(
            FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
        PrimitiveType primitiveType = primitiveType(fieldRef, fileSchema, caseSensitive);
        if (primitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            throw new UnsupportedOperationException();
        }

        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType == null) {
            return primitiveType;
        }
        if (!(logicalType instanceof TimestampLogicalTypeAnnotation)) {
            throw new UnsupportedOperationException();
        }

        TimestampLogicalTypeAnnotation timestampType = (TimestampLogicalTypeAnnotation) logicalType;
        int precision = getTimestampPrecision(fieldRef.type());
        LogicalTypeAnnotation.TimeUnit expectedUnit =
                precision <= 3
                        ? LogicalTypeAnnotation.TimeUnit.MILLIS
                        : LogicalTypeAnnotation.TimeUnit.MICROS;
        boolean expectedAdjustedToUtc = fieldRef.type() instanceof LocalZonedTimestampType;
        if (timestampType.getUnit() != expectedUnit
                || timestampType.isAdjustedToUTC() != expectedAdjustedToUtc) {
            throw new UnsupportedOperationException();
        }
        return primitiveType;
    }

    private static PrimitiveType primitiveType(
            FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
        Type matched = null;
        // Paimon predicates currently reference top-level fields only. Nested field
        // predicates are rejected before reaching the format reader.
        for (Type field : fileSchema.getFields()) {
            if (caseSensitive
                    ? field.getName().equals(fieldRef.name())
                    : field.getName().equalsIgnoreCase(fieldRef.name())) {
                matched = field;
                break;
            }
        }

        if (matched == null || !matched.isPrimitive()) {
            throw new UnsupportedOperationException();
        }

        return matched.asPrimitiveType();
    }

    private static int getTimestampPrecision(org.apache.paimon.types.DataType type) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        } else if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision();
        }
        throw new IllegalArgumentException("Not a timestamp type: " + type);
    }

    private static class ConvertToColumnTypeVisitor
            implements DataTypeVisitor<Operators.Column<?>> {

        private final FieldRef fieldRef;
        private final String name;
        private final MessageType fileSchema;
        private final boolean caseSensitive;

        public ConvertToColumnTypeVisitor(
                FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
            this.fieldRef = fieldRef;
            this.name = fieldRef.name();
            this.fileSchema = fileSchema;
            this.caseSensitive = caseSensitive;
        }

        @Override
        public Operators.Column<?> visit(CharType charType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(VarCharType varCharType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BooleanType booleanType) {
            return FilterApi.booleanColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BinaryType binaryType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(VarBinaryType varBinaryType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(TinyIntType tinyIntType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(SmallIntType smallIntType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(IntType intType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BigIntType bigIntType) {
            return FilterApi.longColumn(name);
        }

        @Override
        public Operators.Column<?> visit(FloatType floatType) {
            return FilterApi.floatColumn(name);
        }

        @Override
        public Operators.Column<?> visit(DoubleType doubleType) {
            return FilterApi.doubleColumn(name);
        }

        @Override
        public Operators.Column<?> visit(DateType dateType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(TimeType timeType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(DecimalType decimalType) {
            PrimitiveType primitiveType = decimalPrimitiveType(fieldRef, fileSchema, caseSensitive);
            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return FilterApi.intColumn(fieldRef.name());
                case INT64:
                    return FilterApi.longColumn(fieldRef.name());
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return FilterApi.binaryColumn(fieldRef.name());
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public Operators.Column<?> visit(TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (precision <= 6) {
                timestampPrimitiveType(fieldRef, fileSchema, caseSensitive);
                return FilterApi.longColumn(name);
            }
            // precision > 6 uses INT96, not supported for filter pushdown
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = localZonedTimestampType.getPrecision();
            if (precision <= 6) {
                timestampPrimitiveType(fieldRef, fileSchema, caseSensitive);
                return FilterApi.longColumn(name);
            }
            // precision > 6 uses INT96, not supported for filter pushdown
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(VariantType variantType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(BlobType blobType) {
            throw new UnsupportedOperationException();
        }

        // ===================== can not support =========================

        @Override
        public Operators.Column<?> visit(ArrayType arrayType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(VectorType vectorType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(MultisetType multisetType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(MapType mapType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(RowType rowType) {
            throw new UnsupportedOperationException();
        }
    }

    /** user defined predicate that keeps double rows where the value is nan. */
    public static class IsNaNDoublePredicate extends UserDefinedPredicate<Double>
            implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean keep(Double value) {
            return value != null && Double.isNaN(value);
        }

        @Override
        public boolean canDrop(Statistics<Double> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(Statistics<Double> statistics) {
            return false;
        }
    }

    /** user defined predicate that keeps float rows where the value is nan. */
    public static class IsNaNFloatPredicate extends UserDefinedPredicate<Float>
            implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean keep(Float value) {
            return value != null && Float.isNaN(value);
        }

        @Override
        public boolean canDrop(Statistics<Float> statistics) {
            return false;
        }

        @Override
        public boolean inverseCanDrop(Statistics<Float> statistics) {
            return false;
        }
    }
}
