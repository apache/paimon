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
import org.apache.paimon.format.parquet.ParquetSchemaConverter;
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
import org.apache.paimon.types.DataTypeRoot;
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

import javax.annotation.Nullable;

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
            Operators.Column<?> column = toParquetColumn(fieldRef);
            if (!(column instanceof Operators.BinaryColumn)) {
                throw new UnsupportedOperationException();
            }

            Binary prefix = (Binary) toParquetObject(literal, fieldRef);
            if (prefix.length() == 0) {
                throw new UnsupportedOperationException();
            }

            FilterPredicate lower = FilterApi.gtEq((Operators.BinaryColumn) column, prefix);
            Binary upper = nextBinary(prefix);
            return upper == null
                    ? lower
                    : FilterApi.and(lower, FilterApi.lt((Operators.BinaryColumn) column, upper));
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
        public FilterPredicate visitArrayContains(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitArraysOverlap(FieldRef fieldRef, List<Object> literals) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitArrayContainsAll(FieldRef fieldRef, List<Object> literals) {
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

            if (value instanceof Timestamp) {
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

            // The literal has to speak whatever the file holds, not what the table declares.
            switch (pushdownTarget(fieldRef, fileSchema, caseSensitive).type) {
                case BOOLEAN:
                    return toBoolean(value);
                case INT32:
                    return toInt(value);
                case INT64:
                    return toLong(value);
                case FLOAT:
                    return toFloat(value);
                case DOUBLE:
                    return toDouble(value);
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return toBinary(value);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private Comparable<?> toBoolean(Object value) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            throw new UnsupportedOperationException();
        }

        private Comparable<?> toInt(Object value) {
            long asLong = toLongValue(value);
            if (asLong < Integer.MIN_VALUE || asLong > Integer.MAX_VALUE) {
                // Truncating would change what the predicate means: `< 3000000000` would become
                // `< -1294967296` and prune row groups that hold matching rows.
                throw new UnsupportedOperationException();
            }
            return (int) asLong;
        }

        private Comparable<?> toLong(Object value) {
            return toLongValue(value);
        }

        private long toLongValue(Object value) {
            if (value instanceof Byte
                    || value instanceof Short
                    || value instanceof Integer
                    || value instanceof Long) {
                return ((Number) value).longValue();
            }
            throw new UnsupportedOperationException();
        }

        private Comparable<?> toFloat(Object value) {
            if (value instanceof Float) {
                return (Float) value;
            }
            throw new UnsupportedOperationException();
        }

        private Comparable<?> toDouble(Object value) {
            // Float widens to double exactly; a double literal must never be narrowed to float,
            // which would round the bound and drop rows that sit next to it.
            if (value instanceof Float || value instanceof Double) {
                return ((Number) value).doubleValue();
            }
            throw new UnsupportedOperationException();
        }

        private Comparable<?> toBinary(Object value) {
            if (value instanceof String) {
                return Binary.fromString((String) value);
            } else if (value instanceof BinaryString) {
                return Binary.fromString(value.toString());
            } else if (value instanceof byte[]) {
                return Binary.fromReusedByteArray((byte[]) value);
            }
            throw new UnsupportedOperationException();
        }

        /** Returns the smallest binary value strictly greater than all values with this prefix. */
        @Nullable
        private Binary nextBinary(Binary prefix) {
            byte[] bytes = prefix.getBytes();
            for (int i = bytes.length - 1; i >= 0; i--) {
                int value = bytes[i] & 0xff;
                if (value != 0xff) {
                    bytes[i] = (byte) (value + 1);
                    return Binary.fromConstantByteArray(bytes, 0, i + 1);
                }
            }
            return null;
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
        PrimitiveType matched = findPrimitiveType(fieldRef, fileSchema, caseSensitive);
        if (matched == null) {
            throw new UnsupportedOperationException();
        }
        return matched;
    }

    /**
     * The file's column for {@code fieldRef}, or null when the file has no such column. A column
     * that exists but is not primitive cannot carry a predicate at all, so it is rejected outright.
     */
    @Nullable
    private static PrimitiveType findPrimitiveType(
            FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
        // Paimon predicates currently reference top-level fields only. Nested field
        // predicates are rejected before reaching the format reader.
        for (Type field : fileSchema.getFields()) {
            if (caseSensitive
                    ? field.getName().equals(fieldRef.name())
                    : field.getName().equalsIgnoreCase(fieldRef.name())) {
                if (!field.isPrimitive()) {
                    throw new UnsupportedOperationException();
                }
                return field.asPrimitiveType();
            }
        }
        return null;
    }

    /**
     * The physical type a pushed-down predicate on {@code fieldRef} has to be expressed in. A
     * Format Table takes its schema from the metastore while its files are written by someone else,
     * so the declared type is not necessarily what the file holds and the predicate has to follow
     * the file.
     *
     * <p>Falls back to what the table declares when the file has no such column: parquet-mr skips
     * validation for a column it cannot find and evaluates it as null, so the predicate still
     * prunes.
     *
     * <p>Throws {@link UnsupportedOperationException} when the two types cannot be reconciled
     * without changing what the predicate means. {@link #convert} swallows that and drops the
     * predicate, which costs pruning but never rows.
     */
    private static PushdownTarget pushdownTarget(
            FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
        PrimitiveType.PrimitiveTypeName[] acceptable = acceptableTypes(fieldRef.type());
        PrimitiveType fileType = findPrimitiveType(fieldRef, fileSchema, caseSensitive);
        if (fileType == null) {
            return new PushdownTarget(fieldRef.name(), acceptable[0]);
        }

        validateBigIntCompatibility(fieldRef, fileType);

        if (ParquetSchemaConverter.isUnsignedInt(fileType)) {
            // An unsigned column orders its statistics unsigned, so a signed bound would prune the
            // wrong row groups. The read still widens the column; only the pruning is given up.
            throw new UnsupportedOperationException();
        }

        validateNarrowIntegerPushdown(fieldRef, fileType);

        for (PrimitiveType.PrimitiveTypeName candidate : acceptable) {
            if (fileType.getPrimitiveTypeName() == candidate) {
                return new PushdownTarget(fileType.getName(), candidate);
            }
        }
        throw new UnsupportedOperationException();
    }

    /** Reject physical integer annotations that cannot be represented as BIGINT. */
    private static void validateBigIntCompatibility(FieldRef fieldRef, PrimitiveType fileType) {
        if (fieldRef.type().getTypeRoot() == DataTypeRoot.BIGINT
                && !ParquetSchemaConverter.isBigIntLogicalTypeCompatible(fileType)) {
            throw new UnsupportedOperationException();
        }
    }

    /** Reject physical integer ranges that the declared narrow type cannot preserve on read. */
    private static void validateNarrowIntegerPushdown(FieldRef fieldRef, PrimitiveType fileType) {
        int maxBitWidth;
        switch (fieldRef.type().getTypeRoot()) {
            case TINYINT:
                maxBitWidth = 8;
                break;
            case SMALLINT:
                maxBitWidth = 16;
                break;
            default:
                return;
        }

        LogicalTypeAnnotation logicalType = fileType.getLogicalTypeAnnotation();
        if (!(logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation)) {
            throw new UnsupportedOperationException();
        }

        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType =
                (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType;
        if (!intType.isSigned() || intType.getBitWidth() > maxBitWidth) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * The file column a predicate has to name and the type it has to speak. The name matters in
     * case-insensitive mode: parquet-mr resolves a predicate against the file by exact column path,
     * so a predicate carrying the metastore's spelling of a column that the file spells differently
     * reads as a column the file does not have - which the statistics filter takes for all-null and
     * prunes away, silently losing every row.
     */
    private static class PushdownTarget {

        private final String name;
        private final PrimitiveType.PrimitiveTypeName type;

        private PushdownTarget(String name, PrimitiveType.PrimitiveTypeName type) {
            this.name = name;
            this.type = type;
        }
    }

    /**
     * The physical types a predicate on this Paimon type can be expressed in, most preferred first.
     * The head is what Paimon itself writes; the tail is a different type the vectorized reader
     * accepts without changing predicate semantics. Lossy narrowing is excluded even when the file
     * can be read, because filtering happens before the reader converts the value.
     */
    private static PrimitiveType.PrimitiveTypeName[] acceptableTypes(
            org.apache.paimon.types.DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.BOOLEAN
                };
            case TINYINT:
            case SMALLINT:
                // INT64 is lossy; INT32 annotations are validated by pushdownTarget.
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.INT32
                };
            case INTEGER:
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.INT64
                };
            case BIGINT:
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.INT64, PrimitiveType.PrimitiveTypeName.INT32
                };
            case FLOAT:
                // A DOUBLE file value may round to the predicate's FLOAT value only after reading.
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.FLOAT
                };
            case DOUBLE:
                // A double bound cannot be narrowed to float without rounding it, so a FLOAT file
                // column is read but never filtered on.
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.DOUBLE
                };
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.INT32
                };
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return new PrimitiveType.PrimitiveTypeName[] {
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                };
            default:
                throw new UnsupportedOperationException();
        }
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
        private final MessageType fileSchema;
        private final boolean caseSensitive;

        public ConvertToColumnTypeVisitor(
                FieldRef fieldRef, MessageType fileSchema, boolean caseSensitive) {
            this.fieldRef = fieldRef;
            this.fileSchema = fileSchema;
            this.caseSensitive = caseSensitive;
        }

        /** The column typed after the file, not after what the table declares. */
        private Operators.Column<?> column() {
            PushdownTarget target = pushdownTarget(fieldRef, fileSchema, caseSensitive);
            switch (target.type) {
                case BOOLEAN:
                    return FilterApi.booleanColumn(target.name);
                case INT32:
                    return FilterApi.intColumn(target.name);
                case INT64:
                    return FilterApi.longColumn(target.name);
                case FLOAT:
                    return FilterApi.floatColumn(target.name);
                case DOUBLE:
                    return FilterApi.doubleColumn(target.name);
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return FilterApi.binaryColumn(target.name);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public Operators.Column<?> visit(CharType charType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(VarCharType varCharType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(BooleanType booleanType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(BinaryType binaryType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(VarBinaryType varBinaryType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(TinyIntType tinyIntType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(SmallIntType smallIntType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(IntType intType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(BigIntType bigIntType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(FloatType floatType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(DoubleType doubleType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(DateType dateType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(TimeType timeType) {
            return column();
        }

        @Override
        public Operators.Column<?> visit(DecimalType decimalType) {
            PrimitiveType primitiveType = decimalPrimitiveType(fieldRef, fileSchema, caseSensitive);
            switch (primitiveType.getPrimitiveTypeName()) {
                case INT32:
                    return FilterApi.intColumn(primitiveType.getName());
                case INT64:
                    return FilterApi.longColumn(primitiveType.getName());
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    return FilterApi.binaryColumn(primitiveType.getName());
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public Operators.Column<?> visit(TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (precision <= 6) {
                return FilterApi.longColumn(
                        timestampPrimitiveType(fieldRef, fileSchema, caseSensitive).getName());
            }
            // precision > 6 uses INT96, not supported for filter pushdown
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = localZonedTimestampType.getPrecision();
            if (precision <= 6) {
                return FilterApi.longColumn(
                        timestampPrimitiveType(fieldRef, fileSchema, caseSensitive).getName());
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
