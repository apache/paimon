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

package dev.vortex.api.expressions;

import org.apache.paimon.shade.guava30.com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.vortex.api.Expression;
import dev.vortex.api.proto.EndianUtils;
import dev.vortex.api.proto.Scalars;
import dev.vortex.api.proto.TemporalMetadatas;
import dev.vortex.proto.DTypeProtos;
import dev.vortex.proto.ExprProtos;
import dev.vortex.proto.ScalarProtos;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Literal value expression in the Vortex query system. */
public abstract class Literal<T> implements Expression {
    private final T value;

    private Literal(T value) {
        this.value = value;
    }

    public static Literal<?> parse(byte[] metadata, List<Expression> children) {
        if (!children.isEmpty()) {
            throw new IllegalArgumentException("Literal expression must have no children, found: " + children.size());
        }
        try {
            ExprProtos.LiteralOpts opts = ExprProtos.LiteralOpts.parseFrom(metadata);
            return deserializeLiteral(opts, children);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to parse literal metadata", e);
        }
    }

    public T getValue() {
        return this.value;
    }

    @Override
    public String id() {
        return "vortex.literal";
    }

    @Override
    public List<Expression> children() {
        return java.util.Collections.emptyList();
    }

    @Override
    public Optional<byte[]> metadata() {
        return Optional.of(ExprProtos.LiteralOpts.newBuilder()
                .setValue(this.acceptLiteralVisitor(LiteralToScalar.INSTANCE))
                .build()
                .toByteArray());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getValue());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Literal)) return false;
        Literal<?> literal = (Literal<?>) o;
        return Objects.equals(value, literal.value);
    }

    public static Literal<Void> nullLit() {
        return NullLiteral.INSTANCE;
    }

    public static Literal<Boolean> bool(Boolean value) {
        return new BooleanLiteral(value);
    }

    public static Literal<Byte> int8(Byte value) {
        return new Int8Literal(value);
    }

    public static Literal<Short> int16(Short value) {
        return new Int16Literal(value);
    }

    public static Literal<Integer> int32(Integer value) {
        return new Int32Literal(value);
    }

    public static Literal<Long> int64(Long value) {
        return new Int64Literal(value);
    }

    public static Literal<Float> float32(Float value) {
        return new Float32Literal(value);
    }

    public static Literal<Double> float64(Double value) {
        return new Float64Literal(value);
    }

    public static Literal<BigDecimal> decimal(BigDecimal value, int precision, int scale) {
        return new DecimalLiteral(value, precision, scale);
    }

    public static Literal<String> string(String value) {
        return new StringLiteral(value);
    }

    public static Literal<byte[]> bytes(byte[] value) {
        return new BytesLiteral(value);
    }

    public static Literal<Integer> timeSeconds(Integer value) {
        return new TimeSeconds(value);
    }

    public static Literal<Integer> timeMillis(Integer value) {
        return new TimeMillis(value);
    }

    public static Literal<Long> timeMicros(Long value) {
        return new TimeMicros(value);
    }

    public static Literal<Long> timeNanos(Long value) {
        return new TimeNanos(value);
    }

    public static Literal<Integer> dateDays(Integer value) {
        return new DateDays(value);
    }

    public static Literal<Long> dateMillis(Long value) {
        return new DateMillis(value);
    }

    public static Literal<Long> timestampMillis(Long value, Optional<String> timeZone) {
        return new TimestampMillis(value, timeZone);
    }

    public static Literal<Long> timestampMicros(Long value, Optional<String> timeZone) {
        return new TimestampMicros(value, timeZone);
    }

    public static Literal<Long> timestampNanos(Long value, Optional<String> timeZone) {
        return new TimestampNanos(value, timeZone);
    }

    @Override
    public <R> R accept(Expression.Visitor<R> visitor) {
        return visitor.visitLiteral(this);
    }

    public abstract <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor);

    public interface LiteralVisitor<U> {
        U visitNull();

        U visitBoolean(Boolean literal);

        U visitInt8(Byte literal);

        U visitInt16(Short literal);

        U visitInt32(Integer literal);

        U visitInt64(Long literal);

        U visitDateDays(Integer days);

        U visitDateMillis(Long millis);

        U visitTimeSeconds(Integer seconds);

        U visitTimeMillis(Integer seconds);

        U visitTimeMicros(Long seconds);

        U visitTimeNanos(Long seconds);

        U visitTimestampMillis(Long epochMillis, Optional<String> timeZone);

        U visitTimestampMicros(Long epochMicros, Optional<String> timeZone);

        U visitTimestampNanos(Long epochNanos, Optional<String> timeZone);

        U visitFloat32(Float literal);

        U visitFloat64(Double literal);

        U visitDecimal(BigDecimal decimal, int precision, int scale);

        U visitString(String literal);

        U visitBytes(byte[] literal);
    }

    static final class NullLiteral extends Literal<Void> {
        static final NullLiteral INSTANCE = new NullLiteral();

        private NullLiteral() {
            super(null);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitNull();
        }
    }

    static final class BooleanLiteral extends Literal<Boolean> {
        BooleanLiteral(Boolean value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitBoolean(getValue());
        }
    }

    static final class Int8Literal extends Literal<Byte> {
        Int8Literal(Byte value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitInt8(getValue());
        }
    }

    static final class Int16Literal extends Literal<Short> {
        Int16Literal(Short value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitInt16(getValue());
        }
    }

    static final class Int32Literal extends Literal<Integer> {
        Int32Literal(Integer value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitInt32(getValue());
        }
    }

    static final class Int64Literal extends Literal<Long> {
        Int64Literal(Long value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitInt64(getValue());
        }
    }

    static final class Float32Literal extends Literal<Float> {
        Float32Literal(Float value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitFloat32(getValue());
        }
    }

    static final class Float64Literal extends Literal<Double> {
        Float64Literal(Double value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitFloat64(getValue());
        }
    }

    static final class DecimalLiteral extends Literal<BigDecimal> {
        private final int precision;
        private final int scale;

        DecimalLiteral(BigDecimal value, int precision, int scale) {
            super(value);
            if (!Objects.isNull(value)) {
                Preconditions.checkArgument(scale == value.scale(), "scale %s ≠ value scale %s", scale, value.scale());
            }
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitDecimal(getValue(), precision, scale);
        }
    }

    static final class StringLiteral extends Literal<String> {
        StringLiteral(String value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitString(getValue());
        }
    }

    static final class BytesLiteral extends Literal<byte[]> {
        BytesLiteral(byte[] value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitBytes(getValue());
        }
    }

    static final class TimeSeconds extends Literal<Integer> {
        TimeSeconds(Integer value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimeSeconds(getValue());
        }
    }

    static final class TimeMillis extends Literal<Integer> {
        TimeMillis(Integer value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimeMillis(getValue());
        }
    }

    static final class TimeMicros extends Literal<Long> {
        TimeMicros(Long value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimeMicros(getValue());
        }
    }

    static final class TimeNanos extends Literal<Long> {
        TimeNanos(Long value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimeNanos(getValue());
        }
    }

    static final class DateDays extends Literal<Integer> {
        DateDays(Integer value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitDateDays(getValue());
        }
    }

    static final class DateMillis extends Literal<Long> {
        DateMillis(Long value) {
            super(value);
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitDateMillis(getValue());
        }
    }

    static final class TimestampMillis extends Literal<Long> {
        private final Optional<String> timeZone;

        TimestampMillis(Long value, Optional<String> timeZone) {
            super(value);
            this.timeZone = timeZone;
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimestampMillis(getValue(), timeZone);
        }
    }

    static final class TimestampMicros extends Literal<Long> {
        private final Optional<String> timeZone;

        TimestampMicros(Long value, Optional<String> timeZone) {
            super(value);
            this.timeZone = timeZone;
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimestampMicros(getValue(), timeZone);
        }
    }

    static final class TimestampNanos extends Literal<Long> {
        private final Optional<String> timeZone;

        TimestampNanos(Long value, Optional<String> timeZone) {
            super(value);
            this.timeZone = timeZone;
        }

        @Override
        public <U> U acceptLiteralVisitor(LiteralVisitor<U> visitor) {
            return visitor.visitTimestampNanos(getValue(), timeZone);
        }
    }

    static final class LiteralToScalar implements LiteralVisitor<ScalarProtos.Scalar> {
        static final LiteralToScalar INSTANCE = new LiteralToScalar();

        private LiteralToScalar() {}

        @Override
        public ScalarProtos.Scalar visitNull() {
            return Scalars.nullNull();
        }

        @Override
        public ScalarProtos.Scalar visitBoolean(Boolean literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullBool();
            } else {
                return Scalars.bool(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitInt8(Byte literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullInt8();
            } else {
                return Scalars.int8(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitInt16(Short literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullInt16();
            } else {
                return Scalars.int16(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitInt32(Integer literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullInt32();
            } else {
                return Scalars.int32(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitInt64(Long literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullInt64();
            } else {
                return Scalars.int64(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitDateDays(Integer days) {
            if (Objects.isNull(days)) {
                return Scalars.nullDateDays();
            } else {
                return Scalars.dateDays(days);
            }
        }

        @Override
        public ScalarProtos.Scalar visitDateMillis(Long millis) {
            if (Objects.isNull(millis)) {
                return Scalars.nullDateMillis();
            } else {
                return Scalars.dateMillis(millis);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimeSeconds(Integer seconds) {
            if (Objects.isNull(seconds)) {
                return Scalars.nullTimeSeconds();
            } else {
                return Scalars.timeSeconds(seconds);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimeMillis(Integer seconds) {
            if (Objects.isNull(seconds)) {
                return Scalars.nullTimeMillis();
            } else {
                return Scalars.timeMillis(seconds);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimeMicros(Long seconds) {
            if (Objects.isNull(seconds)) {
                return Scalars.nullTimeMicros();
            } else {
                return Scalars.timeMicros(seconds);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimeNanos(Long seconds) {
            if (Objects.isNull(seconds)) {
                return Scalars.nullTimeNanos();
            } else {
                return Scalars.timeNanos(seconds);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimestampMillis(Long epochMillis, Optional<String> timeZone) {
            if (Objects.isNull(epochMillis)) {
                return Scalars.nullTimestampMillis(timeZone);
            } else {
                return Scalars.timestampMillis(epochMillis, timeZone);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimestampMicros(Long epochMicros, Optional<String> timeZone) {
            if (Objects.isNull(epochMicros)) {
                return Scalars.nullTimestampMicros(timeZone);
            } else {
                return Scalars.timestampMicros(epochMicros, timeZone);
            }
        }

        @Override
        public ScalarProtos.Scalar visitTimestampNanos(Long epochNanos, Optional<String> timeZone) {
            if (Objects.isNull(epochNanos)) {
                return Scalars.nullTimestampNanos(timeZone);
            } else {
                return Scalars.timestampNanos(epochNanos, timeZone);
            }
        }

        @Override
        public ScalarProtos.Scalar visitFloat32(Float literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullFloat32();
            } else {
                return Scalars.float32(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitFloat64(Double literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullFloat64();
            } else {
                return Scalars.float64(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitDecimal(BigDecimal decimal, int precision, int scale) {
            if (Objects.isNull(decimal)) {
                return Scalars.nullDecimal(precision, scale);
            } else {
                return Scalars.decimal(decimal, precision, scale);
            }
        }

        @Override
        public ScalarProtos.Scalar visitString(String literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullString();
            } else {
                return Scalars.string(literal);
            }
        }

        @Override
        public ScalarProtos.Scalar visitBytes(byte[] literal) {
            if (Objects.isNull(literal)) {
                return Scalars.nullBytes();
            } else {
                return Scalars.bytes(literal);
            }
        }
    }

    private static Literal<?> deserializeLiteral(ExprProtos.LiteralOpts literal, List<Expression> children) {
        ScalarProtos.Scalar literalScalar = literal.getValue();
        DTypeProtos.DType dtype = literalScalar.getDtype();

        // Special handling of extension types
        if (dtype.hasExtension()) {
            return deserializeExtensionLiteral(literal);
        }

        ScalarProtos.ScalarValue scalarValue = literalScalar.getValue();

        switch (scalarValue.getKindCase()) {
            case NULL_VALUE:
                return nullLiteral(dtype);
            case BOOL_VALUE:
                return Literal.bool(scalarValue.getBoolValue());
            case INT64_VALUE:
                return Literal.int64(scalarValue.getInt64Value());
            case UINT64_VALUE:
                return Literal.int64(scalarValue.getUint64Value());
            case F32_VALUE:
                return Literal.float32(scalarValue.getF32Value());
            case F64_VALUE:
                return Literal.float64(scalarValue.getF64Value());
            case STRING_VALUE:
                return Literal.string(scalarValue.getStringValue());
            case BYTES_VALUE:
                if (dtype.hasDecimal()) {
                    ByteString littleEndian = scalarValue.getBytesValue();
                    byte[] bigEndian = EndianUtils.reverse(littleEndian);
                    BigDecimal value = new BigDecimal(
                            new BigInteger(bigEndian), dtype.getDecimal().getScale());
                    return Literal.decimal(
                            value,
                            dtype.getDecimal().getPrecision(),
                            dtype.getDecimal().getScale());
                } else {
                    return Literal.bytes(scalarValue.getBytesValue().toByteArray());
                }
            default:
                throw new UnsupportedOperationException("Unsupported ScalarValue type encountered: " + scalarValue);
        }
    }

    private static Literal<?> deserializeExtensionLiteral(ExprProtos.LiteralOpts literal) {
        ScalarProtos.Scalar scalar = literal.getValue();
        DTypeProtos.DType scalarType = scalar.getDtype();

        Preconditions.checkArgument(scalarType.hasExtension());

        DTypeProtos.Extension extType = scalarType.getExtension();
        String extId = scalarType.getExtension().getId();

        switch (extId) {
            case "vortex.time": {
                byte timeUnit =
                        TemporalMetadatas.getTimeUnit(extType.getMetadata().toByteArray());
                if (timeUnit == TemporalMetadatas.TIME_UNIT_SECONDS) {
                    return Literal.timeSeconds(Math.toIntExact(scalar.getValue().getInt64Value()));
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_MILLIS) {
                    return Literal.timeMillis(Math.toIntExact(scalar.getValue().getInt64Value()));
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_MICROS) {
                    return Literal.timeMicros(scalar.getValue().getInt64Value());
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_NANOS) {
                    return Literal.timeNanos(scalar.getValue().getInt64Value());
                } else {
                    throw new UnsupportedOperationException("Unsupported TIME time unit: " + timeUnit);
                }
            }
            case "vortex.date": {
                byte timeUnit =
                        TemporalMetadatas.getTimeUnit(extType.getMetadata().toByteArray());
                if (timeUnit == TemporalMetadatas.TIME_UNIT_DAYS) {
                    return Literal.dateDays(Math.toIntExact(scalar.getValue().getInt64Value()));
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_MILLIS) {
                    return Literal.dateMillis(scalar.getValue().getInt64Value());
                } else {
                    throw new UnsupportedOperationException("Unsupported DATE time unit: " + timeUnit);
                }
            }
            case "vortex.timestamp": {
                byte timeUnit =
                        TemporalMetadatas.getTimeUnit(extType.getMetadata().toByteArray());
                Optional<String> timeZone =
                        TemporalMetadatas.getTimeZone(extType.getMetadata().toByteArray());
                if (timeUnit == TemporalMetadatas.TIME_UNIT_MILLIS) {
                    return Literal.timestampMillis(scalar.getValue().getInt64Value(), timeZone);
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_MICROS) {
                    return Literal.timestampMicros(scalar.getValue().getInt64Value(), timeZone);
                } else if (timeUnit == TemporalMetadatas.TIME_UNIT_NANOS) {
                    return Literal.timestampNanos(scalar.getValue().getInt64Value(), timeZone);
                } else {
                    throw new UnsupportedOperationException("Unsupported TIMESTAMP time unit: " + timeUnit);
                }
            }
            default:
                throw new UnsupportedOperationException("Unsupported extension type: " + extId);
        }
    }

    private static Literal<?> nullLiteral(DTypeProtos.DType type) {
        switch (type.getDtypeTypeCase()) {
            case NULL:
                return Literal.nullLit();
            case BOOL:
                return Literal.bool(null);
            case PRIMITIVE:
                switch (type.getPrimitive().getType()) {
                    case U8:
                    case I8:
                        return Literal.int8(null);
                    case U16:
                    case I16:
                        return Literal.int16(null);
                    case U32:
                    case I32:
                        return Literal.int32(null);
                    case U64:
                    case I64:
                        return Literal.int64(null);
                    case F32:
                        return Literal.float32(null);
                    case F64:
                        return Literal.float64(null);
                    default:
                        throw new UnsupportedOperationException("Unsupported ScalarValue type encountered: " + type);
                }
            case DECIMAL:
                return Literal.decimal(
                        null,
                        type.getDecimal().getPrecision(),
                        type.getDecimal().getScale());
            case UTF8:
                return Literal.string(null);
            case BINARY:
                return Literal.bytes(null);
            default:
                throw new UnsupportedOperationException("Unsupported ScalarValue type encountered: " + type);
        }
    }
}
