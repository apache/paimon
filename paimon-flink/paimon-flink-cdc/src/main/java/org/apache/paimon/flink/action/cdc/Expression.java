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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.SerializableSupplier;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Produce a computation result for computed column. */
public interface Expression extends Serializable {

    List<String> SUPPORTED_EXPRESSION =
            Arrays.asList(
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                    "date_format",
                    "substring",
                    "truncate");

    /** Return name of referenced field. */
    String fieldReference();

    /** Return {@link DataType} of computed value. */
    DataType outputType();

    /** Compute value from given input. Input and output are serialized to string. */
    String eval(String input);

    static Expression create(
            String exprName, String fieldReference, DataType fieldType, String... literals) {
        switch (exprName.toLowerCase()) {
            case "year":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getYear, literals);
            case "month":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getMonthValue, literals);
            case "day":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getDayOfMonth, literals);
            case "hour":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getHour, literals);
            case "minute":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getMinute, literals);
            case "second":
                return TimeToIntConverter.create(
                        fieldReference, fieldType, () -> LocalDateTime::getSecond, literals);
            case "date_format":
                return DateFormat.create(fieldReference, fieldType, literals);
            case "substring":
                return substring(fieldReference, literals);
            case "truncate":
                return truncate(fieldReference, fieldType, literals);
                // TODO: support more expression
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported expression: %s. Supported expressions are: %s",
                                exprName, String.join(",", SUPPORTED_EXPRESSION)));
        }
    }

    static Expression substring(String fieldReference, String... literals) {
        checkArgument(
                literals.length == 1 || literals.length == 2,
                String.format(
                        "'substring' expression supports one or two arguments, but found '%s'.",
                        literals.length));
        int beginInclusive;
        Integer endExclusive;
        try {
            beginInclusive = Integer.parseInt(literals[0]);
            endExclusive = literals.length == 1 ? null : Integer.parseInt(literals[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    String.format(
                            "The index arguments '%s' contain non integer value.",
                            Arrays.toString(literals)),
                    e);
        }
        checkArgument(
                beginInclusive >= 0,
                "begin index argument (%s) of 'substring' must be >= 0.",
                beginInclusive);
        checkArgument(
                endExclusive == null || endExclusive > beginInclusive,
                "end index (%s) must be larger than begin index (%s).",
                endExclusive,
                beginInclusive);
        return new Substring(fieldReference, beginInclusive, endExclusive);
    }

    static Expression truncate(String fieldReference, DataType fieldType, String... literals) {
        checkArgument(
                literals.length == 1,
                String.format(
                        "'truncate' expression supports one argument, but found '%s'.",
                        literals.length));
        return new TruncateComputer(fieldReference, fieldType, literals[0]);
    }

    abstract class TimeExpressionBase<T> implements Expression {

        private static final long serialVersionUID = 1L;

        private final String fieldReference;
        @Nullable private final String timeUnit;

        private transient Function<LocalDateTime, T> converter;

        private TimeExpressionBase(
                String fieldReference, DataType fieldType, @Nullable String timeUnit) {
            this.fieldReference = fieldReference;

            // when the input is INTEGER_NUMERIC, the time unit must be set
            if (fieldType.getTypeRoot().getFamilies().contains(DataTypeFamily.INTEGER_NUMERIC)
                    && timeUnit == null) {
                timeUnit = "second";
            }
            this.timeUnit = timeUnit;
        }

        @Override
        public String fieldReference() {
            return fieldReference;
        }

        /** If not, this must be overridden! */
        @Override
        public DataType outputType() {
            return DataTypes.INT();
        }

        @Override
        public String eval(String input) {
            if (converter == null) {
                this.converter = createConverter();
            }

            T result = converter.apply(toLocalDateTime(input));
            return String.valueOf(result);
        }

        private LocalDateTime toLocalDateTime(String input) {
            if (timeUnit == null) {
                return DateTimeUtils.toLocalDateTime(input, 9);
            } else {
                long numericValue = Long.parseLong(input);
                long milliseconds = 0;
                int nanosOfMillisecond = 0;
                switch (timeUnit) {
                    case "second":
                        milliseconds = numericValue * 1000L;
                        break;
                    case "millis":
                        milliseconds = numericValue;
                        break;
                    case "micros":
                        milliseconds = numericValue / 1000;
                        nanosOfMillisecond = (int) (numericValue % 1000 * 1000);
                        break;
                    case "nanos":
                        milliseconds = numericValue / 1_000_000;
                        nanosOfMillisecond = (int) (numericValue % 1_000_000);
                        break;
                }
                return Timestamp.fromEpochMillis(milliseconds, nanosOfMillisecond)
                        .toLocalDateTime();
            }
        }

        protected abstract Function<LocalDateTime, T> createConverter();
    }

    /** Convert the time to an integer. */
    final class TimeToIntConverter extends TimeExpressionBase<Integer> {

        private static final long serialVersionUID = 1L;

        private final SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier;

        private TimeToIntConverter(
                String fieldReference,
                DataType fieldType,
                @Nullable String timeUnit,
                SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier) {
            super(fieldReference, fieldType, timeUnit);
            this.converterSupplier = converterSupplier;
        }

        @Override
        protected Function<LocalDateTime, Integer> createConverter() {
            return converterSupplier.get();
        }

        private static TimeToIntConverter create(
                String fieldReference,
                DataType fieldType,
                SerializableSupplier<Function<LocalDateTime, Integer>> converterSupplier,
                String... literals) {
            checkArgument(
                    literals.length == 0 || literals.length == 1,
                    "TimeToIntConverter supports 0 or 1 argument, but found '%s'.",
                    literals.length);

            return new TimeToIntConverter(
                    fieldReference,
                    fieldType,
                    literals.length == 0 ? null : literals[0],
                    converterSupplier);
        }
    }

    /** Convert the time to desired formatted string. */
    final class DateFormat extends TimeExpressionBase<String> {

        private static final long serialVersionUID = 2L;

        private final String pattern;

        private DateFormat(
                String fieldReference,
                DataType fieldType,
                String pattern,
                @Nullable String timeUnit) {
            super(fieldReference, fieldType, timeUnit);
            this.pattern = pattern;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        protected Function<LocalDateTime, String> createConverter() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
            return localDateTime -> localDateTime.format(formatter);
        }

        private static DateFormat create(
                String fieldReference, DataType fieldType, String... literals) {
            checkArgument(
                    literals.length == 1 || literals.length == 2,
                    "'date_format' supports 1 or 2 arguments, but found '%s'.",
                    literals.length);

            return new DateFormat(
                    fieldReference,
                    fieldType,
                    literals[0],
                    literals.length == 1 ? null : literals[1]);
        }
    }

    /** Get substring using {@link String#substring}. */
    final class Substring implements Expression {

        private static final long serialVersionUID = 1L;

        private final String fieldReference;
        private final int beginInclusive;
        @Nullable private final Integer endExclusive;

        private Substring(
                String fieldReference, int beginInclusive, @Nullable Integer endExclusive) {
            this.fieldReference = fieldReference;
            this.beginInclusive = beginInclusive;
            this.endExclusive = endExclusive;
        }

        @Override
        public String fieldReference() {
            return fieldReference;
        }

        @Override
        public DataType outputType() {
            return DataTypes.STRING();
        }

        @Override
        public String eval(String input) {
            try {
                if (endExclusive == null) {
                    return input.substring(beginInclusive);
                } else {
                    return input.substring(beginInclusive, endExclusive);
                }
            } catch (StringIndexOutOfBoundsException e) {
                throw new RuntimeException(
                        String.format(
                                "Cannot get substring from '%s' because the indexes are out of range. Begin index: %s, end index: %s.",
                                input, beginInclusive, endExclusive));
            }
        }
    }

    /** Truncate numeric/decimal/string value. */
    final class TruncateComputer implements Expression {
        private static final long serialVersionUID = 1L;

        private final String fieldReference;

        private final DataType fieldType;

        private final int width;

        TruncateComputer(String fieldReference, DataType fieldType, String literal) {
            this.fieldReference = fieldReference;
            this.fieldType = fieldType;
            try {
                this.width = Integer.parseInt(literal);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid width value for truncate function: %s, expected integer.",
                                literal));
            }
        }

        @Override
        public String fieldReference() {
            return fieldReference;
        }

        @Override
        public DataType outputType() {
            return fieldType;
        }

        @Override
        public String eval(String input) {
            switch (fieldType.getTypeRoot()) {
                case TINYINT:
                case SMALLINT:
                    return String.valueOf(truncateShort(width, Short.parseShort(input)));
                case INTEGER:
                    return String.valueOf(truncateInt(width, Integer.parseInt(input)));
                case BIGINT:
                    return String.valueOf(truncateLong(width, Long.parseLong(input)));
                case DECIMAL:
                    return truncateDecimal(BigInteger.valueOf(width), new BigDecimal(input))
                            .toString();
                case VARCHAR:
                case CHAR:
                    checkArgument(
                            width <= input.length(),
                            "Invalid width value for truncate function: %s, expected less than or equal to %s.",
                            width,
                            input.length());
                    return input.substring(0, width);
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unsupported field type for truncate function: %s.",
                                    fieldType.getTypeRoot().toString()));
            }
        }

        private short truncateShort(int width, short value) {
            return (short) (value - (((value % width) + width) % width));
        }

        private int truncateInt(int width, int value) {
            return value - (((value % width) + width) % width);
        }

        private long truncateLong(int width, long value) {
            return value - (((value % width) + width) % width);
        }

        private BigDecimal truncateDecimal(BigInteger unscaledWidth, BigDecimal value) {
            BigDecimal remainder =
                    new BigDecimal(
                            value.unscaledValue()
                                    .remainder(unscaledWidth)
                                    .add(unscaledWidth)
                                    .remainder(unscaledWidth),
                            value.scale());

            return value.subtract(remainder);
        }
    }
}
