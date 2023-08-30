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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

import java.util.Arrays;
import java.util.OptionalInt;

import static org.apache.paimon.types.VarCharType.MAX_LENGTH;

/**
 * Utils for creating {@link DataType}s.
 *
 * @since 0.4.0
 */
@Public
public class DataTypes {

    public static IntType INT() {
        return INT(true);
    }

    public static TinyIntType TINYINT() {
        return TINYINT(true);
    }

    public static SmallIntType SMALLINT() {
        return SMALLINT(true);
    }

    public static BigIntType BIGINT() {
        return BIGINT(true);
    }

    public static VarCharType STRING() {
        return STRING(true);
    }

    public static DoubleType DOUBLE() {
        return DOUBLE(true);
    }

    public static ArrayType ARRAY(DataType element) {
        return ARRAY(true, element);
    }

    public static CharType CHAR(int length) {
        return CHAR(true, length);
    }

    public static VarCharType VARCHAR(int length) {
        return VARCHAR(true, length);
    }

    public static BooleanType BOOLEAN() {
        return BOOLEAN(true);
    }

    public static DateType DATE() {
        return DATE(true);
    }

    public static TimeType TIME() {
        return TIME(true);
    }

    public static TimeType TIME(int precision) {
        return TIME(true, precision);
    }

    public static TimestampType TIMESTAMP() {
        return TIMESTAMP(true);
    }

    public static TimestampType TIMESTAMP_MILLIS() {
        return TIMESTAMP_MILLIS(true);
    }

    public static TimestampType TIMESTAMP(int precision) {
        return TIMESTAMP(true, precision);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE() {
        return TIMESTAMP_WITH_LOCAL_TIME_ZONE(true);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE(int precision) {
        return TIMESTAMP_WITH_LOCAL_TIME_ZONE(true, precision);
    }

    public static DecimalType DECIMAL(int precision, int scale) {
        return DECIMAL(true, precision, scale);
    }

    public static VarBinaryType BYTES() {
        return BYTES(true);
    }

    public static FloatType FLOAT() {
        return FLOAT(true);
    }

    public static MapType MAP(DataType keyType, DataType valueType) {
        return MAP(true, keyType, valueType);
    }

    public static IntType INT(boolean isNullable) {
        return new IntType(isNullable);
    }

    public static TinyIntType TINYINT(boolean isNullable) {
        return new TinyIntType(isNullable);
    }

    public static SmallIntType SMALLINT(boolean isNullable) {
        return new SmallIntType(isNullable);
    }

    public static BigIntType BIGINT(boolean isNullable) {
        return new BigIntType(isNullable);
    }

    public static VarCharType STRING(boolean isNullable) {
        return new VarCharType(isNullable, MAX_LENGTH);
    }

    public static DoubleType DOUBLE(boolean isNullable) {
        return new DoubleType(isNullable);
    }

    public static ArrayType ARRAY(boolean isNullable, DataType element) {
        return new ArrayType(isNullable, element);
    }

    public static CharType CHAR(boolean isNullable, int length) {
        return new CharType(isNullable, length);
    }

    public static VarCharType VARCHAR(boolean isNullable, int length) {
        return new VarCharType(isNullable, length);
    }

    public static BooleanType BOOLEAN(boolean isNullable) {
        return new BooleanType(isNullable);
    }

    public static DateType DATE(boolean isNullable) {
        return new DateType(isNullable);
    }

    public static TimeType TIME(boolean isNullable) {
        return new TimeType(isNullable);
    }

    public static TimeType TIME(boolean isNullable, int precision) {
        return new TimeType(isNullable, precision);
    }

    public static TimestampType TIMESTAMP(boolean isNullable) {
        return new TimestampType(isNullable);
    }

    public static TimestampType TIMESTAMP_MILLIS(boolean isNullable) {
        return new TimestampType(isNullable, 3);
    }

    public static TimestampType TIMESTAMP(boolean isNullable, int precision) {
        return new TimestampType(isNullable, precision);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE(boolean isNullable) {
        return new LocalZonedTimestampType(isNullable);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            boolean isNullable, int precision) {
        return new LocalZonedTimestampType(isNullable, precision);
    }

    public static DecimalType DECIMAL(boolean isNullable, int precision, int scale) {
        return new DecimalType(isNullable, precision, scale);
    }

    public static VarBinaryType BYTES(boolean isNullable) {
        return new VarBinaryType(isNullable, VarBinaryType.MAX_LENGTH);
    }

    public static FloatType FLOAT(boolean isNullable) {
        return new FloatType(isNullable);
    }

    public static MapType MAP(boolean isNullable, DataType keyType, DataType valueType) {
        return new MapType(isNullable, keyType, valueType);
    }

    public static DataField FIELD(int id, String name, DataType type) {
        return new DataField(id, name, type);
    }

    public static DataField FIELD(int id, String name, DataType type, String description) {
        return new DataField(id, name, type, description);
    }

    public static RowType ROW(DataField... fields) {
        return ROW(true, fields);
    }

    public static RowType ROW(DataType... fieldTypes) {
        return ROW(true, fieldTypes);
    }

    public static BinaryType BINARY(int length) {
        return BINARY(true, length);
    }

    public static VarBinaryType VARBINARY(int length) {
        return VARBINARY(true, length);
    }

    public static MultisetType MULTISET(DataType elementType) {
        return MULTISET(true, elementType);
    }

    public static RowType ROW(boolean isNullable, DataField... fields) {
        return new RowType(isNullable, Arrays.asList(fields));
    }

    public static RowType ROW(boolean isNullable, DataType... fieldTypes) {
        return RowType.builder(isNullable).fields(fieldTypes).build();
    }

    public static BinaryType BINARY(boolean isNullable, int length) {
        return new BinaryType(isNullable, length);
    }

    public static VarBinaryType VARBINARY(boolean isNullable, int length) {
        return new VarBinaryType(isNullable, length);
    }

    public static MultisetType MULTISET(boolean isNullable, DataType elementType) {
        return new MultisetType(isNullable, elementType);
    }

    public static OptionalInt getPrecision(DataType dataType) {
        return dataType.accept(PRECISION_EXTRACTOR);
    }

    public static OptionalInt getLength(DataType dataType) {
        return dataType.accept(LENGTH_EXTRACTOR);
    }

    private static final PrecisionExtractor PRECISION_EXTRACTOR = new PrecisionExtractor();

    private static final LengthExtractor LENGTH_EXTRACTOR = new LengthExtractor();

    private static class PrecisionExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(DecimalType decimalType) {
            return OptionalInt.of(decimalType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimeType timeType) {
            return OptionalInt.of(timeType.getPrecision());
        }

        @Override
        public OptionalInt visit(TimestampType timestampType) {
            return OptionalInt.of(timestampType.getPrecision());
        }

        @Override
        public OptionalInt visit(LocalZonedTimestampType localZonedTimestampType) {
            return OptionalInt.of(localZonedTimestampType.getPrecision());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }

    private static class LengthExtractor extends DataTypeDefaultVisitor<OptionalInt> {

        @Override
        public OptionalInt visit(CharType charType) {
            return OptionalInt.of(charType.getLength());
        }

        @Override
        public OptionalInt visit(VarCharType varCharType) {
            return OptionalInt.of(varCharType.getLength());
        }

        @Override
        public OptionalInt visit(BinaryType binaryType) {
            return OptionalInt.of(binaryType.getLength());
        }

        @Override
        public OptionalInt visit(VarBinaryType varBinaryType) {
            return OptionalInt.of(varBinaryType.getLength());
        }

        @Override
        protected OptionalInt defaultMethod(DataType dataType) {
            return OptionalInt.empty();
        }
    }
}
