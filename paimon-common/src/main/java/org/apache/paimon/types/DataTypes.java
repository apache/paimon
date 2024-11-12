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

/**
 * Utils for creating {@link DataType}s.
 *
 * @since 0.4.0
 */
@Public
public class DataTypes {

    public static IntType INT() {
        return new IntType();
    }

    public static TinyIntType TINYINT() {
        return new TinyIntType();
    }

    public static SmallIntType SMALLINT() {
        return new SmallIntType();
    }

    public static BigIntType BIGINT() {
        return new BigIntType();
    }

    public static VarCharType STRING() {
        return VarCharType.STRING_TYPE;
    }

    public static DoubleType DOUBLE() {
        return new DoubleType();
    }

    public static ArrayType ARRAY(DataType element) {
        return new ArrayType(element);
    }

    public static CharType CHAR(int length) {
        return new CharType(length);
    }

    public static VarCharType VARCHAR(int length) {
        return new VarCharType(length);
    }

    public static BooleanType BOOLEAN() {
        return new BooleanType();
    }

    public static DateType DATE() {
        return new DateType();
    }

    public static TimeType TIME() {
        return new TimeType();
    }

    public static TimeType TIME(int precision) {
        return new TimeType(precision);
    }

    public static TimestampType TIMESTAMP() {
        return new TimestampType();
    }

    public static TimestampType TIMESTAMP_MILLIS() {
        return new TimestampType(3);
    }

    public static TimestampType TIMESTAMP(int precision) {
        return new TimestampType(precision);
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE() {
        return new LocalZonedTimestampType();
    }

    public static LocalZonedTimestampType TIMESTAMP_WITH_LOCAL_TIME_ZONE(int precision) {
        return new LocalZonedTimestampType(precision);
    }

    public static LocalZonedTimestampType TIMESTAMP_LTZ_MILLIS() {
        return new LocalZonedTimestampType(3);
    }

    public static DecimalType DECIMAL(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    public static VarBinaryType BYTES() {
        return new VarBinaryType(VarBinaryType.MAX_LENGTH);
    }

    public static FloatType FLOAT() {
        return new FloatType();
    }

    public static MapType MAP(DataType keyType, DataType valueType) {
        return new MapType(keyType, valueType);
    }

    public static DataField FIELD(int id, String name, DataType type) {
        return new DataField(id, name, type);
    }

    public static DataField FIELD(int id, String name, DataType type, String description) {
        return new DataField(id, name, type, description);
    }

    public static RowType ROW(DataField... fields) {
        return new RowType(Arrays.asList(fields));
    }

    public static RowType ROW(DataType... fieldTypes) {
        return RowType.builder().fields(fieldTypes).build();
    }

    public static BinaryType BINARY(int length) {
        return new BinaryType(length);
    }

    public static VarBinaryType VARBINARY(int length) {
        return new VarBinaryType(length);
    }

    public static MultisetType MULTISET(DataType elementType) {
        return new MultisetType(elementType);
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
