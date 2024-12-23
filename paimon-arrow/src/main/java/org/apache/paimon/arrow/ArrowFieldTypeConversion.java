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

package org.apache.paimon.arrow;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
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

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.time.ZoneId;

/** Utils for conversion between Paimon {@link DataType} and Arrow {@link FieldType}. */
public class ArrowFieldTypeConversion {

    public static final ArrowFieldTypeVisitor ARROW_FIELD_TYPE_VISITOR =
            new ArrowFieldTypeVisitor();

    /** Convert {@link DataType} to {@link FieldType}. */
    public static class ArrowFieldTypeVisitor implements DataTypeVisitor<FieldType> {

        @Override
        public FieldType visit(CharType charType) {
            return new FieldType(charType.isNullable(), Types.MinorType.VARCHAR.getType(), null);
        }

        @Override
        public FieldType visit(VarCharType varCharType) {
            return new FieldType(varCharType.isNullable(), Types.MinorType.VARCHAR.getType(), null);
        }

        @Override
        public FieldType visit(BooleanType booleanType) {
            return new FieldType(booleanType.isNullable(), Types.MinorType.BIT.getType(), null);
        }

        @Override
        public FieldType visit(BinaryType binaryType) {
            return new FieldType(
                    binaryType.isNullable(), Types.MinorType.VARBINARY.getType(), null);
        }

        @Override
        public FieldType visit(VarBinaryType varBinaryType) {
            return new FieldType(
                    varBinaryType.isNullable(), Types.MinorType.VARBINARY.getType(), null);
        }

        @Override
        public FieldType visit(DecimalType decimalType) {
            return new FieldType(
                    decimalType.isNullable(),
                    new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128),
                    null);
        }

        @Override
        public FieldType visit(TinyIntType tinyIntType) {
            return new FieldType(tinyIntType.isNullable(), Types.MinorType.TINYINT.getType(), null);
        }

        @Override
        public FieldType visit(SmallIntType smallIntType) {
            return new FieldType(
                    smallIntType.isNullable(), Types.MinorType.SMALLINT.getType(), null);
        }

        @Override
        public FieldType visit(IntType intType) {
            return new FieldType(intType.isNullable(), Types.MinorType.INT.getType(), null);
        }

        @Override
        public FieldType visit(BigIntType bigIntType) {
            return new FieldType(bigIntType.isNullable(), Types.MinorType.BIGINT.getType(), null);
        }

        @Override
        public FieldType visit(FloatType floatType) {
            return new FieldType(floatType.isNullable(), Types.MinorType.FLOAT4.getType(), null);
        }

        @Override
        public FieldType visit(DoubleType doubleType) {
            return new FieldType(doubleType.isNullable(), Types.MinorType.FLOAT8.getType(), null);
        }

        @Override
        public FieldType visit(DateType dateType) {
            return new FieldType(dateType.isNullable(), Types.MinorType.DATEDAY.getType(), null);
        }

        @Override
        public FieldType visit(TimeType timeType) {
            return new FieldType(timeType.isNullable(), Types.MinorType.TIMEMILLI.getType(), null);
        }

        @Override
        public FieldType visit(TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            TimeUnit timeUnit = getTimeUnit(precision);
            ArrowType arrowType = new ArrowType.Timestamp(timeUnit, null);
            return new FieldType(timestampType.isNullable(), arrowType, null);
        }

        @Override
        public FieldType visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = localZonedTimestampType.getPrecision();
            TimeUnit timeUnit = getTimeUnit(precision);
            ArrowType arrowType =
                    new ArrowType.Timestamp(timeUnit, ZoneId.systemDefault().toString());
            return new FieldType(localZonedTimestampType.isNullable(), arrowType, null);
        }

        @Override
        public FieldType visit(VariantType variantType) {
            throw new UnsupportedOperationException();
        }

        private TimeUnit getTimeUnit(int precision) {
            if (precision == 0) {
                return TimeUnit.SECOND;
            } else if (precision >= 1 && precision <= 3) {
                return TimeUnit.MILLISECOND;
            } else if (precision >= 4 && precision <= 6) {
                return TimeUnit.MICROSECOND;
            } else {
                return TimeUnit.NANOSECOND;
            }
        }

        @Override
        public FieldType visit(ArrayType arrayType) {
            return new FieldType(arrayType.isNullable(), Types.MinorType.LIST.getType(), null);
        }

        @Override
        public FieldType visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Doesn't support MultisetType.");
        }

        @Override
        public FieldType visit(MapType mapType) {
            return new FieldType(mapType.isNullable(), new ArrowType.Map(false), null);
        }

        @Override
        public FieldType visit(RowType rowType) {
            return new FieldType(rowType.isNullable(), Types.MinorType.STRUCT.getType(), null);
        }
    }
}
