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

package org.apache.paimon.format.vortex;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VectorType;

import dev.vortex.api.DType;

import java.util.List;
import java.util.Optional;

/** Utilities for converting Paimon types to Vortex DType. */
public class VortexTypeUtils {

    public static DType toDType(RowType rowType) {
        // Vortex does not support nullable top-level structs
        return toStructDType(rowType, false);
    }

    private static DType toStructDType(RowType rowType, boolean isNullable) {
        List<DataField> fields = rowType.getFields();
        String[] fieldNames = new String[fields.size()];
        DType[] fieldTypes = new DType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            fieldNames[i] = field.name();
            fieldTypes[i] = field.type().accept(new VortexTypeVisitor());
        }
        return DType.newStruct(fieldNames, fieldTypes, isNullable);
    }

    private static class VortexTypeVisitor extends DataTypeDefaultVisitor<DType> {

        @Override
        public DType visit(TinyIntType tinyIntType) {
            return DType.newByte(tinyIntType.isNullable());
        }

        @Override
        public DType visit(SmallIntType smallIntType) {
            return DType.newShort(smallIntType.isNullable());
        }

        @Override
        public DType visit(IntType intType) {
            return DType.newInt(intType.isNullable());
        }

        @Override
        public DType visit(BigIntType bigIntType) {
            return DType.newLong(bigIntType.isNullable());
        }

        @Override
        public DType visit(FloatType floatType) {
            return DType.newFloat(floatType.isNullable());
        }

        @Override
        public DType visit(DoubleType doubleType) {
            return DType.newDouble(doubleType.isNullable());
        }

        @Override
        public DType visit(CharType charType) {
            return DType.newUtf8(charType.isNullable());
        }

        @Override
        public DType visit(VarCharType varCharType) {
            return DType.newUtf8(varCharType.isNullable());
        }

        @Override
        public DType visit(BooleanType booleanType) {
            return DType.newBool(booleanType.isNullable());
        }

        @Override
        public DType visit(BinaryType binaryType) {
            return DType.newBinary(binaryType.isNullable());
        }

        @Override
        public DType visit(VarBinaryType varBinaryType) {
            return DType.newBinary(varBinaryType.isNullable());
        }

        @Override
        public DType visit(DecimalType decimalType) {
            return DType.newDecimal(
                    decimalType.getPrecision(), decimalType.getScale(), decimalType.isNullable());
        }

        @Override
        public DType visit(DateType dateType) {
            return DType.newDate(DType.TimeUnit.DAYS, dateType.isNullable());
        }

        @Override
        public DType visit(TimeType timeType) {
            return DType.newTime(DType.TimeUnit.MILLISECONDS, timeType.isNullable());
        }

        @Override
        public DType visit(TimestampType timestampType) {
            DType.TimeUnit unit;
            int precision = timestampType.getPrecision();
            if (precision <= 0) {
                unit = DType.TimeUnit.SECONDS;
            } else if (precision <= 3) {
                unit = DType.TimeUnit.MILLISECONDS;
            } else if (precision <= 6) {
                unit = DType.TimeUnit.MICROSECONDS;
            } else {
                unit = DType.TimeUnit.NANOSECONDS;
            }
            return DType.newTimestamp(unit, Optional.empty(), timestampType.isNullable());
        }

        @Override
        public DType visit(LocalZonedTimestampType lzTimestampType) {
            DType.TimeUnit unit;
            int precision = lzTimestampType.getPrecision();
            if (precision <= 0) {
                unit = DType.TimeUnit.SECONDS;
            } else if (precision <= 3) {
                unit = DType.TimeUnit.MILLISECONDS;
            } else if (precision <= 6) {
                unit = DType.TimeUnit.MICROSECONDS;
            } else {
                unit = DType.TimeUnit.NANOSECONDS;
            }
            return DType.newTimestamp(unit, Optional.of("UTC"), lzTimestampType.isNullable());
        }

        @Override
        public DType visit(ArrayType arrayType) {
            DType elementType = arrayType.getElementType().accept(this);
            return DType.newList(elementType, arrayType.isNullable());
        }

        @Override
        public DType visit(VectorType vectorType) {
            DType elementType = vectorType.getElementType().accept(this);
            return DType.newFixedSizeList(
                    elementType, vectorType.getLength(), vectorType.isNullable());
        }

        @Override
        public DType visit(RowType rowType) {
            return VortexTypeUtils.toStructDType(rowType, rowType.isNullable());
        }

        @Override
        protected DType defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    "Vortex does not support type: " + dataType.asSQLString());
        }
    }
}
