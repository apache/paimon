/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.spark;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/** Utils for Spark {@link DataType}. */
public class SparkTypeUtils {

    private SparkTypeUtils() {}

    public static StructType fromFlinkRowType(RowType type) {
        return (StructType) fromFlinkType(type);
    }

    public static DataType fromFlinkType(LogicalType type) {
        return type.accept(FlinkToSparkTypeVisitor.INSTANCE);
    }

    private static class FlinkToSparkTypeVisitor extends LogicalTypeDefaultVisitor<DataType> {

        private static final FlinkToSparkTypeVisitor INSTANCE = new FlinkToSparkTypeVisitor();

        @Override
        public DataType visit(CharType charType) {
            return DataTypes.StringType;
        }

        @Override
        public DataType visit(VarCharType varCharType) {
            return DataTypes.StringType;
        }

        @Override
        public DataType visit(BooleanType booleanType) {
            return DataTypes.BooleanType;
        }

        @Override
        public DataType visit(BinaryType binaryType) {
            return DataTypes.BinaryType;
        }

        @Override
        public DataType visit(VarBinaryType varBinaryType) {
            return DataTypes.BinaryType;
        }

        @Override
        public DataType visit(DecimalType decimalType) {
            return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public DataType visit(TinyIntType tinyIntType) {
            return DataTypes.ByteType;
        }

        @Override
        public DataType visit(SmallIntType smallIntType) {
            return DataTypes.ShortType;
        }

        @Override
        public DataType visit(IntType intType) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(BigIntType bigIntType) {
            return DataTypes.LongType;
        }

        @Override
        public DataType visit(FloatType floatType) {
            return DataTypes.FloatType;
        }

        @Override
        public DataType visit(DoubleType doubleType) {
            return DataTypes.DoubleType;
        }

        @Override
        public DataType visit(DateType dateType) {
            return DataTypes.DateType;
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            return DataTypes.TimestampType;
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return DataTypes.TimestampType;
        }

        @Override
        public DataType visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            return DataTypes.createArrayType(elementType.accept(this), elementType.isNullable());
        }

        @Override
        public DataType visit(MultisetType multisetType) {
            return DataTypes.createMapType(
                    multisetType.getElementType().accept(this), DataTypes.IntegerType, false);
        }

        @Override
        public DataType visit(MapType mapType) {
            return DataTypes.createMapType(
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this),
                    mapType.getValueType().isNullable());
        }

        @Override
        public DataType visit(RowType rowType) {
            List<StructField> fields = new ArrayList<>(rowType.getFieldCount());
            for (RowField field : rowType.getFields()) {
                StructField structField =
                        DataTypes.createStructField(
                                field.getName(),
                                field.getType().accept(this),
                                field.getType().isNullable());
                structField =
                        field.getDescription().map(structField::withComment).orElse(structField);
                fields.add(structField);
            }
            return DataTypes.createStructType(fields);
        }

        @Override
        protected DataType defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }
}
