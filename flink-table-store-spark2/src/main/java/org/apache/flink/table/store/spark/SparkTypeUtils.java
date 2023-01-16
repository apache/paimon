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

import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.BinaryType;
import org.apache.flink.table.store.types.BooleanType;
import org.apache.flink.table.store.types.CharType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataTypeDefaultVisitor;
import org.apache.flink.table.store.types.DateType;
import org.apache.flink.table.store.types.DecimalType;
import org.apache.flink.table.store.types.DoubleType;
import org.apache.flink.table.store.types.FloatType;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.LocalZonedTimestampType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.MultisetType;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.types.SmallIntType;
import org.apache.flink.table.store.types.TimestampType;
import org.apache.flink.table.store.types.TinyIntType;
import org.apache.flink.table.store.types.VarBinaryType;
import org.apache.flink.table.store.types.VarCharType;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Utils for Spark {@link DataType}. */
public class SparkTypeUtils {

    private SparkTypeUtils() {}

    public static StructType fromFlinkRowType(RowType type) {
        return (StructType) fromFlinkType(type);
    }

    public static DataType fromFlinkType(org.apache.flink.table.store.types.DataType type) {
        return type.accept(FlinkToSparkTypeVisitor.INSTANCE);
    }

    private static class FlinkToSparkTypeVisitor extends DataTypeDefaultVisitor<DataType> {

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
            org.apache.flink.table.store.types.DataType elementType = arrayType.getElementType();
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
            for (DataField field : rowType.getFields()) {
                StructField structField =
                        DataTypes.createStructField(
                                field.name(), field.type().accept(this), field.type().isNullable());
                structField =
                        Optional.ofNullable(field.description())
                                .map(structField::withComment)
                                .orElse(structField);
                fields.add(structField);
            }
            return DataTypes.createStructType(fields);
        }

        @Override
        protected DataType defaultMethod(org.apache.flink.table.store.types.DataType logicalType) {
            throw new UnsupportedOperationException("Unsupported type: " + logicalType);
        }
    }
}
