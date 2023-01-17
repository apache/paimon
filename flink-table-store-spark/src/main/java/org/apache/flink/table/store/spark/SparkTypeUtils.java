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
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/** Utils for spark {@link DataType}. */
public class SparkTypeUtils {

    private SparkTypeUtils() {}

    public static StructType fromFlinkRowType(RowType type) {
        return (StructType) fromFlinkType(type);
    }

    public static DataType fromFlinkType(org.apache.flink.table.store.types.DataType type) {
        return type.accept(FlinkToSparkTypeVisitor.INSTANCE);
    }

    public static org.apache.flink.table.store.types.DataType toFlinkType(DataType dataType) {
        return SparkToFlinkTypeVisitor.visit(dataType);
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
        protected DataType defaultMethod(org.apache.flink.table.store.types.DataType dataType) {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static class SparkToFlinkTypeVisitor {

        private final AtomicInteger currentIndex = new AtomicInteger(0);

        static org.apache.flink.table.store.types.DataType visit(DataType type) {
            return visit(type, new SparkToFlinkTypeVisitor());
        }

        static org.apache.flink.table.store.types.DataType visit(
                DataType type, SparkToFlinkTypeVisitor visitor) {
            if (type instanceof StructType) {
                StructField[] fields = ((StructType) type).fields();
                List<org.apache.flink.table.store.types.DataType> fieldResults =
                        new ArrayList<>(fields.length);

                for (StructField field : fields) {
                    fieldResults.add(visit(field.dataType(), visitor));
                }

                return visitor.struct((StructType) type, fieldResults);

            } else if (type instanceof org.apache.spark.sql.types.MapType) {
                return visitor.map(
                        (org.apache.spark.sql.types.MapType) type,
                        visit(((org.apache.spark.sql.types.MapType) type).keyType(), visitor),
                        visit(((org.apache.spark.sql.types.MapType) type).valueType(), visitor));

            } else if (type instanceof org.apache.spark.sql.types.ArrayType) {
                return visitor.array(
                        (org.apache.spark.sql.types.ArrayType) type,
                        visit(
                                ((org.apache.spark.sql.types.ArrayType) type).elementType(),
                                visitor));

            } else if (type instanceof UserDefinedType) {
                throw new UnsupportedOperationException("User-defined types are not supported");

            } else {
                return visitor.atomic(type);
            }
        }

        public org.apache.flink.table.store.types.DataType struct(
                StructType struct, List<org.apache.flink.table.store.types.DataType> fieldResults) {
            StructField[] fields = struct.fields();
            List<DataField> newFields = new ArrayList<>(fields.length);
            for (int i = 0; i < fields.length; i += 1) {
                StructField field = fields[i];
                org.apache.flink.table.store.types.DataType fieldType =
                        fieldResults.get(i).copy(field.nullable());
                String comment = field.getComment().getOrElse(() -> null);
                newFields.add(
                        new DataField(
                                currentIndex.getAndIncrement(), field.name(), fieldType, comment));
            }

            return new RowType(newFields);
        }

        public org.apache.flink.table.store.types.DataType array(
                org.apache.spark.sql.types.ArrayType array,
                org.apache.flink.table.store.types.DataType elementResult) {
            return new ArrayType(elementResult.copy(array.containsNull()));
        }

        public org.apache.flink.table.store.types.DataType map(
                org.apache.spark.sql.types.MapType map,
                org.apache.flink.table.store.types.DataType keyResult,
                org.apache.flink.table.store.types.DataType valueResult) {
            return new MapType(keyResult.copy(false), valueResult.copy(map.valueContainsNull()));
        }

        public org.apache.flink.table.store.types.DataType atomic(DataType atomic) {
            if (atomic instanceof org.apache.spark.sql.types.BooleanType) {
                return new BooleanType();
            } else if (atomic instanceof org.apache.spark.sql.types.ByteType) {
                return new TinyIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.ShortType) {
                return new SmallIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.IntegerType) {
                return new IntType();
            } else if (atomic instanceof LongType) {
                return new BigIntType();
            } else if (atomic instanceof org.apache.spark.sql.types.FloatType) {
                return new FloatType();
            } else if (atomic instanceof org.apache.spark.sql.types.DoubleType) {
                return new DoubleType();
            } else if (atomic instanceof org.apache.spark.sql.types.StringType) {
                return new VarCharType(VarCharType.MAX_LENGTH);
            } else if (atomic instanceof org.apache.spark.sql.types.DateType) {
                return new DateType();
            } else if (atomic instanceof org.apache.spark.sql.types.TimestampType) {
                return new TimestampType();
            } else if (atomic instanceof org.apache.spark.sql.types.DecimalType) {
                return new DecimalType(
                        ((org.apache.spark.sql.types.DecimalType) atomic).precision(),
                        ((org.apache.spark.sql.types.DecimalType) atomic).scale());
            } else if (atomic instanceof org.apache.spark.sql.types.BinaryType) {
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            }

            throw new UnsupportedOperationException(
                    "Not a supported type: " + atomic.catalogString());
        }
    }
}
