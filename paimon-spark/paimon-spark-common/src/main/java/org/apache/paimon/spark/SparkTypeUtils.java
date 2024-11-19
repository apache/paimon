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

package org.apache.paimon.spark;

import org.apache.paimon.spark.util.shim.TypeUtils;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeDefaultVisitor;
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

    public static RowType toPartitionType(Table table) {
        int[] projections = table.rowType().getFieldIndices(table.partitionKeys());
        List<DataField> partitionTypes = new ArrayList<>();
        for (int i : projections) {
            partitionTypes.add(table.rowType().getFields().get(i));
        }
        return new RowType(false, partitionTypes);
    }

    public static StructType toSparkPartitionType(Table table) {
        return (StructType) SparkTypeUtils.fromPaimonType(toPartitionType(table));
    }

    public static StructType fromPaimonRowType(RowType type) {
        return (StructType) fromPaimonType(type);
    }

    public static DataType fromPaimonType(org.apache.paimon.types.DataType type) {
        return type.accept(PaimonToSparkTypeVisitor.INSTANCE);
    }

    public static org.apache.paimon.types.RowType toPaimonRowType(StructType type) {
        return (RowType) toPaimonType(type);
    }

    public static org.apache.paimon.types.DataType toPaimonType(DataType dataType) {
        return SparkToPaimonTypeVisitor.visit(dataType);
    }

    /**
     * Prune Paimon `RowType` by required Spark `StructType`, use this method instead of {@link
     * #toPaimonType(DataType)} when need to retain the field id.
     */
    public static RowType prunePaimonRowType(StructType requiredStructType, RowType rowType) {
        return (RowType) prunePaimonType(requiredStructType, rowType);
    }

    private static org.apache.paimon.types.DataType prunePaimonType(
            DataType sparkDataType, org.apache.paimon.types.DataType paimonDataType) {
        if (sparkDataType instanceof StructType) {
            StructType s = (StructType) sparkDataType;
            RowType p = (RowType) paimonDataType;
            List<DataField> newFields = new ArrayList<>();
            for (StructField field : s.fields()) {
                DataField f = p.getField(field.name());
                newFields.add(f.newType(prunePaimonType(field.dataType(), f.type())));
            }
            return p.copy(newFields);
        } else if (sparkDataType instanceof org.apache.spark.sql.types.MapType) {
            org.apache.spark.sql.types.MapType s =
                    (org.apache.spark.sql.types.MapType) sparkDataType;
            MapType p = (MapType) paimonDataType;
            return p.newKeyValueType(
                    prunePaimonType(s.keyType(), p.getKeyType()),
                    prunePaimonType(s.valueType(), p.getValueType()));
        } else if (sparkDataType instanceof org.apache.spark.sql.types.ArrayType) {
            org.apache.spark.sql.types.ArrayType s =
                    (org.apache.spark.sql.types.ArrayType) sparkDataType;
            ArrayType r = (ArrayType) paimonDataType;
            return r.newElementType(prunePaimonType(s.elementType(), r.getElementType()));
        } else {
            return paimonDataType;
        }
    }

    private static class PaimonToSparkTypeVisitor extends DataTypeDefaultVisitor<DataType> {

        private static final PaimonToSparkTypeVisitor INSTANCE = new PaimonToSparkTypeVisitor();

        @Override
        public DataType visit(CharType charType) {
            return new org.apache.spark.sql.types.CharType(charType.getLength());
        }

        @Override
        public DataType visit(VarCharType varCharType) {
            if (varCharType.getLength() == VarCharType.MAX_LENGTH) {
                return DataTypes.StringType;
            } else {
                return new org.apache.spark.sql.types.VarcharType(varCharType.getLength());
            }
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
        public DataType visit(TimeType timeType) {
            return DataTypes.IntegerType;
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            if (TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType()) {
                return DataTypes.TimestampType;
            } else {
                return DataTypes.TimestampNTZType;
            }
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return DataTypes.TimestampType;
        }

        @Override
        public DataType visit(ArrayType arrayType) {
            org.apache.paimon.types.DataType elementType = arrayType.getElementType();
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

        /**
         * For simplicity, as a temporary solution, we directly convert the non-null attribute to
         * nullable on the Spark side.
         */
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
        protected DataType defaultMethod(org.apache.paimon.types.DataType dataType) {
            throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private static class SparkToPaimonTypeVisitor {

        static org.apache.paimon.types.DataType visit(DataType type) {
            AtomicInteger atomicInteger = new AtomicInteger(-1);
            return visit(type, new SparkToPaimonTypeVisitor(), atomicInteger);
        }

        static org.apache.paimon.types.DataType visit(
                DataType type, SparkToPaimonTypeVisitor visitor, AtomicInteger atomicInteger) {
            if (type instanceof StructType) {
                StructField[] fields = ((StructType) type).fields();
                List<org.apache.paimon.types.DataType> fieldResults =
                        new ArrayList<>(fields.length);

                for (StructField field : fields) {
                    fieldResults.add(visit(field.dataType(), visitor, atomicInteger));
                }

                return visitor.struct((StructType) type, fieldResults, atomicInteger);

            } else if (type instanceof org.apache.spark.sql.types.MapType) {
                return visitor.map(
                        (org.apache.spark.sql.types.MapType) type,
                        visit(
                                ((org.apache.spark.sql.types.MapType) type).keyType(),
                                visitor,
                                atomicInteger),
                        visit(
                                ((org.apache.spark.sql.types.MapType) type).valueType(),
                                visitor,
                                atomicInteger));

            } else if (type instanceof org.apache.spark.sql.types.ArrayType) {
                return visitor.array(
                        (org.apache.spark.sql.types.ArrayType) type,
                        visit(
                                ((org.apache.spark.sql.types.ArrayType) type).elementType(),
                                visitor,
                                atomicInteger));

            } else if (type instanceof UserDefinedType) {
                throw new UnsupportedOperationException("User-defined types are not supported");

            } else {
                return visitor.atomic(type);
            }
        }

        public org.apache.paimon.types.DataType struct(
                StructType struct,
                List<org.apache.paimon.types.DataType> fieldResults,
                AtomicInteger atomicInteger) {
            StructField[] fields = struct.fields();
            List<DataField> newFields = new ArrayList<>(fields.length);
            for (int i = 0; i < fields.length; i++) {
                StructField field = fields[i];
                org.apache.paimon.types.DataType fieldType =
                        fieldResults.get(i).copy(field.nullable());
                String comment = field.getComment().getOrElse(() -> null);
                newFields.add(
                        new DataField(
                                atomicInteger.incrementAndGet(), field.name(), fieldType, comment));
            }

            return new RowType(newFields);
        }

        public org.apache.paimon.types.DataType array(
                org.apache.spark.sql.types.ArrayType array,
                org.apache.paimon.types.DataType elementResult) {
            return new ArrayType(elementResult.copy(array.containsNull()));
        }

        public org.apache.paimon.types.DataType map(
                org.apache.spark.sql.types.MapType map,
                org.apache.paimon.types.DataType keyResult,
                org.apache.paimon.types.DataType valueResult) {
            return new MapType(keyResult.copy(false), valueResult.copy(map.valueContainsNull()));
        }

        public org.apache.paimon.types.DataType atomic(DataType atomic) {
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
            } else if (atomic instanceof org.apache.spark.sql.types.VarcharType) {
                return new VarCharType(((org.apache.spark.sql.types.VarcharType) atomic).length());
            } else if (atomic instanceof org.apache.spark.sql.types.CharType) {
                return new CharType(((org.apache.spark.sql.types.CharType) atomic).length());
            } else if (atomic instanceof org.apache.spark.sql.types.DateType) {
                return new DateType();
            } else if (atomic instanceof org.apache.spark.sql.types.TimestampType) {
                if (TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType()) {
                    return new TimestampType();
                } else {
                    return new LocalZonedTimestampType();
                }
            } else if (atomic instanceof org.apache.spark.sql.types.DecimalType) {
                return new DecimalType(
                        ((org.apache.spark.sql.types.DecimalType) atomic).precision(),
                        ((org.apache.spark.sql.types.DecimalType) atomic).scale());
            } else if (atomic instanceof org.apache.spark.sql.types.BinaryType) {
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            } else if (atomic instanceof org.apache.spark.sql.types.TimestampNTZType) {
                // Move TimestampNTZType to the end for compatibility with spark3.3 and below
                return new TimestampType();
            }

            throw new UnsupportedOperationException(
                    "Not a supported type: " + atomic.catalogString());
        }
    }
}
