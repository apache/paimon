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

package org.apache.paimon.spark.utils;

import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;

/** Utils of catalog. */
public class CatalogUtils {

    public static void checkNamespace(String[] namespace, String catalogName) {
        checkArgument(
                namespace.length == 1,
                "Current catalog is %s, catalog %s does not exist or Paimon only support single namespace, but got %s",
                catalogName,
                namespace.length > 0 ? namespace[0] : "unknown",
                Arrays.toString(namespace));
    }

    public static org.apache.paimon.catalog.Identifier toIdentifier(
            Identifier ident, String catalogName) {
        checkNamespace(ident.namespace(), catalogName);
        return new org.apache.paimon.catalog.Identifier(ident.namespace()[0], ident.name());
    }

    public static Identifier removeCatalogName(Identifier ident, String catalogName) {
        String[] namespace = ident.namespace();
        if (namespace.length > 1) {
            checkArgument(
                    namespace[0].equals(catalogName),
                    "Only supports operations within the same catalog, target catalog name: %s, current catalog name: %s",
                    namespace[0],
                    catalogName);
            return Identifier.of(Arrays.copyOfRange(namespace, 1, namespace.length), ident.name());
        } else {
            return ident;
        }
    }

    public static org.apache.spark.sql.types.DataType paimonType2SparkType(
            org.apache.paimon.types.DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringType;
            case BOOLEAN:
                return BooleanType;
            case BINARY:
            case VARBINARY:
                return BinaryType;
            case TINYINT:
                return ByteType;
            case SMALLINT:
                return ShortType;
            case INTEGER:
                return IntegerType;
            case BIGINT:
                return LongType;
            case FLOAT:
                return FloatType;
            case DOUBLE:
                return DoubleType;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return DataTypes.createDecimalType(
                        decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampType;
            case ARRAY:
                return createArrayType(paimonType2SparkType(((ArrayType) type).getElementType()));
            case MAP:
            case MULTISET:
                DataType keyType;
                DataType valueType;
                if (type instanceof MapType) {
                    keyType = ((MapType) type).getKeyType();
                    valueType = ((MapType) type).getValueType();
                } else if (type instanceof MultisetType) {
                    keyType = ((MultisetType) type).getElementType();
                    valueType = new IntType();
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
                return DataTypes.createMapType(
                        paimonType2SparkType(keyType), paimonType2SparkType(valueType));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    public static String paimonType2JavaType(org.apache.paimon.types.DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return "java.lang.String";
            case BOOLEAN:
                return "java.lang.Boolean";
            case BINARY:
            case VARBINARY:
                return "byte[]";
            case TINYINT:
                return "java.lang.Byte";
            case SMALLINT:
                return "java.lang.Short";
            case INTEGER:
                return "java.lang.Integer";
            case BIGINT:
                return "java.lang.Long";
            case FLOAT:
                return "java.lang.Float";
            case DOUBLE:
                return "java.lang.Double";
            case DECIMAL:
                return "java.math.BigDecimal";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "java.time.Instant";
            case ARRAY:
                return paimonType2JavaType(((ArrayType) type).getElementType()) + "[]";
            case MAP:
            case MULTISET:
                DataType keyType;
                DataType valueType;
                if (type instanceof MapType) {
                    keyType = ((MapType) type).getKeyType();
                    valueType = ((MapType) type).getValueType();
                } else if (type instanceof MultisetType) {
                    keyType = ((MultisetType) type).getElementType();
                    valueType = new IntType();
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type);
                }
                return String.format(
                        "java.util.Map<%s,%s>",
                        paimonType2JavaType(keyType), paimonType2JavaType(valueType));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    public static Object convertSparkJavaToPaimonJava(
            org.apache.spark.sql.types.DataType sparkType, Object value) {
        if (value == null) {
            return null;
        }
        if (sparkType == StringType) {
            return (String) value.toString();
        } else if (sparkType == BooleanType) {
            return (Boolean) value;
        } else if (sparkType == BinaryType) {
            return (byte[]) value;
        } else if (sparkType == ByteType) {
            return (byte) value;
        } else if (sparkType == ShortType) {
            return (short) value;
        } else if (sparkType == IntegerType) {
            return (Integer) value;
        } else if (sparkType == LongType) {
            return (Long) value;
        } else if (sparkType == FloatType) {
            return (Float) value;
        } else if (sparkType == DoubleType) {
            return (Double) value;
        } else if (sparkType instanceof org.apache.spark.sql.types.DecimalType) {
            return (java.math.BigDecimal) value;
        } else if (sparkType instanceof org.apache.spark.sql.types.ArrayType) {
            org.apache.spark.sql.types.ArrayType arrayType =
                    (org.apache.spark.sql.types.ArrayType) sparkType;
            List<Object> list = new ArrayList<>();
            if (value instanceof GenericArrayData) {
                GenericArrayData genericArray = (GenericArrayData) value;
                Object[] array = genericArray.array();
                for (Object elem : array) {
                    list.add(convertSparkJavaToPaimonJava(arrayType.elementType(), elem));
                }
                return list;
            } else {
                throw new IllegalArgumentException("Unexpected array type: " + value.getClass());
            }
        } else if (sparkType instanceof org.apache.spark.sql.types.MapType) {
            org.apache.spark.sql.types.MapType mapType =
                    (org.apache.spark.sql.types.MapType) sparkType;
            if (value instanceof ArrayBasedMapData) {
                ArrayBasedMapData arrayBasedMapData = (ArrayBasedMapData) value;
                Map<Object, Object> sparkMap =
                        ArrayBasedMapData.toJavaMap(
                                arrayBasedMapData.keyArray().array(),
                                arrayBasedMapData.valueArray().array());
                Map<Object, Object> javaMap = new HashMap<>();
                for (Map.Entry<Object, Object> entry : sparkMap.entrySet()) {
                    Object key = convertSparkJavaToPaimonJava(mapType.keyType(), entry.getKey());
                    Object val =
                            convertSparkJavaToPaimonJava(mapType.valueType(), entry.getValue());
                    javaMap.put(key, val);
                }
                return javaMap;
            } else {
                throw new IllegalArgumentException("Unexpected array type: " + value.getClass());
            }
        }

        throw new IllegalArgumentException("Unsupported Spark data type: " + sparkType);
    }

    public static void checkNoDefaultValue(TableChange.AddColumn addColumn) {
        try {
            ColumnDefaultValue defaultValue = addColumn.defaultValue();
            if (defaultValue != null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot add column %s with default value %s.",
                                Arrays.toString(addColumn.fieldNames()), defaultValue));
            }
        } catch (NoClassDefFoundError | NoSuchMethodError ignored) {
        }
    }

    public static boolean isUpdateColumnDefaultValue(TableChange tableChange) {
        try {
            return tableChange instanceof TableChange.UpdateColumnDefaultValue;
        } catch (NoClassDefFoundError ignored) {
            return false;
        }
    }

    public static SchemaChange toUpdateColumnDefaultValue(TableChange tableChange) {
        TableChange.UpdateColumnDefaultValue update =
                (TableChange.UpdateColumnDefaultValue) tableChange;
        return SchemaChange.updateColumnDefaultValue(update.fieldNames(), update.newDefaultValue());
    }
}
