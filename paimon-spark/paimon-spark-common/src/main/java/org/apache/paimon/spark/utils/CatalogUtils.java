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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;

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

    public static void checkNamespace(String[] namespace) {
        checkArgument(
                namespace.length == 1,
                "Paimon only support single namespace, but got %s",
                Arrays.toString(namespace));
    }

    public static org.apache.paimon.catalog.Identifier toIdentifier(Identifier ident) {
        checkNamespace(ident.namespace());
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
                        "MAP<%s,%s>", paimonType2JavaType(keyType), paimonType2JavaType(valueType));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
