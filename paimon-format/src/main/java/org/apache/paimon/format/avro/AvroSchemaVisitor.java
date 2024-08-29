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

package org.apache.paimon.format.avro;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;

/** Visitor to visit {@link Schema}. */
public interface AvroSchemaVisitor<T> {

    default T visit(Schema schema, DataType type) {
        switch (schema.getType()) {
            case RECORD:
                return visitRecord(
                        schema,
                        type == null ? Collections.emptyList() : ((RowType) type).getFields());

            case UNION:
                return visitUnion(schema, type);

            case ARRAY:
                if (schema.getLogicalType() instanceof LogicalMap) {
                    MapType mapType = (MapType) type;
                    return visitArrayMap(
                            schema,
                            mapType == null ? null : mapType.getKeyType(),
                            mapType == null ? null : mapType.getValueType());
                } else {
                    return visitArray(
                            schema, type == null ? null : ((ArrayType) type).getElementType());
                }

            case MAP:
                DataType valueType =
                        type == null
                                ? null
                                : type instanceof MapType
                                        ? ((MapType) type).getValueType()
                                        : DataTypes.INT();
                return visitMap(schema, valueType);

            default:
                return primitive(schema, type);
        }
    }

    default T primitive(Schema primitive, DataType type) {
        LogicalType logicalType = primitive.getLogicalType();
        if (logicalType != null) {
            switch (logicalType.getName()) {
                case "date":
                case "time-millis":
                    return visitInt();

                case "timestamp-millis":
                case "local-timestamp-millis":
                    return visitTimestampMillis(getPrecision(type));

                case "timestamp-micros":
                case "local-timestamp-micros":
                    return visitTimestampMicros(getPrecision(type));

                case "decimal":
                    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                    return visitDecimal(decimal.getPrecision(), decimal.getScale());

                default:
                    throw new IllegalArgumentException("Unknown logical type: " + logicalType);
            }
        }

        switch (primitive.getType()) {
            case BOOLEAN:
                return visitBoolean();
            case INT:
                if (type == null) {
                    return visitInt();
                }
                switch (type.getTypeRoot()) {
                    case TINYINT:
                        return visitTinyInt();
                    case SMALLINT:
                        return visitSmallInt();
                    default:
                        return visitInt();
                }
            case LONG:
                return visitBigInt();
            case FLOAT:
                return visitFloat();
            case DOUBLE:
                return visitDouble();
            case STRING:
                return visitString();
            case BYTES:
                return visitBytes();
            default:
                throw new IllegalArgumentException("Unsupported type: " + primitive);
        }
    }

    T visitUnion(Schema schema, DataType type);

    T visitString();

    T visitBytes();

    T visitInt();

    T visitTinyInt();

    T visitSmallInt();

    T visitBoolean();

    T visitBigInt();

    T visitFloat();

    T visitDouble();

    T visitTimestampMillis(Integer precision);

    T visitTimestampMicros(Integer precision);

    T visitDecimal(Integer precision, Integer scale);

    T visitArray(Schema schema, DataType elementType);

    T visitArrayMap(Schema schema, DataType keyType, DataType valueType);

    T visitMap(Schema schema, DataType valueType);

    T visitRecord(Schema schema, @Nonnull List<DataField> fields);
}
