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

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

/**
 * Visitor to visit {@link Schema} which could generate reader without type, but that reader should
 * only call to skip.
 */
public interface AvroSchemaTypelessReader<T> extends AvroSchemaVisitor<T> {

    default T visit(Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                return visitRecord(schema);

            case UNION:
                return visitUnion(schema);

            case ARRAY:
                return visitArray(schema);

            case MAP:
                return visitMap(schema);

            default:
                return primitive(schema);
        }
    }

    default T primitive(Schema primitive) {
        LogicalType logicalType = primitive.getLogicalType();
        if (logicalType != null) {
            switch (logicalType.getName()) {
                case "date":
                case "time-millis":
                    return visitInt();

                case "timestamp-millis":
                    return visitTimestampMillis();

                case "timestamp-micros":
                    return visitTimestampMicros();

                case "decimal":
                    return visitDecimal();

                default:
                    throw new IllegalArgumentException("Unknown logical type: " + logicalType);
            }
        }

        switch (primitive.getType()) {
            case BOOLEAN:
                return visitBoolean();
            case INT:
                return visitInt();
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

    T visitUnion(Schema schema);

    T visitTimestampMillis();

    T visitTimestampMicros();

    T visitDecimal();

    T visitArray(Schema schema);

    T visitMap(Schema schema);

    T visitRecord(Schema schema);
}
