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

package org.apache.paimon.data.serializer;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.types.DataTypeChecks.getFieldTypes;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/** {@link Serializer} of {@link DataType} for internal data structures. */
public final class InternalSerializers {

    /** Creates a {@link Serializer} for internal data structures of the given {@link DataType}. */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> create(DataType type) {
        return (Serializer<T>) createInternal(type);
    }

    /** Creates a {@link Serializer} for internal data structures of the given {@link RowType}. */
    public static InternalRowSerializer create(RowType type) {
        return (InternalRowSerializer) createInternal(type);
    }

    private static Serializer<?> createInternal(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryStringSerializer.INSTANCE;
            case BOOLEAN:
                return BooleanSerializer.INSTANCE;
            case BINARY:
            case VARBINARY:
                return BinarySerializer.INSTANCE;
            case DECIMAL:
                return new DecimalSerializer(getPrecision(type), getScale(type));
            case TINYINT:
                return ByteSerializer.INSTANCE;
            case SMALLINT:
                return ShortSerializer.INSTANCE;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return IntSerializer.INSTANCE;
            case BIGINT:
                return LongSerializer.INSTANCE;
            case FLOAT:
                return FloatSerializer.INSTANCE;
            case DOUBLE:
                return DoubleSerializer.INSTANCE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampSerializer(getPrecision(type));
            case ARRAY:
                return new InternalArraySerializer(((ArrayType) type).getElementType());
            case MULTISET:
                return new InternalMapSerializer(
                        ((MultisetType) type).getElementType(), new IntType(false));
            case MAP:
                MapType mapType = (MapType) type;
                return new InternalMapSerializer(mapType.getKeyType(), mapType.getValueType());
            case ROW:
                return new InternalRowSerializer(getFieldTypes(type).toArray(new DataType[0]));
            case VARIANT:
                return VariantSerializer.INSTANCE;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type '" + type + "' to get internal serializer");
        }
    }

    private InternalSerializers() {
        // no instantiation
    }
}
