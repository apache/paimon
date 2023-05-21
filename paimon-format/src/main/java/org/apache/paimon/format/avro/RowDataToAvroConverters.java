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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.avro.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from {@link InternalRow} to Avro {@link GenericRecord}. */
public class RowDataToAvroConverters {

    // --------------------------------------------------------------------------------
    // Runtime Converters
    // --------------------------------------------------------------------------------

    /**
     * Runtime converter that converts objects of Paimon internal data structures to corresponding
     * Avro data structures.
     */
    @FunctionalInterface
    public interface RowDataToAvroConverter extends Serializable {
        Object convert(Schema schema, Object object);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Avro for
    // sql-client uber jars.
    // --------------------------------------------------------------------------------

    /**
     * Creates a runtime converter according to the given logical type that converts objects of
     * Paimon internal data structures to corresponding Avro data structures.
     */
    public static RowDataToAvroConverter createConverter(DataType type) {
        final RowDataToAvroConverter converter;
        switch (type.getTypeRoot()) {
            case TINYINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Byte) object).intValue();
                            }
                        };
                break;
            case SMALLINT:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Short) object).intValue();
                            }
                        };
                break;
            case BOOLEAN: // boolean
            case INTEGER: // int
            case BIGINT: // long
            case FLOAT: // float
            case DOUBLE: // double
            case TIME_WITHOUT_TIME_ZONE: // int
            case DATE: // int
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return object;
                            }
                        };
                break;
            case CHAR:
            case VARCHAR:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return new Utf8(object.toString());
                            }
                        };
                break;
            case BINARY:
            case VARBINARY:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap((byte[]) object);
                            }
                        };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ((Timestamp) object).toInstant().toEpochMilli();
                            }
                        };
                break;
            case DECIMAL:
                converter =
                        new RowDataToAvroConverter() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Object convert(Schema schema, Object object) {
                                return ByteBuffer.wrap(((Decimal) object).toUnscaledBytes());
                            }
                        };
                break;
            case ARRAY:
                converter = createArrayConverter((ArrayType) type);
                break;
            case ROW:
                converter = createRowConverter((RowType) type);
                break;
            case MAP:
            case MULTISET:
                converter = createMapConverter(type);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        // wrap into nullable converter
        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                if (object == null) {
                    return null;
                }

                // get actual schema if it is a nullable schema
                Schema actualSchema;
                if (schema.getType() == Schema.Type.UNION) {
                    List<Schema> types = schema.getTypes();
                    int size = types.size();
                    if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(0);
                    } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                        actualSchema = types.get(1);
                    } else {
                        throw new IllegalArgumentException(
                                "The Avro schema is not a nullable type: " + schema);
                    }
                } else {
                    actualSchema = schema;
                }
                return converter.convert(actualSchema, object);
            }
        };
    }

    private static RowDataToAvroConverter createRowConverter(RowType rowType) {
        final RowDataToAvroConverter[] fieldConverters =
                rowType.getFieldTypes().stream()
                        .map(RowDataToAvroConverters::createConverter)
                        .toArray(RowDataToAvroConverter[]::new);
        final DataType[] fieldTypes =
                rowType.getFields().stream().map(DataField::type).toArray(DataType[]::new);
        final InternalRow.FieldGetter[] fieldGetters =
                new InternalRow.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(fieldTypes[i], i);
        }
        final int length = rowType.getFieldCount();

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final InternalRow row = (InternalRow) object;
                final List<Schema.Field> fields = schema.getFields();
                final GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < length; ++i) {
                    final Schema.Field schemaField = fields.get(i);
                    try {
                        Object avroObject =
                                fieldConverters[i].convert(
                                        schemaField.schema(), fieldGetters[i].getFieldOrNull(row));
                        record.put(i, avroObject);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                String.format(
                                        "Fail to serialize at field: %s.", schemaField.name()),
                                t);
                    }
                }
                return record;
            }
        };
    }

    private static RowDataToAvroConverter createArrayConverter(ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        final InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(elementType);
        final RowDataToAvroConverter elementConverter = createConverter(arrayType.getElementType());

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema elementSchema = schema.getElementType();
                InternalArray arrayData = (InternalArray) object;
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); ++i) {
                    list.add(
                            elementConverter.convert(
                                    elementSchema, elementGetter.getElementOrNull(arrayData, i)));
                }
                return list;
            }
        };
    }

    private static RowDataToAvroConverter createMapConverter(DataType type) {
        DataType valueType = extractValueTypeToAvroMap(type);
        final InternalArray.ElementGetter valueGetter =
                InternalArray.createElementGetter(valueType);
        final RowDataToAvroConverter valueConverter = createConverter(valueType);

        return new RowDataToAvroConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Schema schema, Object object) {
                final Schema valueSchema = schema.getValueType();
                final InternalMap mapData = (InternalMap) object;
                final InternalArray keyArray = mapData.keyArray();
                final InternalArray valueArray = mapData.valueArray();
                final Map<Object, Object> map = new HashMap<>(mapData.size());
                for (int i = 0; i < mapData.size(); ++i) {
                    final String key = keyArray.getString(i).toString();
                    final Object value =
                            valueConverter.convert(
                                    valueSchema, valueGetter.getElementOrNull(valueArray, i));
                    map.put(key, value);
                }
                return map;
            }
        };
    }
}
