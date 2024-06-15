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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.avro.AvroSchemaConverter.extractValueTypeToAvroMap;

/** Tool class used to convert from Avro {@link GenericRecord} to {@link InternalRow}. * */
public class AvroToRowDataConverters {

    /**
     * Runtime converter that converts Avro data structures into objects of Paimon internal data
     * structures.
     */
    @FunctionalInterface
    public interface AvroToRowDataConverter extends Serializable {
        Object convert(Object object);
    }

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    public static AvroToRowDataConverter createRowConverter(RowType rowType) {
        final AvroToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(DataField::type)
                        .map(AvroToRowDataConverters::createNullableConverter)
                        .toArray(AvroToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();

        return avroObject -> {
            IndexedRecord record = (IndexedRecord) avroObject;
            GenericRow row = new GenericRow(arity);
            for (int i = 0; i < arity; ++i) {
                // avro always deserialize successfully even though the type isn't matched
                // so no need to throw exception about which field can't be deserialized
                row.setField(i, fieldConverters[i].convert(record.get(i)));
            }
            return row;
        };
    }

    /** Creates a runtime converter which is null safe. */
    private static AvroToRowDataConverter createNullableConverter(DataType type) {
        final AvroToRowDataConverter converter = createConverter(type);
        return avroObject -> {
            if (avroObject == null) {
                return null;
            }
            return converter.convert(avroObject);
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    @VisibleForTesting
    static AvroToRowDataConverter createConverter(DataType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return avroObject -> ((Integer) avroObject).byteValue();
            case SMALLINT:
                return avroObject -> ((Integer) avroObject).shortValue();
            case BOOLEAN: // boolean
            case INTEGER: // int
            case BIGINT: // long
            case FLOAT: // float
            case DOUBLE: // double
                return avroObject -> avroObject;
            case DATE:
                return AvroToRowDataConverters::convertToDate;
            case TIME_WITHOUT_TIME_ZONE:
                return AvroToRowDataConverters::convertToTime;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = ((TimestampType) type).getPrecision();
                if (precision <= 3) {
                    return AvroToRowDataConverters::convertToTimestampFromMillis;
                } else if (precision <= 6) {
                    return AvroToRowDataConverters::convertToTimestampFromMicros;
                } else {
                    throw new UnsupportedOperationException("Unsupported precision: " + precision);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                precision = ((LocalZonedTimestampType) type).getPrecision();
                if (precision <= 3) {
                    return AvroToRowDataConverters::convertToTimestampFromMillis;
                } else if (precision <= 6) {
                    return AvroToRowDataConverters::convertToTimestampFromMicros;
                } else {
                    throw new UnsupportedOperationException("Unsupported precision: " + precision);
                }
            case CHAR:
            case VARCHAR:
                return avroObject -> BinaryString.fromString(avroObject.toString());
            case BINARY:
            case VARBINARY:
                return AvroToRowDataConverters::convertToBytes;
            case DECIMAL:
                return createDecimalConverter((DecimalType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case ROW:
                return createRowConverter((RowType) type);
            case MAP:
            case MULTISET:
                return createMapConverter(type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static AvroToRowDataConverter createDecimalConverter(DecimalType decimalType) {
        final int precision = decimalType.getPrecision();
        final int scale = decimalType.getScale();
        return avroObject -> {
            final byte[] bytes;
            if (avroObject instanceof GenericFixed) {
                bytes = ((GenericFixed) avroObject).bytes();
            } else if (avroObject instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) avroObject;
                bytes = new byte[byteBuffer.remaining()];
                byteBuffer.get(bytes);
            } else {
                bytes = (byte[]) avroObject;
            }
            return Decimal.fromUnscaledBytes(bytes, precision, scale);
        };
    }

    private static AvroToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final AvroToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());
        final Class<?> elementClass = InternalRow.getDataClass(arrayType.getElementType());

        return avroObject -> {
            final List<?> list = (List<?>) avroObject;
            final int length = list.size();
            final Object[] array = (Object[]) Array.newInstance(elementClass, length);
            for (int i = 0; i < length; ++i) {
                array[i] = elementConverter.convert(list.get(i));
            }
            return new GenericArray(array);
        };
    }

    private static AvroToRowDataConverter createMapConverter(DataType type) {
        final AvroToRowDataConverter keyConverter = createConverter(DataTypes.STRING());
        final AvroToRowDataConverter valueConverter =
                createNullableConverter(extractValueTypeToAvroMap(type));

        return avroObject -> {
            final Map<?, ?> map = (Map<?, ?>) avroObject;
            Map<Object, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = keyConverter.convert(entry.getKey());
                Object value = valueConverter.convert(entry.getValue());
                result.put(key, value);
            }
            return new GenericMap(result);
        };
    }

    private static Timestamp convertToTimestampFromMillis(Object object) {
        return convertToTimestamp(object, 3);
    }

    private static Timestamp convertToTimestampFromMicros(Object object) {
        return convertToTimestamp(object, 6);
    }

    @VisibleForTesting
    static Timestamp convertToTimestamp(Object object, int precision) {
        if (object instanceof Long) {
            if (precision <= 3) {
                return Timestamp.fromEpochMillis((Long) object);
            } else if (precision <= 6) {
                return Timestamp.fromMicros((Long) object);
            } else {
                throw new UnsupportedOperationException("Unsupported precision: " + precision);
            }
        } else if (object instanceof Instant) {
            return Timestamp.fromInstant((Instant) object);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return Timestamp.fromEpochMillis(jodaConverter.convertTimestamp(object));
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIMESTAMP logical type. Received: " + object);
            }
        }
    }

    private static int convertToDate(Object object) {
        if (object instanceof Integer) {
            return (Integer) object;
        } else if (object instanceof LocalDate) {
            return (int) ((LocalDate) object).toEpochDay();
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                return (int) jodaConverter.convertDate(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for DATE logical type. Received: " + object);
            }
        }
    }

    private static int convertToTime(Object object) {
        final int millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else if (object instanceof LocalTime) {
            millis = ((LocalTime) object).get(ChronoField.MILLI_OF_DAY);
        } else {
            JodaConverter jodaConverter = JodaConverter.getConverter();
            if (jodaConverter != null) {
                millis = jodaConverter.convertTime(object);
            } else {
                throw new IllegalArgumentException(
                        "Unexpected object type for TIME logical type. Received: " + object);
            }
        }
        return millis;
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            return (byte[]) object;
        }
    }
}
