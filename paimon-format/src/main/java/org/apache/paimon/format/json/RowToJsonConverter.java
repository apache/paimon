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

package org.apache.paimon.format.json;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converter to convert Paimon internal rows to JSON objects. */
public class RowToJsonConverter {

    private final RowType rowType;
    private final Options options;
    private final boolean timestampFormatStandard;

    public RowToJsonConverter(RowType rowType, Options options) {
        this.rowType = rowType;
        this.options = options;
        this.timestampFormatStandard = options.get(JsonFileFormat.JSON_TIMESTAMP_FORMAT_STANDARD);
    }

    public Object convert(InternalRow row) {
        return convertRow(row, rowType);
    }

    public Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return value;
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value).toString();
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case DECIMAL:
                return ((Decimal) value).toBigDecimal().toString();
            case DATE:
                int daysSinceEpoch = (Integer) value;
                return LocalDate.ofEpochDay(daysSinceEpoch).toString();
            case TIME_WITHOUT_TIME_ZONE:
                int millisOfDay = (Integer) value;
                return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L).toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp timestamp = (Timestamp) value;
                if (timestampFormatStandard) {
                    return timestamp
                            .toLocalDateTime()
                            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                } else {
                    return timestamp.getMillisecond();
                }
            case ARRAY:
                return convertArray((InternalArray) value, (ArrayType) dataType);
            case MAP:
                return convertMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRow((InternalRow) value, (RowType) dataType);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + dataType);
        }
    }

    private List<Object> convertArray(InternalArray array, ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        List<Object> result = new ArrayList<>();

        for (int i = 0; i < array.size(); i++) {
            Object element = InternalRowUtils.get(array, i, elementType);
            result.add(convertValue(element, elementType));
        }

        return result;
    }

    private Map<String, Object> convertMap(InternalMap map, MapType mapType) {
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        Map<String, Object> result = new LinkedHashMap<>();

        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();

        for (int i = 0; i < map.size(); i++) {
            Object key = InternalRowUtils.get(keyArray, i, keyType);
            Object value = InternalRowUtils.get(valueArray, i, valueType);

            String keyStr = convertToString(key, keyType);
            Object convertedValue = convertValue(value, valueType);
            result.put(keyStr, convertedValue);
        }

        return result;
    }

    private String convertToString(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return ((BinaryString) value).toString();
            default:
                return value.toString();
        }
    }

    public Map<String, Object> convertRow(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        Map<String, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            Object value = InternalRowUtils.get(row, i, field.type());
            result.put(field.name(), convertValue(value, field.type()));
        }

        return result;
    }
}
