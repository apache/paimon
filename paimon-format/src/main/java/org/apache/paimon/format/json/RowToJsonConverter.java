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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converter to convert Paimon internal rows to JSON objects. */
public class RowToJsonConverter {

    public static String convertRow2String(InternalRow row, RowType rowType)
            throws JsonProcessingException {
        Map<String, Object> result = convertRow(row, rowType);
        return JsonSerdeUtil.writeValueAsString(result);
    }

    private static Map<String, Object> convertRow(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        Map<String, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            Object value = InternalRowUtils.get(row, i, field.type());
            result.put(field.name(), convertValue(value, field.type()));
        }
        return result;
    }

    private static Object convertValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        DataTypeRoot typeRoot = dataType.getTypeRoot();
        switch (typeRoot) {
            case BINARY:
            case VARBINARY:
                return Base64.getEncoder().encodeToString((byte[]) value);
            case ARRAY:
                return convertArray((InternalArray) value, (ArrayType) dataType);
            case MAP:
                return convertMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRow((InternalRow) value, (RowType) dataType);
            default:
                CastExecutor cast = CastExecutors.resolveToString(dataType);
                return cast.cast(value).toString();
        }
    }

    private static List<Object> convertArray(InternalArray array, ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        List<Object> result = new ArrayList<>();

        for (int i = 0; i < array.size(); i++) {
            Object element = InternalRowUtils.get(array, i, elementType);
            result.add(convertValue(element, elementType));
        }

        return result;
    }

    private static Map<String, Object> convertMap(InternalMap map, MapType mapType) {
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

    private static String convertToString(Object value, DataType dataType) {
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
}
