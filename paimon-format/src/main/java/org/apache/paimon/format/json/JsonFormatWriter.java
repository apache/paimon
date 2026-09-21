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
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.format.text.AbstractTextFileWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Json format writer implementation. */
public class JsonFormatWriter extends AbstractTextFileWriter {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private final String lineDelimiter;
    private final JsonOptions.MapNullKeyMode mapNullKeyMode;
    private final String mapNullKeyLiteral;

    public JsonFormatWriter(
            PositionOutputStream outputStream,
            RowType rowType,
            JsonOptions options,
            String compression)
            throws IOException {
        super(outputStream, rowType, compression);
        this.lineDelimiter = options.getLineDelimiter();
        this.mapNullKeyMode = options.getMapNullKeyMode();
        this.mapNullKeyLiteral = options.getMapNullKeyLiteral();
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        try {
            String jsonString = convertRowToJsonString(element, rowType);
            writer.write(jsonString);
            writer.write(lineDelimiter);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to convert row to JSON string", e);
        }
    }

    private String convertRowToJsonString(InternalRow row, RowType rowType)
            throws JsonProcessingException {
        Map<String, Object> result = convertRowToMap(row, rowType);
        return JsonSerdeUtil.writeValueAsString(result);
    }

    private Map<String, Object> convertRowToMap(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        int fieldCount = fields.size();
        Map<String, Object> result = new LinkedHashMap<>(fieldCount); // Pre-allocate capacity

        for (int i = 0; i < fieldCount; i++) {
            DataField field = fields.get(i);
            Object value = InternalRowUtils.get(row, i, field.type());
            result.put(field.name(), convertRowValue(value, field.type()));
        }
        return result;
    }

    private Object convertRowValue(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        switch (dataType.getTypeRoot()) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case CHAR:
            case VARCHAR:
                return value.toString();
            case BINARY:
            case VARBINARY:
                return BASE64_ENCODER.encodeToString((byte[]) value);
            case ARRAY:
                return convertRowArray((InternalArray) value, (ArrayType) dataType);
            case VECTOR:
                return convertRowVector((InternalVector) value, (VectorType) dataType);
            case MAP:
                return convertRowMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRowToMap((InternalRow) value, (RowType) dataType);
            default:
                CastExecutor cast = CastExecutors.resolveToString(dataType);
                return cast.cast(value).toString();
        }
    }

    private List<Object> convertRowArray(InternalArray array, ArrayType arrayType) {
        int size = array.size();
        List<Object> result = new ArrayList<>(size); // Pre-allocate capacity
        DataType elementType = arrayType.getElementType();

        for (int i = 0; i < size; i++) {
            result.add(convertRowValue(InternalRowUtils.get(array, i, elementType), elementType));
        }
        return result;
    }

    private List<Object> convertRowVector(InternalVector vector, VectorType vectorType) {
        if (vector.size() != vectorType.getLength()) {
            throw new IllegalArgumentException(
                    "Size " + vector.size() + " != " + vectorType.getLength() + " in JsonWriter");
        }
        return convertRowArray(vector, DataTypes.ARRAY(vectorType.getElementType()));
    }

    private Map<String, Object> convertRowMap(InternalMap map, MapType mapType) {
        int size = map.size();
        Map<String, Object> result = new LinkedHashMap<>(size); // Pre-allocate capacity
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();

        for (int i = 0; i < size; i++) {
            Object key = InternalRowUtils.get(keyArray, i, keyType);
            String keyString;
            if (key == null) {
                switch (mapNullKeyMode) {
                    case DROP:
                        continue;
                    case LITERAL:
                        validateMapNullKeyLiteral(keyType);
                        keyString = mapNullKeyLiteral;
                        break;
                    case FAIL:
                        throw new IllegalArgumentException(
                                "JSON format does not support null map keys when "
                                        + "'json.map-null-key-mode' is 'FAIL'.");
                    default:
                        throw new IllegalStateException(
                                "Unknown map null key mode: " + mapNullKeyMode);
                }
            } else {
                keyString = convertToString(key, keyType);
            }
            Object value = InternalRowUtils.get(valueArray, i, valueType);
            result.put(keyString, convertRowValue(value, valueType));
        }
        return result;
    }

    private void validateMapNullKeyLiteral(DataType keyType) {
        try {
            Object converted = JsonMapKeyConverter.convert(mapNullKeyLiteral, keyType);
            if (converted == null) {
                throw new IllegalArgumentException("Converted map key is null.");
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot convert 'json.map-null-key-literal' value '%s' to map key type %s.",
                            mapNullKeyLiteral, keyType),
                    e);
        }
    }

    private String convertToString(Object value, DataType dataType) {
        if (value == null) {
            return null;
        }

        DataTypeRoot typeRoot = dataType.getTypeRoot();
        if (typeRoot == DataTypeRoot.CHAR || typeRoot == DataTypeRoot.VARCHAR) {
            return ((BinaryString) value).toString();
        }
        return value.toString();
    }
}
