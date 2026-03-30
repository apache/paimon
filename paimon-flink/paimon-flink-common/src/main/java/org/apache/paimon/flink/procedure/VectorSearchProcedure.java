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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
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
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vector search procedure. This procedure takes one vector and searches for topK nearest vectors.
 * Usage:
 *
 * <pre><code>
 *  CALL sys.vector_search(
 *    `table` => 'tableId',
 *    vector_column => 'v',
 *    query_vector => '1.0,2.0,3.0',
 *    top_k => 5
 *  )
 *
 *  -- with projection and options
 *  CALL sys.vector_search(
 *    `table` => 'tableId',
 *    vector_column => 'v',
 *    query_vector => '1.0,2.0,3.0',
 *    top_k => 5,
 *    projection => 'id,name',
 *    options => 'k1=v1;k2=v2'
 *  )
 * </code></pre>
 */
public class VectorSearchProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "vector_search";

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "vector_column", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "query_vector", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "top_k", type = @DataTypeHint("INT")),
                @ArgumentHint(
                        name = "projection",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            String vectorColumn,
            String queryVectorStr,
            Integer topK,
            String projection,
            String options)
            throws Exception {
        Table table = catalog.getTable(Identifier.fromString(tableId));

        Map<String, String> optionsMap = optionalConfigMap(options);
        if (!optionsMap.isEmpty()) {
            table = table.copy(optionsMap);
        }

        float[] queryVector = parseVector(queryVectorStr);

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVector(queryVector)
                        .withVectorColumn(vectorColumn)
                        .withLimit(topK)
                        .executeLocal();

        RowType tableRowType = table.rowType();
        int[] projectionIndices = parseProjection(projection, tableRowType);

        ReadBuilder readBuilder = table.newReadBuilder();
        if (projectionIndices != null) {
            readBuilder.withProjection(projectionIndices);
        }

        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();

        RowType readType =
                projectionIndices != null
                        ? buildProjectedRowType(tableRowType, projectionIndices)
                        : tableRowType;

        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        try {
                            rows.add(convertRowToJsonString(row, readType));
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to convert row to JSON string", e);
                        }
                    });
        }

        return rows.toArray(new String[0]);
    }

    private static float[] parseVector(String vectorStr) {
        String[] parts = vectorStr.split(",");
        float[] vector = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            vector[i] = Float.parseFloat(parts[i].trim());
        }
        return vector;
    }

    private static int[] parseProjection(String projection, RowType rowType) {
        if (StringUtils.isNullOrWhitespaceOnly(projection)) {
            return null;
        }
        String[] columns = projection.split(",");
        int[] indices = new int[columns.length];
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < columns.length; i++) {
            String col = columns[i].trim();
            int idx = fieldNames.indexOf(col);
            if (idx < 0) {
                throw new IllegalArgumentException(
                        "Column '" + col + "' not found in table schema: " + fieldNames);
            }
            indices[i] = idx;
        }
        return indices;
    }

    private static RowType buildProjectedRowType(RowType rowType, int[] projection) {
        List<DataField> fields = rowType.getFields();
        List<DataField> projected = new ArrayList<>(projection.length);
        for (int idx : projection) {
            projected.add(fields.get(idx));
        }
        return new RowType(projected);
    }

    // --- Row to JSON conversion (reuses the same pattern as JsonFormatWriter) ---

    private static String convertRowToJsonString(InternalRow row, RowType rowType)
            throws Exception {
        Map<String, Object> result = convertRowToMap(row, rowType);
        return JsonSerdeUtil.writeValueAsString(result);
    }

    private static Map<String, Object> convertRowToMap(InternalRow row, RowType rowType) {
        List<DataField> fields = rowType.getFields();
        Map<String, Object> result = new LinkedHashMap<>(fields.size());
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
                return convertArray((InternalArray) value, (ArrayType) dataType);
            case VECTOR:
                return convertVector((InternalVector) value, (VectorType) dataType);
            case MAP:
                return convertMap((InternalMap) value, (MapType) dataType);
            case ROW:
                return convertRowToMap((InternalRow) value, (RowType) dataType);
            default:
                @SuppressWarnings("unchecked")
                CastExecutor<Object, BinaryString> cast =
                        (CastExecutor<Object, BinaryString>)
                                CastExecutors.resolveToString(dataType);
                return cast.cast(value).toString();
        }
    }

    private static List<Object> convertArray(InternalArray array, ArrayType arrayType) {
        DataType elementType = arrayType.getElementType();
        List<Object> result = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            result.add(convertValue(InternalRowUtils.get(array, i, elementType), elementType));
        }
        return result;
    }

    private static List<Object> convertVector(InternalVector vector, VectorType vectorType) {
        return convertArray(vector, DataTypes.ARRAY(vectorType.getElementType()));
    }

    private static Map<String, Object> convertMap(InternalMap map, MapType mapType) {
        DataType keyType = mapType.getKeyType();
        DataType valueType = mapType.getValueType();
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (int i = 0; i < map.size(); i++) {
            Object key = InternalRowUtils.get(keyArray, i, keyType);
            Object val = InternalRowUtils.get(valueArray, i, valueType);
            DataTypeRoot typeRoot = keyType.getTypeRoot();
            String keyStr =
                    (typeRoot == DataTypeRoot.CHAR || typeRoot == DataTypeRoot.VARCHAR)
                            ? ((BinaryString) key).toString()
                            : key.toString();
            result.put(keyStr, convertValue(val, valueType));
        }
        return result;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
