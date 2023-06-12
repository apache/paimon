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

package org.apache.paimon.flink.action.cdc.kafka.canal;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.NullNode;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link EventParser} for Canal-json. */
public class CanalJsonEventParser implements EventParser<String> {

    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    private static final String TYPE = "type";
    private static final String MYSQL_TYPE = "mysqlType";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private JsonNode root;

    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    private final List<ComputedColumn> computedColumns;

    public CanalJsonEventParser(boolean caseSensitive, List<ComputedColumn> computedColumns) {
        this(caseSensitive, new TableNameConverter(caseSensitive), computedColumns);
    }

    public CanalJsonEventParser(boolean caseSensitive, TableNameConverter tableNameConverter) {
        this(caseSensitive, tableNameConverter, Collections.emptyList());
    }

    public CanalJsonEventParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        this.caseSensitive = caseSensitive;
        this.tableNameConverter = tableNameConverter;
        this.computedColumns = computedColumns;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            root = objectMapper.readValue(rawEvent, JsonNode.class);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String parseTableName() {
        String tableName = root.get("table").asText();
        return tableNameConverter.convert(tableName);
    }

    private boolean isSchemaChange() {
        if (root.get("isDdl") == null) {
            return false;
        } else {
            return "true".equals(root.get("isDdl").asText());
        }
    }

    @Override
    public List<DataField> parseSchemaChange() {
        if (!isSchemaChange()) {
            return Collections.emptyList();
        }

        String sql = root.get("sql").asText();

        if (StringUtils.isEmpty(sql)) {
            return Collections.emptyList();
        }
        List<DataField> result = new ArrayList<>();
        int id = 0;
        SQLStatement sqlStatement = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        if (sqlStatement instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement sqlAlterTableStatement = (SQLAlterTableStatement) sqlStatement;
            for (int i = 0; i < sqlAlterTableStatement.getItems().size(); i++) {
                SQLAlterTableItem sqlAlterTableItem = sqlAlterTableStatement.getItems().get(i);
                if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                    SQLAlterTableAddColumn sqlAlterTableAddColumn =
                            (SQLAlterTableAddColumn) sqlAlterTableItem;
                    List<SQLColumnDefinition> columns = sqlAlterTableAddColumn.getColumns();
                    for (SQLColumnDefinition column : columns) {
                        String columnName = column.getColumnName().replace("`", "");
                        String columnType = column.getDataType().toString();
                        DataType dataType = MySqlTypeUtils.toDataType(columnType);
                        boolean notNull = column.toString().toUpperCase().contains("NOT NULL");
                        dataType = notNull ? dataType.notNull() : dataType.nullable();
                        result =
                                result.stream()
                                        .filter(dataField -> !dataField.name().equals(columnName))
                                        .collect(Collectors.toList());
                        result.add(
                                new DataField(
                                        id++,
                                        caseSensitive ? columnName : columnName.toLowerCase(),
                                        dataType));
                    }
                } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                    // ignore
                } else if (sqlAlterTableItem instanceof MySqlAlterTableModifyColumn) {
                    MySqlAlterTableModifyColumn mySqlAlterTableModifyColumn =
                            (MySqlAlterTableModifyColumn) sqlAlterTableItem;
                    SQLColumnDefinition newColumnDefinition =
                            mySqlAlterTableModifyColumn.getNewColumnDefinition();
                    String columnName = newColumnDefinition.getColumnName().replace("`", "");
                    String columnType = newColumnDefinition.getDataType().toString();
                    DataType dataType = MySqlTypeUtils.toDataType(columnType);
                    boolean notNull =
                            newColumnDefinition.toString().toUpperCase().contains("NOT NULL");
                    dataType = notNull ? dataType.notNull() : dataType.nullable();
                    result.add(
                            new DataField(
                                    id++,
                                    caseSensitive ? columnName : columnName.toLowerCase(),
                                    dataType));

                } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                    MySqlAlterTableChangeColumn mySqlAlterTableChangeColumn =
                            (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                    SQLColumnDefinition newColumnDefinition =
                            mySqlAlterTableChangeColumn.getNewColumnDefinition();
                    String oldColumnName =
                            mySqlAlterTableChangeColumn
                                    .getColumnName()
                                    .getSimpleName()
                                    .replace("`", "");
                    String columnName = newColumnDefinition.getColumnName().replace("`", "");
                    String columnType = newColumnDefinition.getDataType().toString();
                    DataType dataType = MySqlTypeUtils.toDataType(columnType);
                    boolean notNull =
                            newColumnDefinition.toString().toUpperCase().contains("NOT NULL");
                    dataType = notNull ? dataType.notNull() : dataType.nullable();
                    result =
                            result.stream()
                                    .filter(dataField -> !dataField.name().equals(oldColumnName))
                                    .collect(Collectors.toList());
                    result.add(
                            new DataField(
                                    id++,
                                    caseSensitive ? columnName : columnName.toLowerCase(),
                                    dataType));
                }
            }
        }

        return result;
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (isSchemaChange()) {
            return Collections.emptyList();
        }
        Preconditions.checkNotNull(
                root.get(TYPE),
                "CanalJsonEventParser only supports canal-json format,"
                        + "please make sure that your topic's format is accurate.");
        List<CdcRecord> records = new ArrayList<>();
        String type = root.get(TYPE).asText();
        if (OP_UPDATE.equals(type)) {
            ArrayNode data = (ArrayNode) root.get(FIELD_DATA);
            ArrayNode old =
                    root.get(FIELD_OLD) instanceof NullNode
                            ? null
                            : (ArrayNode) root.get(FIELD_OLD);
            for (int i = 0; i < data.size(); i++) {
                Map<String, String> after = extractRow(data.get(i));
                if (old != null) {
                    Map<String, String> before = extractRow(old.get(i));
                    // fields in "old" (before) means the fields are changed
                    // fields not in "old" (before) means the fields are not changed
                    // so we just copy the not changed fields into before
                    for (Map.Entry<String, String> entry : after.entrySet()) {
                        if (!before.containsKey(entry.getKey())) {
                            before.put(entry.getKey(), entry.getValue());
                        }
                    }
                    before = caseSensitive ? before : keyCaseInsensitive(before);
                    records.add(new CdcRecord(RowKind.DELETE, before));
                }
                after = caseSensitive ? after : keyCaseInsensitive(after);
                records.add(new CdcRecord(RowKind.INSERT, after));
            }
        } else if (OP_INSERT.equals(type)) {
            ArrayNode data = (ArrayNode) root.get(FIELD_DATA);
            for (int i = 0; i < data.size(); i++) {
                Map<String, String> after = extractRow(data.get(i));
                after = caseSensitive ? after : keyCaseInsensitive(after);
                records.add(new CdcRecord(RowKind.INSERT, after));
            }
        } else if (OP_DELETE.equals(type)) {
            ArrayNode data = (ArrayNode) root.get(FIELD_DATA);
            for (int i = 0; i < data.size(); i++) {
                Map<String, String> after = extractRow(data.get(i));
                after = caseSensitive ? after : keyCaseInsensitive(after);
                records.add(new CdcRecord(RowKind.DELETE, after));
            }
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        Map<String, String> mySqlFieldTypes = new HashMap<>();
        Preconditions.checkNotNull(
                root.get(MYSQL_TYPE),
                "CanalJsonEventParser only supports canal-json format,"
                        + "please make sure that your topic's format is accurate.");
        JsonNode schema = root.get(MYSQL_TYPE);
        Iterator<String> iterator = schema.fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            String fieldType = schema.get(fieldName).asText();
            mySqlFieldTypes.put(fieldName, fieldType);
        }

        Map<String, Object> jsonMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }
        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : mySqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue();
            Object objectValue = jsonMap.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String oldValue = objectValue.toString();
            String newValue = oldValue;
            if (MySqlTypeUtils.isSetType(MySqlTypeUtils.getShortType(mySqlType))) {
                newValue = CanalFieldParser.convertSet(newValue, mySqlType);
            } else if (MySqlTypeUtils.isEnumType(MySqlTypeUtils.getShortType(mySqlType))) {
                newValue = CanalFieldParser.convertEnum(newValue, mySqlType);
            } else if (MySqlTypeUtils.isGeoType(MySqlTypeUtils.getShortType(mySqlType))) {
                try {
                    byte[] wkb =
                            CanalFieldParser.convertGeoType2WkbArray(
                                    oldValue.getBytes(StandardCharsets.ISO_8859_1));
                    newValue = MySqlTypeUtils.convertWkbArray(wkb);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Failed to convert %s to geometry JSON.", oldValue), e);
                }
            }
            resultMap.put(fieldName, newValue);
        }
        // generate values of computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
        }

        return resultMap;
    }

    private Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
        Map<String, String> keyCaseInsensitive = new HashMap<>();
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String fieldName = entry.getKey().toLowerCase();
            checkArgument(
                    !keyCaseInsensitive.containsKey(fieldName),
                    "Duplicate key appears when converting map keys to case-insensitive form. Original map is:\n%s",
                    origin);
            keyCaseInsensitive.put(fieldName, entry.getValue());
        }
        return keyCaseInsensitive;
    }
}
