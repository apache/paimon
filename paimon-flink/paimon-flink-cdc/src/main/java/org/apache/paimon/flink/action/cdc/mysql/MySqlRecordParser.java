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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * A parser for MySql Debezium JSON strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class MySqlRecordParser implements FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final List<ComputedColumn> computedColumns;
    private final TypeMapping typeMapping;
    private final boolean isDebeziumSchemaCommentsEnabled;
    private DebeziumEvent root;

    // NOTE: current table name is not converted by tableNameConverter
    private String currentTable;
    private String databaseName;
    private final CdcMetadataConverter[] metadataConverters;

    private final Set<String> nonPkTables = new HashSet<>();

    public MySqlRecordParser(
            Configuration mySqlConfig,
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this.computedColumns = computedColumns;
        this.typeMapping = typeMapping;
        this.metadataConverters = metadataConverters;
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String stringifyServerTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);

        this.isDebeziumSchemaCommentsEnabled =
                mySqlConfig.getBoolean(
                        DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX
                                + RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_COMMENTS.name(),
                        false);
        this.serverTimeZone =
                stringifyServerTimeZone == null
                        ? ZoneId.systemDefault()
                        : ZoneId.of(stringifyServerTimeZone);
    }

    @Override
    public void flatMap(CdcSourceRecord rawEvent, Collector<RichCdcMultiplexRecord> out)
            throws Exception {
        root = objectMapper.readValue((String) rawEvent.getValue(), DebeziumEvent.class);
        currentTable = root.payload().source().get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        databaseName = root.payload().source().get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();

        if (root.payload().isSchemaChange()) {
            extractSchemaChange().forEach(out::collect);
            return;
        }
        extractRecords().forEach(out::collect);
    }

    private List<RichCdcMultiplexRecord> extractSchemaChange() {
        DebeziumEvent.Payload payload = root.payload();
        if (!payload.hasHistoryRecord()) {
            return Collections.emptyList();
        }

        TableChanges.TableChange tableChange = null;
        try {
            Iterator<TableChanges.TableChange> tableChanges = payload.getTableChanges();
            long count;
            for (count = 0L; tableChanges.hasNext(); ++count) {
                tableChange = tableChanges.next();
            }
            if (count != 1) {
                LOG.error(
                        "Invalid historyRecord, because tableChanges should contain exactly 1 item.\n"
                                + payload.historyRecord());
                return Collections.emptyList();
            }
        } catch (Exception e) {
            LOG.error("Failed to parse history record for schema changes", e);
            return Collections.emptyList();
        }

        if (TableChanges.TableChangeType.CREATE == tableChange.getType()
                && tableChange.getTable().primaryKeyColumnNames().isEmpty()) {
            LOG.error(
                    "Didn't find primary keys from MySQL DDL for table '{}'. "
                            + "This table won't be synchronized.",
                    currentTable);
            nonPkTables.add(currentTable);
            return Collections.emptyList();
        }

        Table table = tableChange.getTable();

        List<DataField> fields = extractFields(table);
        List<String> primaryKeys = table.primaryKeyColumnNames();

        // TODO : add table comment and column comment when we upgrade flink cdc to 2.4
        return Collections.singletonList(
                new RichCdcMultiplexRecord(
                        databaseName, currentTable, fields, primaryKeys, CdcRecord.emptyRecord()));
    }

    private List<DataField> extractFields(Table table) {
        RowType.Builder rowType = RowType.builder();
        List<Column> columns = table.columns();

        for (Column column : columns) {
            DataType dataType =
                    MySqlTypeUtils.toDataType(
                            column.typeExpression(),
                            column.length(),
                            column.scale().orElse(null),
                            typeMapping);
            dataType = dataType.copy(typeMapping.containsMode(TO_NULLABLE) || column.isOptional());

            // add column comment when we upgrade flink cdc to 2.4
            if (isDebeziumSchemaCommentsEnabled) {
                rowType.field(column.name(), dataType, column.comment());
            } else {
                rowType.field(column.name(), dataType);
            }
        }
        return rowType.build().getFields();
    }

    private List<RichCdcMultiplexRecord> extractRecords() {
        if (nonPkTables.contains(currentTable)) {
            return Collections.emptyList();
        }
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(root.payload().before());
        if (!before.isEmpty()) {
            records.add(createRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(root.payload().after());
        if (!after.isEmpty()) {
            records.add(createRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        if (JsonSerdeUtil.isNull(recordRow)) {
            return new HashMap<>();
        }

        DebeziumEvent.Field schema =
                Preconditions.checkNotNull(
                        root.schema(),
                        "MySqlRecordParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, DebeziumEvent.Field> fields = schema.beforeAndAfterFields();

        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, DebeziumEvent.Field> field : fields.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue().type();
            JsonNode objectValue = recordRow.get(fieldName);
            if (isNull(objectValue)) {
                continue;
            }

            String className = field.getValue().name();
            String oldValue = objectValue.asText();
            String newValue =
                    DebeziumSchemaUtils.transformRawValue(
                            oldValue,
                            mySqlType,
                            className,
                            typeMapping,
                            objectValue,
                            serverTimeZone);
            resultMap.put(fieldName, newValue);
        }

        // generate values of computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
        }

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            resultMap.put(
                    metadataConverter.columnName(),
                    metadataConverter.read(root.payload().source()));
        }

        return resultMap;
    }

    protected RichCdcMultiplexRecord createRecord(RowKind rowKind, Map<String, String> data) {
        return new RichCdcMultiplexRecord(
                databaseName,
                currentTable,
                Collections.emptyList(),
                Collections.emptyList(),
                new CdcRecord(rowKind, data));
    }
}
