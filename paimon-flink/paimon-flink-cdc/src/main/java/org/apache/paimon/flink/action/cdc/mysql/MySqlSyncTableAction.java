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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlSchemasInfo;
import org.apache.paimon.flink.action.cdc.mysql.schema.MySqlTableInfo;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple MySQL tables into one Paimon table.
 *
 * <p>You should specify MySQL source table in {@code mySqlConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified MySQL tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified MySQL tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 */
public class MySqlSyncTableAction extends SyncTableActionBase {

    private MySqlSchemasInfo mySqlSchemasInfo;

    public MySqlSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        super(warehouse, database, table, catalogConfig, mySqlConfig);
        MySqlActionUtils.registerJdbcDriver();
    }

    @Override
    protected Optional<CdcMetadataConverter<?>> metadataConverter(String column) {
        return Optional.of(MySqlMetadataProcessor.converter(column));
    }

    @Override
    protected void checkCdcSourceArgument() {
        checkArgument(
                cdcSourceConfig.contains(MySqlSourceOptions.TABLE_NAME),
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.TABLE_NAME.key()));
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        this.mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        cdcSourceConfig,
                        monitorTablePredication(),
                        new ArrayList<>(),
                        typeMapping,
                        catalog.caseSensitive());
        validateMySqlTableInfos(mySqlSchemasInfo);
        MySqlTableInfo tableInfo = mySqlSchemasInfo.mergeAll();
        return tableInfo.schema();
    }

    @Override
    protected DataStreamSource<String> buildSource() throws Exception {
        String tableList =
                mySqlSchemasInfo.pkTables().stream()
                        .map(i -> i.getDatabaseName() + "\\." + i.getObjectName())
                        .collect(Collectors.joining("|"));
        return buildDataStreamSource(MySqlActionUtils.buildMySqlSource(cdcSourceConfig, tableList));
    }

    @Override
    protected String sourceName() {
        return "MySQL Source";
    }

    @Override
    protected FlatMapFunction<String, RichCdcMultiplexRecord> recordParse() {
        boolean caseSensitive = catalog.caseSensitive();
        return new MySqlRecordParser(
                cdcSourceConfig, caseSensitive, computedColumns, typeMapping, metadataConverters);
    }

    @Override
    protected String jobName() {
        return String.format("MySQL-Paimon Table Sync: %s.%s", database, table);
    }

    private void validateMySqlTableInfos(MySqlSchemasInfo mySqlSchemasInfo) {
        List<Identifier> nonPkTables = mySqlSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "Source tables of MySQL table synchronization job cannot contain table "
                        + "which doesn't have primary keys.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !mySqlSchemasInfo.pkTables().isEmpty(),
                "No table satisfies the given database name and table name.");
    }

    private Predicate<String> monitorTablePredication() {
        return tableName -> {
            Pattern tableNamePattern =
                    Pattern.compile(cdcSourceConfig.get(MySqlSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(tableName).matches();
        };
    }
}
