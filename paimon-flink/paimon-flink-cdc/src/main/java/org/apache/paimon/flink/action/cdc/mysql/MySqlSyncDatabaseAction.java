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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionBase;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.schemaCompatible;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.tableList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the whole MySQL database into one Paimon database.
 *
 * <p>You should specify MySQL source database in {@code mySqlConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>For each MySQL table to be synchronized, if the corresponding Paimon table does not exist,
 * this action will automatically create the table. Its schema will be derived from all specified
 * MySQL tables. If the Paimon table already exists, its schema will be compared against the schema
 * of all specified MySQL tables.
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
 *
 * <p>This action creates a Paimon table sink for each Paimon table to be written, so this action is
 * not very efficient in resource saving. We may optimize this action by merging all sinks into one
 * instance in the future.
 */
public class MySqlSyncDatabaseAction extends SyncDatabaseActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSyncDatabaseAction.class);

    private boolean ignoreIncompatible = false;

    // for test purpose
    private final List<Identifier> monitoredTables = new ArrayList<>();
    private final List<Identifier> excludedTables = new ArrayList<>();

    public MySqlSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        super(warehouse, database, catalogConfig, mySqlConfig, SyncJobHandler.SourceType.MYSQL);
        this.mode = DIVIDED;
    }

    public MySqlSyncDatabaseAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        JdbcSchemasInfo mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        cdcSourceConfig,
                        tableName ->
                                shouldMonitorTable(tableName, includingPattern, excludingPattern),
                        excludedTables,
                        typeMapping);

        logNonPkTables(mySqlSchemasInfo.nonPkTables());
        List<JdbcTableInfo> jdbcTableInfos = mySqlSchemasInfo.toMySqlTableInfos(mergeShards);

        checkArgument(
                !jdbcTableInfos.isEmpty(),
                "No tables found in MySQL database "
                        + cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        TableNameConverter tableNameConverter =
                new TableNameConverter(
                        caseSensitive, mergeShards, tablePrefix, tableSuffix, tableMapping);
        for (JdbcTableInfo tableInfo : jdbcTableInfos) {
            Identifier identifier =
                    Identifier.create(
                            database,
                            tableNameConverter.convert("", tableInfo.toPaimonTableName()));
            FileStoreTable table;
            Schema fromMySql =
                    CdcActionCommonUtils.buildPaimonSchema(
                            identifier.getFullName(),
                            partitionKeys,
                            primaryKeys,
                            Collections.emptyList(),
                            tableConfig,
                            tableInfo.schema(),
                            metadataConverters,
                            caseSensitive,
                            false,
                            true);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), tableInfo, identifier);
                if (shouldMonitorTable(table.schema(), fromMySql, errMsg)) {
                    table = alterTableOptions(identifier, table);
                    tables.add(table);
                    monitoredTables.addAll(tableInfo.identifiers());
                } else {
                    excludedTables.addAll(tableInfo.identifiers());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                tables.add(table);
                monitoredTables.addAll(tableInfo.identifiers());
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return MySqlActionUtils.createCdcTimestampExtractor();
    }

    @Override
    protected MySqlSource<CdcSourceRecord> buildSource() {
        validateRuntimeExecutionMode();
        return MySqlActionUtils.buildMySqlSource(
                cdcSourceConfig,
                tableList(
                        mode,
                        cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME),
                        includingTables,
                        monitoredTables,
                        excludedTables),
                typeMapping);
    }

    private void logNonPkTables(List<Identifier> nonPkTables) {
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. "
                            + "These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(Identifier::getFullName)
                            .collect(Collectors.joining(",")));
            excludedTables.addAll(nonPkTables);
        }
    }

    private boolean shouldMonitorTable(
            String mySqlTableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", mySqlTableName);
        }
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema mySqlSchema, Supplier<String> errMsg) {
        if (schemaCompatible(tableSchema, mySqlSchema.fields())) {
            return true;
        } else if (ignoreIncompatible) {
            LOG.warn(errMsg.get() + "This table will be ignored.");
            return false;
        } else {
            throw new IllegalArgumentException(
                    errMsg.get()
                            + "If you want to ignore the incompatible tables, please specify --ignore-incompatible to true.");
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, JdbcTableInfo jdbcTableInfo, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        jdbcTableInfo.location(),
                        jdbcTableInfo.schema().fields());
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables() {
        return monitoredTables;
    }

    @VisibleForTesting
    public List<Identifier> excludedTables() {
        return excludedTables;
    }

    @Override
    protected boolean requirePrimaryKeys() {
        return true;
    }
}
