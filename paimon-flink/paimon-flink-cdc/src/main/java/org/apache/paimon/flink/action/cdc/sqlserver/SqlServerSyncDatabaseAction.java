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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionBase;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.schemaCompatible;

/** An {@link Action} which synchronize the whole SqlServer database into one Paimon database. */
public class SqlServerSyncDatabaseAction extends SyncDatabaseActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSyncDatabaseAction.class);
    private boolean ignoreIncompatible = false;
    private final List<Pair<Identifier, String>> monitoredTables = new ArrayList<>();
    private final List<Pair<Identifier, String>> excludedTables = new ArrayList<>();

    public SqlServerSyncDatabaseAction(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            Map<String, String> sqlServerConfig) {
        super(
                warehouse,
                database,
                catalogConfig,
                sqlServerConfig,
                SyncJobHandler.SourceType.SQLSERVER);
        this.mode = DIVIDED;
    }

    public SqlServerSyncDatabaseAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        JdbcSchemasInfo jdbcSchemasInfo =
                SqlServerActionUtils.getSqlServerTableInfos(
                        cdcSourceConfig,
                        tableName ->
                                shouldMonitorTable(tableName, includingPattern, excludingPattern),
                        excludedTables,
                        typeMapping);

        logNonPkTables(jdbcSchemasInfo);

        Map<String, Set<String>> schemaMappings = getSchemaMapping(jdbcSchemasInfo);

        List<JdbcTableInfo> sqlServerTableInfos = jdbcSchemasInfo.toTableInfos(mergeShards);

        TableNameConverter tableNameConverter =
                new TableNameConverter(caseSensitive, mergeShards, tablePrefix, tableSuffix);
        for (JdbcTableInfo tableInfo : sqlServerTableInfos) {
            Identifier identifier =
                    Identifier.create(
                            database, tableNameConverter.convert(tableInfo.toPaimonTableName()));
            FileStoreTable table;
            Schema fromSqlServer =
                    CdcActionCommonUtils.buildPaimonSchema(
                            identifier.getFullName(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            tableConfig,
                            tableInfo.schema(),
                            metadataConverters,
                            caseSensitive,
                            true);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                table = alterTableOptions(identifier, table);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), tableInfo, identifier);

                if (shouldMonitorTable(table.schema(), fromSqlServer, errMsg)) {
                    tables.add(table);
                    setTables(schemaMappings, tableInfo.identifiers(), monitoredTables);
                } else {
                    setTables(schemaMappings, tableInfo.identifiers(), excludedTables);
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromSqlServer, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                tables.add(table);
                setTables(schemaMappings, tableInfo.identifiers(), monitoredTables);
            }
        }
    }

    private void setTables(
            Map<String, Set<String>> schemaMappings,
            List<Identifier> identifiers,
            List<Pair<Identifier, String>> tables) {
        identifiers.stream()
                .forEach(
                        item -> {
                            Set<String> schemas = schemaMappings.get(item.getFullName());
                            schemas.stream()
                                    .forEach(
                                            schemaName -> {
                                                tables.add(Pair.of(item, schemaName));
                                            });
                        });
    }

    private static Map<String, Set<String>> getSchemaMapping(JdbcSchemasInfo jdbcSchemasInfo) {
        Map<String, Set<String>> schemaMapping = new HashMap<>();
        List<JdbcSchemasInfo.JdbcSchemaInfo> jdbcSchemaInfos = jdbcSchemasInfo.schemaInfos();
        for (JdbcSchemasInfo.JdbcSchemaInfo jdbcSchemaInfo : jdbcSchemaInfos) {
            if (!jdbcSchemaInfo.isPkTable()) {
                continue;
            }
            String fullName = jdbcSchemaInfo.identifier().getFullName();
            if (!schemaMapping.containsKey(fullName)) {
                Set<String> schemaNames = new HashSet<>();
                schemaNames.add(jdbcSchemaInfo.schemaName());
                schemaMapping.put(fullName, schemaNames);
            } else {
                Set<String> existsSchemas = schemaMapping.get(fullName);
                existsSchemas.add(jdbcSchemaInfo.schemaName());
            }
        }
        return schemaMapping;
    }

    @Override
    protected Object buildSource() {
        try {
            return SqlServerActionUtils.buildSqlServerSource(
                    cdcSourceConfig,
                    SqlServerActionUtils.tableList(
                            mode,
                            cdcSourceConfig.get(SqlServerSourceOptions.SCHEMA_NAME),
                            includingTables,
                            monitoredTables,
                            excludedTables));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, JdbcTableInfo tableInfo, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "SqlServer table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        tableInfo.location(),
                        tableInfo.schema().fields());
    }

    private void logNonPkTables(JdbcSchemasInfo jdbcSchemasInfo) {
        List<Identifier> nonPkTables = jdbcSchemasInfo.nonPkTables();
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. "
                            + "These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(Identifier::getFullName)
                            .collect(Collectors.joining(",")));
            jdbcSchemasInfo.schemaInfos().stream()
                    .forEach(
                            jdbcSchemaInfo -> {
                                if (!jdbcSchemaInfo.isPkTable()) {
                                    excludedTables.add(
                                            Pair.of(
                                                    jdbcSchemaInfo.identifier(),
                                                    jdbcSchemaInfo.schemaName()));
                                }
                            });
        }
    }

    private boolean shouldMonitorTable(
            String tableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(tableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(tableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", tableName);
        }
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema sqlServerSchema, Supplier<String> errMsg) {
        if (schemaCompatible(tableSchema, sqlServerSchema.fields())) {
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
}
