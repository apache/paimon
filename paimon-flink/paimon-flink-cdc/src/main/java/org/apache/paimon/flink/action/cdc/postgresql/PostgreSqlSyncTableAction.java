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

package org.apache.paimon.flink.action.cdc.postgresql;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.sink.cdc.CdcSinkBuilder;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An {@link Action} which synchronize one or multiple PostgreSQL tables into one Paimon table. */
public class PostgreSqlSyncTableAction extends ActionBase {

    private final Configuration postgreSqlConfig;
    private final String database;
    private final String table;
    private List<String> partitionKeys;
    private List<String> primaryKeys;
    private List<String> computedColumnArgs;
    private Map<String, String> tableConfig;

    public PostgreSqlSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> postgreSqlConfig) {
        super(warehouse, catalogConfig);
        this.postgreSqlConfig = Configuration.fromMap(postgreSqlConfig);
        this.database = database;
        this.table = table;
    }

    public PostgreSqlSyncTableAction withPartitionKeys(String... partitionKeys) {
        return withPartitionKeys(Arrays.asList(partitionKeys));
    }

    public PostgreSqlSyncTableAction withPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public PostgreSqlSyncTableAction withPrimaryKeys(String... primaryKeys) {
        return withPrimaryKeys(Arrays.asList(primaryKeys));
    }

    public PostgreSqlSyncTableAction withPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
        return this;
    }

    public PostgreSqlSyncTableAction withComputedColumnArgs(List<String> computedColumnArgs) {
        this.computedColumnArgs = computedColumnArgs;
        return this;
    }

    public PostgreSqlSyncTableAction withTableConfig(Map<String, String> tableConfig) {
        this.tableConfig = tableConfig;
        return this;
    }

    public void build() throws Exception {
        SourceFunction<String> source =
                PostgreSqlActionUtils.buildPostgreSqlSource(postgreSqlConfig);
        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        PostgreSqlSchema postgreSqlSchema = getPostgreSqlSchemaList().stream()
                .reduce(PostgreSqlSchema::merge)
                .orElseThrow(
                        () ->
                                new RuntimeException(
                                        "No table satisfies the given schema name and table name"));

        catalog.createDatabase(database, true);

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        List<ComputedColumn> computedColumns =
                buildComputedColumns(computedColumnArgs, postgreSqlSchema.schema().fields());

        Schema fromPostgreSql =
                PostgreSqlActionUtils.buildPaimonSchema(
                        postgreSqlSchema,
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        tableConfig,
                        catalog.caseSensitive());

        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            checkArgument(
                    computedColumnArgs.isEmpty(),
                    "Cannot add computed column when table already exists.");
            PostgreSqlActionUtils.assertSchemaCompatible(table.schema(), fromPostgreSql);
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, fromPostgreSql, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }

        String serverTimeZone = postgreSqlConfig.get(PostgresSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        EventParser.Factory<String> parserFactory =
                () ->
                        new PostgreSqlDebeziumJsonEventParser(
                                zoneId, catalog.caseSensitive(), computedColumns);

        CdcSinkBuilder<String> sinkBuilder =
                new CdcSinkBuilder<String>()
                        .withInput(env.addSource(source, "PostgreSQL Source"))
                        .withParserFactory(parserFactory)
                        .withTable(table);
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private List<PostgreSqlSchema> getPostgreSqlSchemaList() throws Exception {
        String databaseName = postgreSqlConfig.get(PostgresSourceOptions.DATABASE_NAME);
        Pattern schemaPattern =
                Pattern.compile(postgreSqlConfig.get(PostgresSourceOptions.SCHEMA_NAME));
        Pattern tablePattern =
                Pattern.compile(postgreSqlConfig.get(PostgresSourceOptions.TABLE_NAME));
        List<PostgreSqlSchema> postgreSqlSchemaList = new ArrayList<>();
        try (Connection conn = PostgreSqlActionUtils.getConnection(postgreSqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    Matcher schemaMatcher = schemaPattern.matcher(schemaName);
                    if (schemaMatcher.matches()) {
                        try (ResultSet tables =
                                metaData.getTables(null, schemaName, "%", new String[] {"TABLE"})) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                String tableComment = tables.getString("REMARKS");
                                Matcher tableMatcher = tablePattern.matcher(tableName);
                                if (tableMatcher.matches()) {
                                    postgreSqlSchemaList.add(
                                            new PostgreSqlSchema(
                                                    metaData, databaseName, schemaName, tableName, tableComment));
                                }
                            }
                        }
                    }
                }
            }
        }
        return postgreSqlSchemaList;
    }

    private void validateCaseInsensitive() {
        Map<String, String> itemsToCheck = new HashMap<>();
        itemsToCheck.put("Database name", database);
        itemsToCheck.put("Table name", table);
        itemsToCheck.put("Partition keys", String.join(",", partitionKeys));
        itemsToCheck.put("Primary keys", String.join(",", primaryKeys));

        for (Map.Entry<String, String> item : itemsToCheck.entrySet()) {
            checkArgument(
                    item.getValue().equals(item.getValue().toLowerCase()),
                    String.format(
                            "%s [%s] cannot contain upper case in case-insensitive catalog.",
                            item.getKey(), item.getValue()));
        }
    }

    @Override
    public void run() throws Exception {
        build();
        execute(String.format("PostgreSQL-Paimon Table Sync: %s.%s", database, table));
    }
}
