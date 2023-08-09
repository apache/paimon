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

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.flink.action.cdc.mysql.MySqlActionUtils.MYSQL_CONVERTER_TINYINT1_BOOL;
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
public class MySqlSyncTableAction extends ActionBase {

    private final Configuration mySqlConfig;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private final List<String> computedColumnArgs;
    private final Map<String, String> tableConfig;

    public MySqlSyncTableAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                mySqlConfig,
                warehouse,
                database,
                table,
                partitionKeys,
                primaryKeys,
                Collections.emptyList(),
                catalogConfig,
                tableConfig);
    }

    public MySqlSyncTableAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<String> computedColumnArgs,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        super(warehouse, catalogConfig);
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
        this.database = database;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.computedColumnArgs = computedColumnArgs;
        this.tableConfig = tableConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        checkArgument(
                mySqlConfig.contains(MySqlSourceOptions.TABLE_NAME),
                String.format(
                        "mysql-conf [%s] must be specified.", MySqlSourceOptions.TABLE_NAME.key()));

        boolean caseSensitive = catalog.caseSensitive();

        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        List<MySqlSchema> mySqlSchemaList =
                MySqlActionUtils.getMySqlSchemaList(
                        mySqlConfig, monitorTablePredication(), new ArrayList<>());

        String tableList =
                mySqlSchemaList.stream()
                        .map(m -> m.identifier().getDatabaseName() + "." + m.tableName())
                        .collect(Collectors.joining("|"));

        MySqlSource<String> source = MySqlActionUtils.buildMySqlSource(mySqlConfig, tableList);

        MySqlSchema mySqlSchema =
                mySqlSchemaList.stream()
                        .reduce(MySqlSchema::merge)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No table satisfies the given database name and table name"));

        catalog.createDatabase(database, true);

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        List<ComputedColumn> computedColumns =
                buildComputedColumns(computedColumnArgs, mySqlSchema.typeMapping());
        Schema fromMySql =
                MySqlActionUtils.buildPaimonSchema(
                        mySqlSchema,
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        tableConfig,
                        caseSensitive);
        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            if (computedColumns.size() > 0) {
                List<String> computedFields =
                        computedColumns.stream()
                                .map(ComputedColumn::columnName)
                                .collect(Collectors.toList());
                List<String> fieldNames = table.schema().fieldNames();
                checkArgument(
                        new HashSet<>(fieldNames).containsAll(computedFields),
                        " Exists Table should contain all computed columns %s, but are %s.",
                        computedFields,
                        fieldNames);
            }
            MySqlActionUtils.assertSchemaCompatible(table.schema(), fromMySql);
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, fromMySql, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        Boolean convertTinyint1ToBool = mySqlConfig.get(MYSQL_CONVERTER_TINYINT1_BOOL);
        EventParser.Factory<String> parserFactory =
                () ->
                        new MySqlDebeziumJsonEventParser(
                                zoneId, caseSensitive, computedColumns, convertTinyint1ToBool);

        CdcSinkBuilder<String> sinkBuilder =
                new CdcSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withTable(table)
                        .withIdentifier(identifier)
                        .withCatalogLoader(catalogLoader());
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void validateCaseInsensitive() {
        checkArgument(
                database.equals(database.toLowerCase()),
                String.format(
                        "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                        database));
        checkArgument(
                table.equals(table.toLowerCase()),
                String.format(
                        "Table name [%s] cannot contain upper case in case-insensitive catalog.",
                        table));
        for (String part : partitionKeys) {
            checkArgument(
                    part.equals(part.toLowerCase()),
                    String.format(
                            "Partition keys [%s] cannot contain upper case in case-insensitive catalog.",
                            partitionKeys));
        }
        for (String pk : primaryKeys) {
            checkArgument(
                    pk.equals(pk.toLowerCase()),
                    String.format(
                            "Primary keys [%s] cannot contain upper case in case-insensitive catalog.",
                            primaryKeys));
        }
    }

    private Predicate<MySqlSchema> monitorTablePredication() {
        return schema -> {
            Pattern tableNamePattern =
                    Pattern.compile(mySqlConfig.get(MySqlSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(schema.tableName()).matches();
        };
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, String.format("MySQL-Paimon Table Sync: %s.%s", database, table));
    }
}
