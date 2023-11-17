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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.sqlserver.SqlServerActionUtils.buildSqlServerSource;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An {@link Action} which synchronize one or multiple SqlServer tables into one Paimon table. */
public class SqlServerSyncTableAction extends SyncTableActionBase {
    private JdbcSchemasInfo sqlServerSchemasInfo;

    public SqlServerSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> cdcSourceConfig) {
        super(
                warehouse,
                database,
                table,
                catalogConfig,
                cdcSourceConfig,
                SyncJobHandler.SourceType.SQLSERVER);
        SqlServerActionUtils.registerJdbcDriver();
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        this.sqlServerSchemasInfo =
                SqlServerActionUtils.getSqlServerTableInfos(
                        cdcSourceConfig,
                        matchTablePredication(cdcSourceConfig),
                        new ArrayList<>(),
                        typeMapping);
        validateSqlServerTableInfos(sqlServerSchemasInfo);
        JdbcTableInfo tableInfo = sqlServerSchemasInfo.mergeAll();
        return tableInfo.schema();
    }

    private static Predicate<String> matchTablePredication(Configuration cdcSourceConfig) {
        return tableName -> {
            Pattern tableNamePattern =
                    Pattern.compile(cdcSourceConfig.get(SqlServerSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(tableName).matches();
        };
    }

    private void validateSqlServerTableInfos(JdbcSchemasInfo sqlServerSchemasInfo) {
        List<Identifier> nonPkTables = sqlServerSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "SqlServer source will not synchronize tables without a primary key.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !sqlServerSchemasInfo.pkTables().isEmpty(),
                "No tables matching the configuration were found.");
    }

    @Override
    protected SqlServerSourceBuilder.SqlServerIncrementalSource<String> buildSource() {
        String tableList =
                sqlServerSchemasInfo.pkTables().stream()
                        .map(i -> i.schemaName() + "." + i.identifier().getObjectName())
                        .collect(Collectors.joining("|"));
        return buildSqlServerSource(cdcSourceConfig, tableList);
    }
}
