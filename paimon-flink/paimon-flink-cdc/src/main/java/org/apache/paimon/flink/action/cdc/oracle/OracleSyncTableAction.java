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

package org.apache.paimon.flink.action.cdc.oracle;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.schema.Schema;

import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.oracle.source.config.OracleSourceOptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple Oracle tables into one Paimon table.
 *
 * <p>You should specify Oracle source table in {@code OracleConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified Oracle tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified Oracle tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 */
public class OracleSyncTableAction extends SyncTableActionBase {

    private JdbcSchemasInfo oracleSchemasInfo;

    public OracleSyncTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> oracleConfig) {
        super(
                warehouse,
                database,
                table,
                catalogConfig,
                oracleConfig,
                SyncJobHandler.SourceType.ORACLE);
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        this.oracleSchemasInfo =
                OracleActionUtils.getOracleTableInfos(
                        cdcSourceConfig, monitorTablePredication(), new ArrayList<>(), typeMapping);
        validateOracleTableInfos(oracleSchemasInfo);
        JdbcTableInfo tableInfo = oracleSchemasInfo.mergeAll();
        return tableInfo.schema();
    }

    @Override
    protected JdbcIncrementalSource<String> buildSource() {
        List<JdbcSchemasInfo.JdbcSchemaInfo> pkTables = oracleSchemasInfo.pkTables();
        Set<String> schemaList = new HashSet<>();
        String[] tableList = new String[pkTables.size()];
        for (int i = 0; i < pkTables.size(); i++) {
            JdbcSchemasInfo.JdbcSchemaInfo pkTable = pkTables.get(i);
            tableList[i] = pkTable.schemaName() + "." + pkTable.identifier().getObjectName();
            schemaList.add(pkTable.schemaName());
        }
        return OracleActionUtils.buildOracleSource(
                cdcSourceConfig, schemaList.toArray(new String[0]), tableList);
    }

    private void validateOracleTableInfos(JdbcSchemasInfo jdbcSchemasInfo) {
        List<Identifier> nonPkTables = jdbcSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "Source tables of Oracle table synchronization job cannot contain table "
                        + "which doesn't have primary keys.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !jdbcSchemasInfo.pkTables().isEmpty(),
                "No table satisfies the given database name and table name.");
    }

    private Predicate<String> monitorTablePredication() {
        return tableName -> {
            Pattern tableNamePattern =
                    Pattern.compile(cdcSourceConfig.get(OracleSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(tableName).matches();
        };
    }
}
