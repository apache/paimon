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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.TableMigrationUtils;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.table.procedure.ProcedureContext;

/** Migrate procedure to migrate hive table to paimon table. */
public class MigrateTableProcedure extends ProcedureBase {

    private static final String PAIMON_SUFFIX = "_paimon_";

    @Override
    public String identifier() {
        return "migrate_table";
    }

    public String[] call(
            ProcedureContext procedureContext, String connector, String sourceTablePath)
            throws Exception {
        return call(procedureContext, connector, sourceTablePath, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String properties)
            throws Exception {
        return call(
                procedureContext,
                connector,
                sourceTablePath,
                properties,
                Runtime.getRuntime().availableProcessors());
    }

    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String properties,
            Integer parallelism)
            throws Exception {
        Preconditions.checkArgument(connector.equals("hive"));
        String targetPaimonTablePath = sourceTablePath + PAIMON_SUFFIX;

        Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

        Migrator migrator =
                TableMigrationUtils.getImporter(
                        connector,
                        catalog,
                        sourceTablePath,
                        targetTableId.getDatabaseName(),
                        targetTableId.getObjectName(),
                        parallelism,
                        ParameterUtils.parseCommaSeparatedKeyValues(properties));

        migrator.executeMigrate();

        migrator.renameTable(false);
        return new String[] {"Success"};
    }

    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String properties,
            Integer parallelism,
            String icebrgConf)
            throws Exception {
        Preconditions.checkArgument(connector.equals("iceberg"));
        Migrator migrator =
                TableMigrationUtils.getIcebergImporter(
                        catalog,
                        parallelism,
                        ParameterUtils.parseCommaSeparatedKeyValues(properties),
                        ParameterUtils.parseCommaSeparatedKeyValues(icebrgConf));
        migrator.executeMigrate();

        migrator.renameTable(false);
        return new String[] {"Success"};
    }
}
