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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

/** Migrate procedure to migrate hive table to paimon table. */
public class MigrateTableProcedure extends ProcedureBase {

    private static final String PAIMON_SUFFIX = "_paimon_";

    @Override
    public String identifier() {
        return "migrate_table";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "connector", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "source_table",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "parallelism",
                        type = @DataTypeHint("Integer"),
                        isOptional = true),
                @ArgumentHint(
                        name = "iceberg_options",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
            })
    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String properties,
            Integer parallelism,
            String icebergOptions)
            throws Exception {
        properties = notnull(properties);
        icebergOptions = notnull(icebergOptions);

        Integer p = parallelism == null ? Runtime.getRuntime().availableProcessors() : parallelism;

        Migrator migrator;
        switch (connector) {
            case "hive":
                Preconditions.checkArgument(
                        sourceTablePath != null, "please set 'source_table' for hive migrator");
                String targetPaimonTablePath = sourceTablePath + PAIMON_SUFFIX;
                Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);
                migrator =
                        TableMigrationUtils.getImporter(
                                connector,
                                catalog,
                                sourceTablePath,
                                targetTableId.getDatabaseName(),
                                targetTableId.getObjectName(),
                                p,
                                ParameterUtils.parseCommaSeparatedKeyValues(properties));
                break;
            case "iceberg":
                migrator =
                        TableMigrationUtils.getIcebergImporter(
                                catalog,
                                p,
                                ParameterUtils.parseCommaSeparatedKeyValues(properties),
                                ParameterUtils.parseCommaSeparatedKeyValues(icebergOptions));
                break;
            default:
                throw new UnsupportedOperationException("Don't support connector " + connector);
        }

        migrator.executeMigrate();

        migrator.renameTable(false);
        return new String[] {"Success"};
    }
}
