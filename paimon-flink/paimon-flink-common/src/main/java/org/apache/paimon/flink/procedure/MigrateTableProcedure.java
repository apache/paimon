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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Migrate procedure to migrate hive table to paimon table. */
public class MigrateTableProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateTableProcedure.class);

    private static final String PAIMON_SUFFIX = "_paimon_";

    @Override
    public String identifier() {
        return "migrate_table";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "connector", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "source_table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "parallelism",
                        type = @DataTypeHint("Integer"),
                        isOptional = true),
                @ArgumentHint(
                        name = "target_table",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "iceberg_options",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String properties,
            Integer parallelism,
            String targetTablePath,
            String icebergProperties)
            throws Exception {
        properties = notnull(properties);
        icebergProperties = notnull(icebergProperties);

        String targetPaimonTablePath = sourceTablePath + PAIMON_SUFFIX;
        if (targetTablePath != null) {
            targetPaimonTablePath = targetTablePath;
        }

        Identifier sourceTableId = Identifier.fromString(sourceTablePath);
        Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

        Integer p = parallelism == null ? Runtime.getRuntime().availableProcessors() : parallelism;

        Migrator migrator =
                TableMigrationUtils.getImporter(
                        connector,
                        catalog,
                        sourceTableId.getDatabaseName(),
                        sourceTableId.getObjectName(),
                        targetTableId.getDatabaseName(),
                        targetTableId.getObjectName(),
                        p,
                        ParameterUtils.parseCommaSeparatedKeyValues(properties),
                        ParameterUtils.parseCommaSeparatedKeyValues(icebergProperties));
        LOG.info("create migrator success.");
        migrator.executeMigrate();

        migrator.renameTable(false);
        return new String[] {"Success"};
    }
}
