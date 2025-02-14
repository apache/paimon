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

import org.apache.flink.table.procedure.ProcedureContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Migrate procedure to migrate iceberg table to paimon table. */
public class MigrateIcebergTableProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateIcebergTableProcedure.class);

    private static final String PAIMON_SUFFIX = "_paimon_";

    @Override
    public String identifier() {
        return "migrate_iceberg_table";
    }

    public String[] call(
            ProcedureContext procedureContext, String sourceTablePath, String icebergProperties)
            throws Exception {

        return call(procedureContext, sourceTablePath, icebergProperties, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String sourceTablePath,
            String icebergProperties,
            String properties)
            throws Exception {

        return call(
                procedureContext,
                sourceTablePath,
                icebergProperties,
                properties,
                Runtime.getRuntime().availableProcessors());
    }

    public String[] call(
            ProcedureContext procedureContext,
            String sourceTablePath,
            String icebergProperties,
            String properties,
            Integer parallelism)
            throws Exception {
        String targetTablePath = sourceTablePath + PAIMON_SUFFIX;

        Identifier sourceTableId = Identifier.fromString(sourceTablePath);
        Identifier targetTableId = Identifier.fromString(targetTablePath);

        Migrator migrator =
                TableMigrationUtils.getIcebergImporter(
                        catalog,
                        sourceTableId.getDatabaseName(),
                        sourceTableId.getObjectName(),
                        targetTableId.getDatabaseName(),
                        targetTableId.getObjectName(),
                        parallelism,
                        ParameterUtils.parseCommaSeparatedKeyValues(properties),
                        ParameterUtils.parseCommaSeparatedKeyValues(icebergProperties));
        LOG.info("create migrator success.");
        migrator.executeMigrate();

        migrator.renameTable(false);
        return new String[] {"Success"};
    }
}
