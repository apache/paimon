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

import org.apache.paimon.flink.utils.TableMigrationUtils;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.procedure.ProcedureContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Migrate procedure to migrate all hive tables in database to paimon table. */
public class MigrateDatabaseProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateDatabaseProcedure.class);

    @Override
    public String identifier() {
        return "migrate_database";
    }

    public String[] call(
            ProcedureContext procedureContext, String connector, String sourceDatabasePath)
            throws Exception {
        return call(procedureContext, connector, sourceDatabasePath, "");
    }

    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceDatabasePath,
            String properties)
            throws Exception {
        List<Migrator> migrators =
                TableMigrationUtils.getImporters(
                        connector,
                        catalog,
                        sourceDatabasePath,
                        Runtime.getRuntime().availableProcessors(),
                        ParameterUtils.parseCommaSeparatedKeyValues(properties));

        String retStr = handleMigrators(migrators);
        return new String[] {retStr};
    }

    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceDatabasePath,
            String properties,
            Integer parallelism)
            throws Exception {
        Integer p = parallelism == null ? Runtime.getRuntime().availableProcessors() : parallelism;
        List<Migrator> migrators =
                TableMigrationUtils.getImporters(
                        connector,
                        catalog,
                        sourceDatabasePath,
                        p,
                        ParameterUtils.parseCommaSeparatedKeyValues(properties));

        String retStr = handleMigrators(migrators);
        return new String[] {retStr};
    }

    public String handleMigrators(List<Migrator> migrators) {
        int errorCount = 0;
        int successCount = 0;

        for (Migrator migrator : migrators) {
            try {
                migrator.executeMigrate();
                migrator.renameTable(false);
                successCount++;
            } catch (Exception e) {
                errorCount++;
                LOG.error("Call migrate_database error:" + e.getMessage());
            }
        }
        String retStr =
                String.format(
                        "migrate database is finished, success cnt: %s , failed cnt: %s",
                        String.valueOf(successCount), String.valueOf(errorCount));
        return retStr;
    }
}
