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
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** Migrate procedure to migrate all hive tables in database to paimon table. */
public class MigrateDatabaseProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateDatabaseProcedure.class);
    private static final String PAIMON_SUFFIX = "_paimon_";

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
        if (!(catalog instanceof HiveCatalog)) {
            throw new IllegalArgumentException("Only support Hive Catalog");
        }
        HiveCatalog hiveCatalog = (HiveCatalog) this.catalog;
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        List<String> sourceTables = client.getAllTables(sourceDatabasePath);
        for (String sourceTable : sourceTables) {
            String sourceTablePath = sourceDatabasePath + "." + sourceTable;
            String targetPaimonTablePath = sourceTablePath + PAIMON_SUFFIX;

            Identifier sourceTableId = Identifier.fromString(sourceTablePath);
            Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

            TableMigrationUtils.getImporter(
                            connector,
                            (HiveCatalog) this.catalog,
                            sourceTableId.getDatabaseName(),
                            sourceTableId.getObjectName(),
                            targetTableId.getDatabaseName(),
                            targetTableId.getObjectName(),
                            ParameterUtils.parseCommaSeparatedKeyValues(properties))
                    .executeMigrate();

            LOG.info("rename " + targetTableId + " to " + sourceTableId);
            this.catalog.renameTable(targetTableId, sourceTableId, false);
        }
        return new String[] {"Success"};
    }
}
