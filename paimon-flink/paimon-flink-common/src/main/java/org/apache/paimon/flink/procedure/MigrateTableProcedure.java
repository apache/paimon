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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Migrate procedure to migrate hive table to paimon table. */
public class MigrateTableProcedure extends GenericProcedureBase {

    private static final String BACK_SUFFIX = "_backup_";

    @Override
    public String identifier() {
        return "migrate_table";
    }

    public String[] call(ProcedureContext procedureContext, String sourceTablePath)
            throws Exception {
        return call(procedureContext, sourceTablePath, "");
    }

    public String[] call(
            ProcedureContext procedureContext, String sourceTablePath, String properties)
            throws Exception {
        TableEnvironmentImpl tableEnvironment =
                TableEnvironmentImpl.create(EnvironmentSettings.inBatchMode());
        Identifier sourceTableId = Identifier.getOrDefault(sourceTablePath, defaultDatabase);

        CatalogTable sourceFlinkTable =
                (CatalogTable)
                        flinkGenericCatalog.getTable(
                                new ObjectPath(
                                        sourceTableId.getDatabaseName(),
                                        sourceTableId.getObjectName()));
        ResolvedCatalogTable resolvedSourceCatalogTable =
                tableEnvironment.getCatalogManager().resolveCatalogTable(sourceFlinkTable);
        ResolvedSchema resolvedSchema = resolvedSourceCatalogTable.getResolvedSchema();

        if (resolvedSchema.getPrimaryKey().isPresent()
                && !resolvedSchema.getPrimaryKey().get().getColumns().isEmpty()) {
            throw new IllegalArgumentException("Can't migrate primary key table yet.");
        }

        String backTable = sourceTablePath + BACK_SUFFIX;

        Identifier backTableId = Identifier.getOrDefault(backTable, defaultDatabase);

        ObjectPath sourceObjectPath =
                new ObjectPath(sourceTableId.getDatabaseName(), sourceTableId.getObjectName());

        Map<String, String> paimonOption =
                toPaimonOption(ParameterUtils.parseCommaSeparatedKeyValues(properties));

        CatalogTable table =
                new CatalogTableImpl(
                        resolvedSourceCatalogTable.getSchema(),
                        resolvedSourceCatalogTable.getPartitionKeys(),
                        paimonOption,
                        resolvedSourceCatalogTable.getComment());
        flinkGenericCatalog.renameTable(sourceObjectPath, backTableId.getObjectName(), false);
        flinkGenericCatalog.createTable(sourceObjectPath, table, false);

        MigrateFileProcedure migrateFileProcedure = new MigrateFileProcedure();
        migrateFileProcedure.withDefaultDatabase(defaultDatabase);
        migrateFileProcedure.withFlinkCatalog(flinkGenericCatalog);

        return migrateFileProcedure.call(procedureContext, backTable, sourceTablePath, false);
    }

    private Map<String, String> toPaimonOption(Map<String, String> optionsMap) {
        HashMap<String, String> map = new HashMap<>(optionsMap);
        map.put(CoreOptions.BUCKET.key(), "-1");
        map.put(CONNECTOR.key(), FlinkCatalogFactory.IDENTIFIER);
        return map;
    }
}
