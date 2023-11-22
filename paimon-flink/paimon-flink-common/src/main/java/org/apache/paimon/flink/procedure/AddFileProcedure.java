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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.utils.TableMigrationUtils;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.procedure.ProcedureContext;

/** Add file procedure to add file from hive to paimon. */
public class AddFileProcedure extends GenericProcedureBase {

    @Override
    public String identifier() {
        return "add_file";
    }

    public String[] call(
            ProcedureContext procedureContext, String sourceTablePath, String targetPaimonTablePath)
            throws Exception {
        return call(procedureContext, sourceTablePath, targetPaimonTablePath, false, false);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String sourceTablePath,
            String targetPaimonTablePath,
            boolean sync,
            boolean deleteOrigin)
            throws Exception {
        Catalog paimonCatalog = flinkGenericCatalog.paimonFlinkCatalog().catalog();
        Identifier sourceTableId = Identifier.getOrDefault(sourceTablePath, defaultDatabase);
        Identifier targetTableId = Identifier.getOrDefault(targetPaimonTablePath, defaultDatabase);

        CatalogBaseTable sourceFlinkTable =
                flinkGenericCatalog.getTable(
                        new ObjectPath(
                                sourceTableId.getDatabaseName(), sourceTableId.getObjectName()));

        Table targetPaimonTable =
                flinkGenericCatalog.paimonFlinkCatalog().catalog().getTable(targetTableId);

        if (!(targetPaimonTable instanceof AbstractFileStoreTable)) {
            throw new IllegalArgumentException("Target table must be paimon data table");
        }

        TableMigrationUtils.getImporter(
                        sourceFlinkTable,
                        paimonCatalog,
                        sourceTableId.getDatabaseName(),
                        sourceTableId.getObjectName(),
                        (AbstractFileStoreTable) targetPaimonTable)
                .executeImport(sync, deleteOrigin);

        return new String[] {"Success"};
    }
}
