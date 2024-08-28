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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;

/** Add file procedure to add file from hive to paimon. */
public class MigrateFileProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "migrate_file";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "connector", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "source_table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "target_table", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "delete_origin",
                        type = @DataTypeHint("BOOLEAN"),
                        isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String connector,
            String sourceTablePath,
            String targetPaimonTablePath,
            Boolean deleteOrigin)
            throws Exception {
        if (deleteOrigin == null) {
            deleteOrigin = true;
        }
        migrateHandle(connector, sourceTablePath, targetPaimonTablePath, deleteOrigin);
        return new String[] {"Success"};
    }

    public void migrateHandle(
            String connector,
            String sourceTablePath,
            String targetPaimonTablePath,
            boolean deleteOrigin)
            throws Exception {
        Identifier sourceTableId = Identifier.fromString(sourceTablePath);
        Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

        if (!(catalog.tableExists(targetTableId))) {
            throw new IllegalArgumentException(
                    "Target paimon table does not exist: " + targetPaimonTablePath);
        }

        Migrator importer =
                TableMigrationUtils.getImporter(
                        connector,
                        catalog,
                        sourceTableId.getDatabaseName(),
                        sourceTableId.getObjectName(),
                        targetTableId.getDatabaseName(),
                        targetTableId.getObjectName(),
                        Collections.emptyMap());
        importer.deleteOriginTable(deleteOrigin);
        importer.executeMigrate();
    }
}
