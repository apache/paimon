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
import org.apache.paimon.utils.ParameterUtils;

import org.apache.flink.table.procedure.ProcedureContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Migrate procedure to migrate hive table to paimon table. */
public class MigrateTableProcedure extends ProcedureBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateTableProcedure.class);

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
        String targetPaimonTablePath = sourceTablePath + PAIMON_SUFFIX;

        Identifier sourceTableId = Identifier.fromString(sourceTablePath);
        Identifier targetTableId = Identifier.fromString(targetPaimonTablePath);

        HashMap<String, String> options = new HashMap<>();
        options.putAll(ParameterUtils.parseCommaSeparatedKeyValues(properties));

        TableMigrationUtils.getImporter(
                        connector,
                        catalog,
                        sourceTableId.getDatabaseName(),
                        sourceTableId.getObjectName(),
                        targetTableId.getDatabaseName(),
                        targetTableId.getObjectName(),
                        options)
                .executeMigrate();

        LOG.info("Last step: rename " + targetTableId + " to " + sourceTableId);
        catalog.renameTable(targetTableId, sourceTableId, false);
        return new String[] {"Success"};
    }
}
