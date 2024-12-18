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

import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.HashMap;
import java.util.Map;

/** Clone Procedure. */
public class CloneProcedure extends ProcedureBase {
    public static final String IDENTIFIER = "clone";

    @ProcedureHint(
            argument = {
                @ArgumentHint(
                        name = "warehouse",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "catalog_conf",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "target_warehouse", type = @DataTypeHint("STRING")),
                @ArgumentHint(
                        name = "target_database",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "target_table",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "target_catalog_conf",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "parallelism", type = @DataTypeHint("INT"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String tableName,
            String sourceCatalogConfigStr,
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            String targetCatalogConfigStr,
            Integer parallelismStr)
            throws Exception {
        Map<String, String> sourceCatalogConfig =
                new HashMap<>(optionalConfigMap(sourceCatalogConfigStr));
        if (!StringUtils.isNullOrWhitespaceOnly(warehouse)
                && !sourceCatalogConfig.containsKey("warehouse")) {
            sourceCatalogConfig.put("warehouse", warehouse);
        }

        Map<String, String> targetCatalogConfig =
                new HashMap<>(optionalConfigMap(targetCatalogConfigStr));
        if (!StringUtils.isNullOrWhitespaceOnly(warehouse)
                && !targetCatalogConfig.containsKey("warehouse")) {
            targetCatalogConfig.put("warehouse", targetWarehouse);
        }

        CloneAction cloneAction =
                new CloneAction(
                        database,
                        tableName,
                        sourceCatalogConfig,
                        targetDatabase,
                        targetTableName,
                        targetCatalogConfig,
                        parallelismStr == null ? null : Integer.toString(parallelismStr));
        return execute(procedureContext, cloneAction, "Clone Job");
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
