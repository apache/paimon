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

import org.apache.paimon.flink.action.CloneDatabaseAction;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.StringUtils.isBlank;

/**
 * Clone database procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.clone_database('warehouse', 'database', 'catalog_config', 'target_warehouse', 'target_database',
 *       'target_catalog_config', parallelism)
 * </code></pre>
 */
public class CloneDatabaseProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "clone_database";

    public String[] call(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String catalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetCatalogConfig,
            String parallelism)
            throws Exception {

        return innerCall(
                procedureContext,
                warehouse,
                database,
                getCatalogConfigMap(catalogConfig),
                targetWarehouse,
                targetDatabase,
                getCatalogConfigMap(targetCatalogConfig),
                parallelism);
    }

    private String[] innerCall(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            Map<String, String> catalogConfigMap,
            String targetWarehouse,
            String targetDatabase,
            Map<String, String> targetCatalogConfigMap,
            String parallelism)
            throws Exception {
        CloneDatabaseAction cloneDatabaseAction =
                new CloneDatabaseAction(
                        warehouse,
                        database,
                        catalogConfigMap,
                        targetWarehouse,
                        targetDatabase,
                        targetCatalogConfigMap,
                        parallelism);

        return execute(procedureContext, cloneDatabaseAction, "CloneDatabase");
    }

    private Map<String, String> getCatalogConfigMap(String catalogConfig) {
        return isBlank(catalogConfig)
                ? Collections.emptyMap()
                : parseCommaSeparatedKeyValues(catalogConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
