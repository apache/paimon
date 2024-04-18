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

import org.apache.paimon.clone.CloneType;
import org.apache.paimon.flink.action.CloneAction;

import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.utils.ParameterUtils.parseCommaSeparatedKeyValues;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StringUtils.isBlank;

/**
 * Clone snapshot/tag/table procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.clone('warehouse', 'database', 'table', 'catalog_config', 'target_warehouse', 'target_database',
 *      'target_table', 'target_catalog_config', parallelism, 'clone_type')
 * </code></pre>
 */
public class CloneProcedure extends ProcedureBase {

    public static final String IDENTIFIER = "clone";

    public String[] call(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String table,
            String catalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTable,
            String targetCatalogConfig,
            String parallelism,
            String cloneTypeStr)
            throws Exception {
        CloneType cloneType = CloneType.valueOf(cloneTypeStr);
        checkArgument(
                cloneType.equals(CloneType.Table) || cloneType.equals(CloneType.LatestSnapshot));

        return innerCall(
                procedureContext,
                warehouse,
                database,
                table,
                getCatalogConfigMap(catalogConfig),
                targetWarehouse,
                targetDatabase,
                targetTable,
                getCatalogConfigMap(targetCatalogConfig),
                parallelism,
                cloneType,
                null,
                null,
                null);
    }

    public String[] call(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String table,
            String catalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTable,
            String targetCatalogConfig,
            String parallelism,
            String cloneTypeStr,
            long snapshotIdOrTimestamp)
            throws Exception {
        CloneType cloneType = CloneType.valueOf(cloneTypeStr);
        checkArgument(
                cloneType.equals(CloneType.SpecificSnapshot)
                        || cloneType.equals(CloneType.FromTimestamp));

        if (cloneType.equals(CloneType.SpecificSnapshot)) {
            return innerCall(
                    procedureContext,
                    warehouse,
                    database,
                    table,
                    getCatalogConfigMap(catalogConfig),
                    targetWarehouse,
                    targetDatabase,
                    targetTable,
                    getCatalogConfigMap(targetCatalogConfig),
                    parallelism,
                    cloneType,
                    snapshotIdOrTimestamp,
                    null,
                    null);
        } else {
            return innerCall(
                    procedureContext,
                    warehouse,
                    database,
                    table,
                    getCatalogConfigMap(catalogConfig),
                    targetWarehouse,
                    targetDatabase,
                    targetTable,
                    getCatalogConfigMap(targetCatalogConfig),
                    parallelism,
                    cloneType,
                    null,
                    null,
                    snapshotIdOrTimestamp);
        }
    }

    public String[] call(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String table,
            String catalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTable,
            String targetCatalogConfig,
            String parallelism,
            String cloneTypeStr,
            String tagName)
            throws Exception {
        CloneType cloneType = CloneType.valueOf(cloneTypeStr);
        checkArgument(cloneType.equals(CloneType.Tag));

        return innerCall(
                procedureContext,
                warehouse,
                database,
                table,
                getCatalogConfigMap(catalogConfig),
                targetWarehouse,
                targetDatabase,
                targetTable,
                getCatalogConfigMap(targetCatalogConfig),
                parallelism,
                cloneType,
                null,
                tagName,
                null);
    }

    private String[] innerCall(
            ProcedureContext procedureContext,
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfigMap,
            String targetWarehouse,
            String targetDatabase,
            String targetTable,
            Map<String, String> targetCatalogConfigMap,
            String parallelism,
            CloneType cloneType,
            @Nullable Long snapshotId,
            @Nullable String tagName,
            @Nullable Long timestamp)
            throws Exception {
        CloneAction cloneAction =
                new CloneAction(
                        warehouse,
                        database,
                        table,
                        catalogConfigMap,
                        targetWarehouse,
                        targetDatabase,
                        targetTable,
                        targetCatalogConfigMap,
                        parallelism,
                        cloneType,
                        snapshotId,
                        tagName,
                        timestamp);

        return execute(procedureContext, cloneAction, "Clone");
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
