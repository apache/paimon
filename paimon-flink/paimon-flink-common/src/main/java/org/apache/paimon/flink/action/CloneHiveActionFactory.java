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

package org.apache.paimon.flink.action;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CloneHiveAction}. */
public class CloneHiveActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "clone_hive";
    private static final String TARGET_WAREHOUSE = "target_warehouse";
    private static final String TARGET_DATABASE = "target_database";
    private static final String TARGET_TABLE = "target_table";
    private static final String TARGET_CATALOG_CONF = "target_catalog_conf";
    private static final String PARALLELISM = "parallelism";
    private static final String WHERE = "where";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = catalogConfigMap(params);

        Map<String, String> targetCatalogConfig =
                new HashMap<>(optionalConfigMap(params, TARGET_CATALOG_CONF));
        String targetWarehouse = params.get(TARGET_WAREHOUSE);
        if (targetWarehouse != null && !targetCatalogConfig.containsKey(WAREHOUSE)) {
            targetCatalogConfig.put(WAREHOUSE, targetWarehouse);
        }

        String parallelism = params.get(PARALLELISM);

        CloneHiveAction cloneHiveAction =
                new CloneHiveAction(
                        params.get(DATABASE),
                        params.get(TABLE),
                        catalogConfig,
                        params.get(TARGET_DATABASE),
                        params.get(TARGET_TABLE),
                        targetCatalogConfig,
                        parallelism == null ? null : Integer.parseInt(parallelism),
                        params.get(WHERE));

        return Optional.of(cloneHiveAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"clone_hive\" clones the source files and migrate them to paimon table.");
        System.out.println();
    }
}
