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

import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CloneDatabaseAction}. */
public class CloneDatabaseActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "clone_database";
    private static final String PARALLELISM = "parallelism";
    private static final String TARGET_WAREHOUSE = "target_warehouse";
    private static final String TARGET_DATABASE = "target_database";
    private static final String TARGET_CATALOG_CONF = "target_catalog_conf";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String sourceWarehouse = getRequiredValue(params, WAREHOUSE);
        String sourceDatabase = getRequiredValue(params, DATABASE);
        Map<String, String> sourceCatalogConfig = optionalConfigMap(params, CATALOG_CONF);
        String targetWarehouse = getRequiredValue(params, TARGET_WAREHOUSE);
        String targetDatabase = getRequiredValue(params, TARGET_DATABASE);

        Map<String, String> targetCatalogConfig = optionalConfigMap(params, TARGET_CATALOG_CONF);
        String parallelismStr = params.get(PARALLELISM);

        CloneDatabaseAction cloneDatabaseAction =
                new CloneDatabaseAction(
                        sourceWarehouse,
                        sourceDatabase,
                        sourceCatalogConfig,
                        targetWarehouse,
                        targetDatabase,
                        targetCatalogConfig,
                        parallelismStr);

        return Optional.of(cloneDatabaseAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"clone_database\" runs a batch job for clone specified Database.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  clone_database --warehouse <warehouse_path>"
                        + "--database <database_name> "
                        + "[--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]]"
                        + "--target_warehouse <target_warehouse_path>"
                        + "--target_database <target_database_name> "
                        + "[--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]]");

        System.out.println(
                "  clone_database --warehouse s3:///path1/from/warehouse "
                        + "--database test_db "
                        + "--catalog_conf s3.endpoint=https://****.com "
                        + "--catalog_conf s3.access-key=***** "
                        + "--catalog_conf s3.secret-key=***** "
                        + "--target_warehouse s3:///path2/to/warehouse "
                        + "--target_database test_db_copy "
                        + "--target_catalog_conf s3.endpoint=https://****.com "
                        + "--target_catalog_conf s3.access-key=***** "
                        + "--target_catalog_conf s3.access-key=***** ");
    }
}
