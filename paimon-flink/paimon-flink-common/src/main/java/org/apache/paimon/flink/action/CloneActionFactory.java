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

/** Factory to create {@link CloneAction}. */
public class CloneActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "clone";
    private static final String PARALLELISM = "parallelism";
    private static final String TARGET_WAREHOUSE = "target_warehouse";
    private static final String TARGET_DATABASE = "target_database";
    private static final String TARGET_TABLE = "target_table";
    private static final String TARGET_CATALOG_CONF = "target_catalog_conf";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> catalogConfig = catalogConfigMap(params);

        Map<String, String> targetCatalogConfig = optionalConfigMap(params, TARGET_CATALOG_CONF);
        String targetWarehouse = params.get(TARGET_WAREHOUSE);
        if (targetWarehouse != null && !targetCatalogConfig.containsKey(TARGET_WAREHOUSE)) {
            catalogConfig.put(TARGET_WAREHOUSE, targetWarehouse);
        }

        CloneAction cloneAction =
                new CloneAction(
                        params.get(DATABASE),
                        params.get(TABLE),
                        catalogConfig,
                        params.get(TARGET_DATABASE),
                        params.get(TARGET_TABLE),
                        targetCatalogConfig,
                        params.get(PARALLELISM));

        return Optional.of(cloneAction);
    }

    @Override
    public void printHelp() {
        System.out.println("Action \"clone\" runs a batch job for clone the latest Snapshot.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  clone --warehouse <warehouse_path> "
                        + "[--database <database_name>] "
                        + "[--table <table_name>] "
                        + "[--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] "
                        + "--target_warehouse <target_warehouse_path> "
                        + "[--target_database <target_database_name>] "
                        + "[--target_table <target_table_name>] "
                        + "[--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] "
                        + "[--parallelism <parallelism>]");

        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  clone --warehouse s3:///path1/from/warehouse "
                        + "--database test_db "
                        + "--table test_table "
                        + "--catalog_conf s3.endpoint=https://****.com "
                        + "--catalog_conf s3.access-key=***** "
                        + "--catalog_conf s3.secret-key=***** "
                        + "--target_warehouse s3:///path2/to/warehouse "
                        + "--target_database test_db_copy "
                        + "--target_table test_table_copy "
                        + "--target_catalog_conf s3.endpoint=https://****.com "
                        + "--target_catalog_conf s3.access-key=***** "
                        + "--target_catalog_conf s3.secret-key=***** ");
    }
}
