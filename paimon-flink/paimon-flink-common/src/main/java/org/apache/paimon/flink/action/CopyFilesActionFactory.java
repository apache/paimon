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

/**
 * The Latest Snapshot copy files action factory for Flink.
 *
 * @deprecated The normal process should commit a snapshot to the catalog, but this action does not
 *     do so. Currently, this action can only be applied to the FileSystemCatalog.
 */
@Deprecated
public class CopyFilesActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "copy_files";
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

        Map<String, String> targetCatalogConfig =
                new HashMap<>(optionalConfigMap(params, TARGET_CATALOG_CONF));
        String targetWarehouse = params.get(TARGET_WAREHOUSE);
        if (targetWarehouse != null && !targetCatalogConfig.containsKey(WAREHOUSE)) {
            targetCatalogConfig.put(WAREHOUSE, targetWarehouse);
        }

        CopyFilesAction action =
                new CopyFilesAction(
                        params.get(DATABASE),
                        params.get(TABLE),
                        catalogConfig,
                        params.get(TARGET_DATABASE),
                        params.get(TARGET_TABLE),
                        targetCatalogConfig,
                        params.get(PARALLELISM));

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"copy_files\" runs a batch job for copying files the latest Snapshot.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  copy_files --warehouse <warehouse_path> \\\n"
                        + "[--database <database_name>] \\\n"
                        + "[--table <table_name>] \\\n"
                        + "[--catalog_conf <source-paimon-catalog-conf> [--catalog_conf <source-paimon-catalog-conf> ...]] \\\n"
                        + "--target_warehouse <target_warehouse_path> \\\n"
                        + "[--target_database <target_database_name>] \\\n"
                        + "[--target_table <target_table_name>] \\\n"
                        + "[--target_catalog_conf <target-paimon-catalog-conf> [--target_catalog_conf <target-paimon-catalog-conf> ...]] \\\n"
                        + "[--parallelism <parallelism>]");

        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  copy_files --warehouse s3:///path1/from/warehouse \\\n"
                        + "--database test_db \\\n"
                        + "--table test_table \\\n"
                        + "--catalog_conf s3.endpoint=https://****.com \\\n"
                        + "--catalog_conf s3.access-key=***** \\\n"
                        + "--catalog_conf s3.secret-key=***** \\\n"
                        + "--target_warehouse s3:///path2/to/warehouse \\\n"
                        + "--target_database test_db_copy \\\n"
                        + "--target_table test_table_copy \\\n"
                        + "--target_catalog_conf s3.endpoint=https://****.com \\\n"
                        + "--target_catalog_conf s3.access-key=***** \\\n"
                        + "--target_catalog_conf s3.secret-key=***** ");
    }
}
