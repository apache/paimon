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

import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CloneAction}. */
public class CloneActionFactory implements ActionFactory {

    private static final String IDENTIFIER = "clone";
    private static final String TARGET_WAREHOUSE = "target_warehouse";
    private static final String TARGET_DATABASE = "target_database";
    private static final String TARGET_TABLE = "target_table";
    private static final String TARGET_CATALOG_CONF = "target_catalog_conf";
    private static final String PARALLELISM = "parallelism";
    private static final String WHERE = "where";
    private static final String INCLUDED_TABLES = "included_tables";
    private static final String EXCLUDED_TABLES = "excluded_tables";
    private static final String PREFER_FILE_FORMAT = "prefer_file_format";
    private static final String CLONE_FROM = "clone_from";
    private static final String META_ONLY = "meta_only";
    private static final String CLONE_IF_EXISTS = "clone_if_exists";

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

        String includedTablesStr = params.get(INCLUDED_TABLES);
        List<String> includedTables =
                StringUtils.isNullOrWhitespaceOnly(includedTablesStr)
                        ? null
                        : Arrays.asList(StringUtils.split(includedTablesStr, ","));

        String excludedTablesStr = params.get(EXCLUDED_TABLES);
        List<String> excludedTables =
                StringUtils.isNullOrWhitespaceOnly(excludedTablesStr)
                        ? null
                        : Arrays.asList(StringUtils.split(excludedTablesStr, ","));

        String cloneFrom = params.get(CLONE_FROM);
        if (StringUtils.isNullOrWhitespaceOnly(cloneFrom)) {
            cloneFrom = "hive";
        }
        String preferFileFormat = params.get(PREFER_FILE_FORMAT);

        String metaOnlyStr = params.get(META_ONLY);
        boolean metaOnly =
                !StringUtils.isNullOrWhitespaceOnly(metaOnlyStr)
                        && Boolean.parseBoolean(metaOnlyStr);

        String cloneIfExistsStr = params.get(CLONE_IF_EXISTS);
        boolean cloneIfExists =
                StringUtils.isNullOrWhitespaceOnly(cloneIfExistsStr)
                        || Boolean.parseBoolean(cloneIfExistsStr);

        CloneAction cloneAction =
                new CloneAction(
                        params.get(DATABASE),
                        params.get(TABLE),
                        catalogConfig,
                        params.get(TARGET_DATABASE),
                        params.get(TARGET_TABLE),
                        targetCatalogConfig,
                        parallelism == null ? null : Integer.parseInt(parallelism),
                        params.get(WHERE),
                        includedTables,
                        excludedTables,
                        preferFileFormat,
                        cloneFrom,
                        metaOnly,
                        cloneIfExists);

        return Optional.of(cloneAction);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"clone\" clones the source files and migrate them to paimon table.");
        System.out.println();
    }
}
