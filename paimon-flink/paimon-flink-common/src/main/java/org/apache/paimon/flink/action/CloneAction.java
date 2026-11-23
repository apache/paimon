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

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.clone.CloneFileFormatUtils;
import org.apache.paimon.flink.clone.CloneHiveTableUtils;
import org.apache.paimon.flink.clone.ClonePaimonTableUtils;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Clone source table to target table. */
public class CloneAction extends ActionBase {

    private final Map<String, String> sourceCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;

    private final Map<String, String> targetCatalogConfig;
    private final Map<String, String> targetTableConfig;
    private final String targetDatabase;
    private final String targetTableName;

    private final int parallelism;
    @Nullable private final String whereSql;
    @Nullable private final List<String> includedTables;
    @Nullable private final List<String> excludedTables;
    @Nullable private final String preferFileFormat;
    private final String cloneFrom;
    private final boolean metaOnly;
    private final boolean cloneIfExists;
    private final boolean copyFiles;
    private final boolean recursiveListing;

    public CloneAction(
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            Map<String, String> targetTableConfig,
            @Nullable Integer parallelism,
            @Nullable String whereSql,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            @Nullable String preferFileFormat,
            String cloneFrom,
            boolean metaOnly,
            boolean cloneIfExists) {
        this(
                sourceDatabase,
                sourceTableName,
                sourceCatalogConfig,
                targetDatabase,
                targetTableName,
                targetCatalogConfig,
                targetTableConfig,
                parallelism,
                whereSql,
                includedTables,
                excludedTables,
                preferFileFormat,
                cloneFrom,
                metaOnly,
                cloneIfExists,
                false,
                false);
    }

    public CloneAction(
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            Map<String, String> targetTableConfig,
            @Nullable Integer parallelism,
            @Nullable String whereSql,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            @Nullable String preferFileFormat,
            String cloneFrom,
            boolean metaOnly,
            boolean cloneIfExists,
            boolean copyFiles,
            boolean recursiveListing) {
        super(sourceCatalogConfig);

        if ((copyFiles || recursiveListing) && !cloneFrom.equalsIgnoreCase("hive")) {
            throw new UnsupportedOperationException(
                    "Parameters 'copy_files' and 'recursive_listing' are only supported when clone_from is hive.");
        }
        if (recursiveListing && !copyFiles) {
            throw new IllegalArgumentException(
                    "Parameter 'recursive_listing' requires 'copy_files' to be true.");
        }
        if (copyFiles && !StringUtils.isNullOrWhitespaceOnly(preferFileFormat)) {
            throw new IllegalArgumentException(
                    "Parameter 'prefer_file_format' can not be used with 'copy_files', copied files keep their format.");
        }

        if (cloneFrom.equalsIgnoreCase("hive")) {
            if (!targetTableConfig.isEmpty()) {
                throw new UnsupportedOperationException(
                        "Parameter 'target_table_conf' is only supported when clone_from is paimon.");
            }

            Catalog sourceCatalog = catalog;
            if (sourceCatalog instanceof CachingCatalog) {
                sourceCatalog = ((CachingCatalog) sourceCatalog).wrapped();
            }
            if (!(sourceCatalog instanceof HiveCatalog)) {
                throw new UnsupportedOperationException(
                        "Only support clone hive tables using HiveCatalog, but current source catalog is "
                                + sourceCatalog.getClass().getName());
            }
        }

        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.sourceCatalogConfig = sourceCatalogConfig;

        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
        this.targetCatalogConfig = targetCatalogConfig;
        this.targetTableConfig = targetTableConfig;

        this.parallelism = parallelism == null ? env.getParallelism() : parallelism;
        this.whereSql = whereSql;
        this.includedTables = includedTables;
        this.excludedTables = excludedTables;
        CloneFileFormatUtils.validateFileFormat(preferFileFormat);
        this.preferFileFormat =
                StringUtils.isNullOrWhitespaceOnly(preferFileFormat)
                        ? preferFileFormat
                        : preferFileFormat.toLowerCase(Locale.ROOT);
        this.cloneFrom = cloneFrom;
        this.metaOnly = metaOnly;
        this.cloneIfExists = cloneIfExists;
        this.copyFiles = copyFiles;
        this.recursiveListing = recursiveListing;
    }

    @Override
    public void build() throws Exception {
        switch (cloneFrom) {
            case "hive":
                CloneHiveTableUtils.build(
                        env,
                        catalog,
                        sourceDatabase,
                        sourceTableName,
                        sourceCatalogConfig,
                        targetDatabase,
                        targetTableName,
                        targetCatalogConfig,
                        parallelism,
                        whereSql,
                        includedTables,
                        excludedTables,
                        preferFileFormat,
                        metaOnly,
                        cloneIfExists,
                        copyFiles,
                        recursiveListing);
                break;
            case "paimon":
                ClonePaimonTableUtils.build(
                        env,
                        catalog,
                        sourceDatabase,
                        sourceTableName,
                        sourceCatalogConfig,
                        targetDatabase,
                        targetTableName,
                        targetCatalogConfig,
                        targetTableConfig,
                        parallelism,
                        whereSql,
                        includedTables,
                        excludedTables,
                        preferFileFormat,
                        metaOnly,
                        cloneIfExists);
                break;
        }
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone job");
    }

    private void validateFileFormat(String preferFileFormat) {}
}
