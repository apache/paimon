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

package org.apache.paimon.flink.clone.history;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.clone.FullHistoryCloneMarker;
import org.apache.paimon.clone.FullHistoryClonePlan;
import org.apache.paimon.clone.FullHistoryClonePlanner;
import org.apache.paimon.clone.FullHistoryCopyPlan;
import org.apache.paimon.clone.PathMapping;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.StringUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for building full-history physical clone jobs for Paimon tables. */
public class ClonePaimonFullHistoryTableUtils {

    public static void build(
            StreamExecutionEnvironment env,
            Catalog sourceCatalog,
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            int parallelism,
            @Nullable String whereSql,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            @Nullable String preferFileFormat,
            @Nullable List<String> pathMappings,
            boolean metaOnly,
            boolean cloneIfExists)
            throws Exception {
        validateFullHistoryOptions(
                sourceDatabase,
                sourceTableName,
                targetDatabase,
                targetTableName,
                whereSql,
                includedTables,
                excludedTables,
                preferFileFormat,
                metaOnly);

        Table table = sourceCatalog.getTable(Identifier.create(sourceDatabase, sourceTableName));
        checkArgument(
                table instanceof FileStoreTable,
                "Full-history Paimon clone only supports FileStoreTable, but the source is %s.",
                table.getClass().getName());
        FileStoreTable sourceTable = (FileStoreTable) table;
        PathMapping mapping = PathMapping.parse(pathMappings);
        FullHistoryClonePlan clonePlan =
                new FullHistoryClonePlanner(sourceTable, mapping).planStructure();

        try (FileIO targetFileIO =
                FullHistoryFileIOUtils.createResolvingFileIO(
                        clonePlan.targetRoot(), targetCatalogConfig)) {
            FullHistoryCloneMarker.prepare(
                    targetFileIO,
                    clonePlan,
                    mapping,
                    targetDatabase,
                    targetTableName,
                    cloneIfExists);
        }

        TypeInformation<FullHistoryCopyPlan.FileCopy> copyType =
                TypeInformation.of(FullHistoryCopyPlan.FileCopy.class);
        DataStream<Boolean> copyCompleted =
                env.fromCollection(Collections.singletonList(1), TypeInformation.of(Integer.class))
                        .name("Plan Full-History Clone Files")
                        .setParallelism(1)
                        .process(
                                new PlanFullHistoryFilesFunction(
                                        sourceCatalogConfig,
                                        sourceDatabase,
                                        sourceTableName,
                                        pathMappings,
                                        clonePlan.sourceFingerprint(),
                                        clonePlan))
                        .returns(copyType)
                        .name("Discover Full-History Clone Files")
                        .setParallelism(1)
                        .keyBy(file -> file.target().toString())
                        .transform(
                                "Copy Full-History Files",
                                TypeInformation.of(Boolean.class),
                                new CopyFullHistoryFileOperator(
                                        sourceCatalogConfig,
                                        targetCatalogConfig,
                                        sourceDatabase,
                                        sourceTableName))
                        .setParallelism(parallelism);

        DataStream<Boolean> result =
                copyCompleted
                        .transform(
                                "Publish and Validate Full-History Clone",
                                TypeInformation.of(Boolean.class),
                                new FullHistoryCloneFinalizeOperator(
                                        sourceCatalogConfig,
                                        targetCatalogConfig,
                                        sourceDatabase,
                                        sourceTableName,
                                        targetDatabase,
                                        targetTableName,
                                        pathMappings,
                                        clonePlan))
                        .forceNonParallel();
        result.sinkTo(new DiscardingSink<>()).name("Full-History Clone Result").setParallelism(1);
    }

    public static void validateFullHistoryOptions(
            @Nullable String sourceDatabase,
            @Nullable String sourceTableName,
            @Nullable String targetDatabase,
            @Nullable String targetTableName,
            @Nullable String whereSql,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            @Nullable String preferFileFormat,
            boolean metaOnly) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(sourceDatabase)
                        && !StringUtils.isNullOrWhitespaceOnly(sourceTableName),
                "Full-history Paimon clone only supports cloning a single table in the first "
                        + "version.");
        checkArgument(
                StringUtils.isNullOrWhitespaceOnly(whereSql),
                "The where option is not supported for full-history Paimon clone.");
        checkArgument(
                CollectionUtils.isEmpty(includedTables),
                "included_tables is not supported when cloning a single table with full-history "
                        + "Paimon clone.");
        checkArgument(
                CollectionUtils.isEmpty(excludedTables),
                "excluded_tables is not supported when cloning a single table with full-history "
                        + "Paimon clone.");
        checkArgument(
                StringUtils.isNullOrWhitespaceOnly(preferFileFormat),
                "prefer_file_format is not supported for full-history Paimon clone.");
        checkArgument(!metaOnly, "meta_only is not supported for full-history Paimon clone.");
    }

    private ClonePaimonFullHistoryTableUtils() {}
}
