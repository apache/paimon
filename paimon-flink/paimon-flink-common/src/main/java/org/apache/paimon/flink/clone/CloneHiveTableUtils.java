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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CloneAction;
import org.apache.paimon.flink.clone.files.CloneFileInfo;
import org.apache.paimon.flink.clone.files.CloneFilesCommitOperator;
import org.apache.paimon.flink.clone.files.CloneFilesFunction;
import org.apache.paimon.flink.clone.files.DataFileInfo;
import org.apache.paimon.flink.clone.files.ListCloneFilesFunction;
import org.apache.paimon.flink.clone.files.ShuffleDataFileByTableComputer;
import org.apache.paimon.flink.clone.schema.CloneHiveSchemaFunction;
import org.apache.paimon.flink.clone.schema.CloneSchemaInfo;
import org.apache.paimon.flink.clone.spits.CloneSplitInfo;
import org.apache.paimon.flink.clone.spits.CloneSplitsFunction;
import org.apache.paimon.flink.clone.spits.CommitMessageInfo;
import org.apache.paimon.flink.clone.spits.CommitMessageTableOperator;
import org.apache.paimon.flink.clone.spits.ListCloneSplitsFunction;
import org.apache.paimon.flink.clone.spits.ShuffleCommitMessageByTableComputer;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.utils.StringUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Utils for building {@link CloneAction} for append tables. */
public class CloneHiveTableUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CloneHiveTableUtils.class);

    public static DataStream<Tuple2<Identifier, Identifier>> buildSource(
            String sourceDatabase,
            String sourceTableName,
            String targetDatabase,
            String targetTableName,
            Catalog sourceCatalog,
            @Nullable List<String> includedTables,
            @Nullable List<String> excludedTables,
            StreamExecutionEnvironment env)
            throws Exception {
        List<Tuple2<Identifier, Identifier>> result = new ArrayList<>();
        HiveCatalog hiveCatalog = getRootHiveCatalog(sourceCatalog);
        if (StringUtils.isNullOrWhitespaceOnly(sourceDatabase)) {
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(sourceTableName),
                    "sourceTableName must be blank when database is null.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must be blank when clone all tables in a catalog.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier :
                    org.apache.paimon.hive.clone.HiveCloneUtils.listTables(
                            hiveCatalog, includedTables, excludedTables)) {
                result.add(new Tuple2<>(identifier, identifier));
            }
        } else if (StringUtils.isNullOrWhitespaceOnly(sourceTableName)) {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone all tables in a database.");
            checkArgument(
                    StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must be blank when clone all tables in a catalog.");

            for (Identifier identifier :
                    org.apache.paimon.hive.clone.HiveCloneUtils.listTables(
                            hiveCatalog, sourceDatabase, includedTables, excludedTables)) {
                result.add(
                        new Tuple2<>(
                                identifier,
                                Identifier.create(targetDatabase, identifier.getObjectName())));
            }
        } else {
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetDatabase),
                    "targetDatabase must not be blank when clone a table.");
            checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(targetTableName),
                    "targetTableName must not be blank when clone a table.");
            checkArgument(
                    CollectionUtils.isEmpty(includedTables),
                    "includedTables must be empty when clone a single table.");
            checkArgument(
                    CollectionUtils.isEmpty(excludedTables),
                    "excludedTables must be empty when clone a single table.");
            result.add(
                    new Tuple2<>(
                            Identifier.create(sourceDatabase, sourceTableName),
                            Identifier.create(targetDatabase, targetTableName)));
        }

        checkState(!result.isEmpty(), "Didn't find any table in source catalog.");

        if (LOG.isDebugEnabled()) {
            LOG.debug("The clone identifiers of source table and target table are: {}", result);
        }
        return env.fromCollection(result).forceNonParallel();
    }

    public static HiveCatalog getRootHiveCatalog(Catalog catalog) {
        Catalog rootCatalog = DelegateCatalog.rootCatalog(catalog);
        checkArgument(
                rootCatalog instanceof HiveCatalog,
                "Only support HiveCatalog now but found %s.",
                rootCatalog.getClass().getName());
        return (HiveCatalog) rootCatalog;
    }

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
            boolean metaOnly)
            throws Exception {
        // list source tables
        DataStream<Tuple2<Identifier, Identifier>> source =
                buildSource(
                        sourceDatabase,
                        sourceTableName,
                        targetDatabase,
                        targetTableName,
                        sourceCatalog,
                        includedTables,
                        excludedTables,
                        env);

        DataStream<Tuple2<Identifier, Identifier>> partitionedSource =
                FlinkStreamPartitioner.partition(
                        source, new ShuffleIdentifierByTableComputer(), parallelism);

        // create target table, check support clone splits
        DataStream<CloneSchemaInfo> schemaInfos =
                partitionedSource
                        .process(
                                new CloneHiveSchemaFunction(
                                        sourceCatalogConfig, targetCatalogConfig, preferFileFormat))
                        .name("Clone Schema")
                        .setParallelism(parallelism);

        // if metaOnly is true, only clone schema and skip data cloning
        if (metaOnly) {
            schemaInfos.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
            return;
        }

        buildForCloneSplits(
                sourceCatalogConfig, targetCatalogConfig, parallelism, whereSql, schemaInfos);

        buildForCloneFile(
                sourceCatalogConfig, targetCatalogConfig, parallelism, whereSql, schemaInfos);
    }

    public static void buildForCloneSplits(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            int parallelism,
            @Nullable String whereSql,
            DataStream<CloneSchemaInfo> schemaInfos) {

        // list splits
        DataStream<CloneSplitInfo> splits =
                schemaInfos
                        .filter(cloneSchemaInfo -> cloneSchemaInfo.supportCloneSplits())
                        .rebalance()
                        .process(
                                new ListCloneSplitsFunction(
                                        sourceCatalogConfig, targetCatalogConfig, whereSql))
                        .name("List Splits")
                        .setParallelism(parallelism);

        // copy splits and commit
        DataStream<CommitMessageInfo> commitMessage =
                splits.rebalance()
                        .process(new CloneSplitsFunction(sourceCatalogConfig, targetCatalogConfig))
                        .name("Copy Splits")
                        .setParallelism(parallelism);

        DataStream<CommitMessageInfo> partitionedCommitMessage =
                FlinkStreamPartitioner.partition(
                        commitMessage, new ShuffleCommitMessageByTableComputer(), parallelism);

        DataStream<Long> committed =
                partitionedCommitMessage
                        .transform(
                                "Commit Table Splits",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new CommitMessageTableOperator(targetCatalogConfig))
                        .setParallelism(parallelism);
        committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void buildForCloneFile(
            Map<String, String> sourceCatalogConfig,
            Map<String, String> targetCatalogConfig,
            int parallelism,
            @Nullable String whereSql,
            DataStream<CloneSchemaInfo> schemaInfos) {
        // list files and group by <table, partition>
        DataStream<CloneFileInfo> files =
                schemaInfos
                        .filter(cloneSchemaInfo -> !cloneSchemaInfo.supportCloneSplits())
                        .rebalance()
                        .process(
                                new ListCloneFilesFunction(
                                        sourceCatalogConfig, targetCatalogConfig, whereSql))
                        .name("List Files")
                        .setParallelism(parallelism);

        // copy files and commit
        DataStream<DataFileInfo> dataFile =
                files.rebalance()
                        .process(new CloneFilesFunction(sourceCatalogConfig, targetCatalogConfig))
                        .name("Copy Files")
                        .setParallelism(parallelism);

        DataStream<DataFileInfo> partitionedDataFile =
                FlinkStreamPartitioner.partition(
                        dataFile, new ShuffleDataFileByTableComputer(), parallelism);

        DataStream<Long> committed =
                partitionedDataFile
                        .transform(
                                "Commit Table Files",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new CloneFilesCommitOperator(targetCatalogConfig))
                        .setParallelism(parallelism);
        committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }
}
