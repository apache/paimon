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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.clone.CloneAndMigrateUtils;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.CatalogOptions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Clone External source files and commit metas to construct Paimon table. Currently, supports:
 *
 * <p>Hive table to paimon table.
 */
public class CloneAndMigrateAction extends ActionBase {

    private final Map<String, String> sourceCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;

    private final Map<String, String> targetCatalogConfig;
    private final String targetDatabase;
    private final String targetTableName;

    private final int parallelism;

    public CloneAndMigrateAction(
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            @Nullable Integer parallelism) {
        super(sourceCatalogConfig);
        // TODO: Currently, only hive table is supported, and we check 'metastore'
        String metastore = sourceCatalogConfig.get(CatalogOptions.METASTORE.key());
        if (!"hive".equals(metastore)) {
            throw new UnsupportedOperationException(
                    "Only support clone hive table now. Maybe forget to set 'metastore' = 'hive'?");
        }

        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.sourceCatalogConfig = sourceCatalogConfig;

        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
        this.targetCatalogConfig = targetCatalogConfig;

        this.parallelism = parallelism == null ? env.getParallelism() : parallelism;
    }

    @Override
    public void build() throws Exception {
        // list source tables
        DataStream<Tuple2<Identifier, Identifier>> source =
                CloneAndMigrateUtils.buildSource(
                        sourceDatabase,
                        sourceTableName,
                        targetDatabase,
                        targetTableName,
                        (HiveCatalog) catalog,
                        env);

        DataStream<Tuple2<Identifier, Identifier>> partitionedSource =
                FlinkStreamPartitioner.partition(
                        source, new CloneAndMigrateUtils.TableChannelComputer(), parallelism);

        // create target table, list files and group by <table, partition>
        DataStream<List<CloneAndMigrateUtils.MigrateFilesInfo>> files =
                partitionedSource
                        .process(
                                CloneAndMigrateUtils.createTargetTableAndListFilesFunction(
                                        sourceCatalogConfig, targetCatalogConfig))
                        .name("List Files")
                        .setParallelism(parallelism);

        // copy files and commit
        DataStream<Void> committed =
                files.forward()
                        .process(
                                CloneAndMigrateUtils.copyAndCommitFunction(
                                        sourceCatalogConfig, targetCatalogConfig))
                        .name("Copy and Commit")
                        .setParallelism(parallelism);

        // commit
        committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone And Migrate job");
    }
}
