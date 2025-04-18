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
import org.apache.paimon.flink.clone.hive.CloneFileInfo;
import org.apache.paimon.flink.clone.hive.CloneHiveUtils;
import org.apache.paimon.flink.clone.hive.CommitTableOperator;
import org.apache.paimon.flink.clone.hive.CopyHiveFilesFunction;
import org.apache.paimon.flink.clone.hive.DataFileInfo;
import org.apache.paimon.flink.clone.hive.ListHiveFilesFunction;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.options.CatalogOptions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import javax.annotation.Nullable;

import java.util.Map;

/** Clone source files managed by HiveMetaStore and commit metas to construct Paimon table. */
public class CloneHiveAction extends ActionBase {

    private final Map<String, String> sourceCatalogConfig;
    private final String sourceDatabase;
    private final String sourceTableName;

    private final Map<String, String> targetCatalogConfig;
    private final String targetDatabase;
    private final String targetTableName;

    private final int parallelism;
    @Nullable private final String whereSql;

    public CloneHiveAction(
            String sourceDatabase,
            String sourceTableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            @Nullable Integer parallelism,
            @Nullable String whereSql) {
        super(sourceCatalogConfig);
        String metastore = sourceCatalogConfig.get(CatalogOptions.METASTORE.key());
        if (!"hive".equals(metastore)) {
            throw new UnsupportedOperationException(
                    "Only support clone hive table. Maybe you forget to set --catalog_conf metastore=hive ?");
        }

        this.sourceDatabase = sourceDatabase;
        this.sourceTableName = sourceTableName;
        this.sourceCatalogConfig = sourceCatalogConfig;

        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
        this.targetCatalogConfig = targetCatalogConfig;

        this.parallelism = parallelism == null ? env.getParallelism() : parallelism;
        this.whereSql = whereSql;
    }

    @Override
    public void build() throws Exception {
        // list source tables
        DataStream<Tuple2<Identifier, Identifier>> source =
                CloneHiveUtils.buildSource(
                        sourceDatabase,
                        sourceTableName,
                        targetDatabase,
                        targetTableName,
                        catalog,
                        env);

        DataStream<Tuple2<Identifier, Identifier>> partitionedSource =
                FlinkStreamPartitioner.partition(
                        source, new CloneHiveUtils.TableChannelComputer(), parallelism);

        // create target table, list files and group by <table, partition>
        DataStream<CloneFileInfo> files =
                partitionedSource
                        .process(
                                new ListHiveFilesFunction(
                                        sourceCatalogConfig, targetCatalogConfig, whereSql))
                        .name("List Files")
                        .setParallelism(parallelism);

        // copy files and commit
        DataStream<DataFileInfo> dataFile =
                files.rebalance()
                        .process(
                                new CopyHiveFilesFunction(sourceCatalogConfig, targetCatalogConfig))
                        .name("Copy Files")
                        .setParallelism(parallelism);

        DataStream<DataFileInfo> partitionedDataFile =
                FlinkStreamPartitioner.partition(
                        dataFile, new CloneHiveUtils.DataFileChannelComputer(), parallelism);

        DataStream<Long> committed =
                partitionedDataFile
                        .transform(
                                "Commit table",
                                BasicTypeInfo.LONG_TYPE_INFO,
                                new CommitTableOperator(targetCatalogConfig))
                        .setParallelism(parallelism);
        committed.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone Hive job");
    }
}
