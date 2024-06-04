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

import org.apache.paimon.flink.clone.CloneFileInfo;
import org.apache.paimon.flink.clone.CloneSourceBuilder;
import org.apache.paimon.flink.clone.CopyFileOperator;
import org.apache.paimon.flink.clone.PickFilesForCloneOperator;
import org.apache.paimon.flink.clone.SnapshotHintChannelComputer;
import org.apache.paimon.flink.clone.SnapshotHintOperator;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.options.CatalogOptions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.StringUtils.isBlank;

/** The Latest Snapshot clone action for Flink. */
public class CloneAction extends ActionBase {

    private final int parallelism;

    private Map<String, String> sourceCatalogConfig;
    private final String database;
    private final String tableName;

    private Map<String, String> targetCatalogConfig;
    private final String targetDatabase;
    private final String targetTableName;

    public CloneAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> sourceCatalogConfig,
            String targetWarehouse,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            String parallelismStr) {
        super(warehouse, sourceCatalogConfig);

        checkNotNull(warehouse, "warehouse must not be null.");
        checkNotNull(targetWarehouse, "targetWarehouse must not be null.");

        this.parallelism =
                isBlank(parallelismStr) ? env.getParallelism() : Integer.parseInt(parallelismStr);

        this.sourceCatalogConfig = new HashMap<>();
        if (!sourceCatalogConfig.isEmpty()) {
            this.sourceCatalogConfig = sourceCatalogConfig;
        }
        this.sourceCatalogConfig.put(CatalogOptions.WAREHOUSE.key(), warehouse);
        this.database = database;
        this.tableName = tableName;

        this.targetCatalogConfig = new HashMap<>();
        if (!targetCatalogConfig.isEmpty()) {
            this.targetCatalogConfig = targetCatalogConfig;
        }
        this.targetCatalogConfig.put(CatalogOptions.WAREHOUSE.key(), targetWarehouse);
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    @Override
    public void build() {
        try {
            buildCloneFlinkJob(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void buildCloneFlinkJob(StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple2<String, String>> cloneSource =
                new CloneSourceBuilder(
                                env,
                                sourceCatalogConfig,
                                database,
                                tableName,
                                targetDatabase,
                                targetTableName)
                        .build();

        SingleOutputStreamOperator<CloneFileInfo> pickFilesForClone =
                cloneSource
                        .transform(
                                "Pick Files",
                                TypeInformation.of(CloneFileInfo.class),
                                new PickFilesForCloneOperator(
                                        sourceCatalogConfig, targetCatalogConfig))
                        .forceNonParallel();

        SingleOutputStreamOperator<CloneFileInfo> copyFiles =
                pickFilesForClone
                        .rebalance()
                        .transform(
                                "Copy Files",
                                TypeInformation.of(CloneFileInfo.class),
                                new CopyFileOperator(sourceCatalogConfig, targetCatalogConfig))
                        .setParallelism(parallelism);

        SingleOutputStreamOperator<CloneFileInfo> snapshotHintOperator =
                FlinkStreamPartitioner.partition(
                                copyFiles, new SnapshotHintChannelComputer(), parallelism)
                        .transform(
                                "Recreate Snapshot Hint",
                                TypeInformation.of(CloneFileInfo.class),
                                new SnapshotHintOperator(targetCatalogConfig))
                        .setParallelism(parallelism);

        snapshotHintOperator.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Clone job");
    }
}
