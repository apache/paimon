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

import org.apache.paimon.flink.copy.CopyDataFileOperator;
import org.apache.paimon.flink.copy.CopyFileInfo;
import org.apache.paimon.flink.copy.CopyManifestFileOperator;
import org.apache.paimon.flink.copy.CopyMetaFilesFunction;
import org.apache.paimon.flink.copy.CopySourceBuilder;
import org.apache.paimon.flink.copy.SnapshotHintChannelComputer;
import org.apache.paimon.flink.copy.SnapshotHintOperator;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/**
 * The Latest Snapshot copy files action for Flink.
 *
 * @deprecated The normal process should commit a snapshot to the catalog, but this action does not
 *     do so. Currently, this action can only be applied to the FileSystemCatalog.
 */
@Deprecated
public class CopyFilesAction extends ActionBase {

    private final int parallelism;

    private Map<String, String> sourceCatalogConfig;
    private final String database;
    private final String tableName;

    private Map<String, String> targetCatalogConfig;
    private final String targetDatabase;
    private final String targetTableName;

    public CopyFilesAction(
            String database,
            String tableName,
            Map<String, String> sourceCatalogConfig,
            String targetDatabase,
            String targetTableName,
            Map<String, String> targetCatalogConfig,
            String parallelismStr) {
        super(sourceCatalogConfig);

        this.parallelism =
                isNullOrWhitespaceOnly(parallelismStr)
                        ? env.getParallelism()
                        : Integer.parseInt(parallelismStr);

        this.sourceCatalogConfig = new HashMap<>();
        if (!sourceCatalogConfig.isEmpty()) {
            this.sourceCatalogConfig = sourceCatalogConfig;
        }
        this.database = database;
        this.tableName = tableName;

        this.targetCatalogConfig = new HashMap<>();
        if (!targetCatalogConfig.isEmpty()) {
            this.targetCatalogConfig = targetCatalogConfig;
        }
        this.targetDatabase = targetDatabase;
        this.targetTableName = targetTableName;
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    @Override
    public void build() {
        try {
            buildCopyFilesFlinkJob(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void buildCopyFilesFlinkJob(StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple2<String, String>> source =
                new CopySourceBuilder(
                                env,
                                sourceCatalogConfig,
                                database,
                                tableName,
                                targetDatabase,
                                targetTableName)
                        .build();

        SingleOutputStreamOperator<Void> copyMetaFiles =
                source.forward()
                        .process(
                                new CopyMetaFilesFunction(sourceCatalogConfig, targetCatalogConfig))
                        .name("Side Output")
                        .setParallelism(1);

        DataStream<CopyFileInfo> indexFilesStream =
                copyMetaFiles.getSideOutput(CopyMetaFilesFunction.INDEX_FILES_TAG);
        DataStream<CopyFileInfo> dataManifestFilesStream =
                copyMetaFiles.getSideOutput(CopyMetaFilesFunction.DATA_MANIFEST_FILES_TAG);

        SingleOutputStreamOperator<CopyFileInfo> copyIndexFiles =
                indexFilesStream
                        .transform(
                                "Copy Index Files",
                                TypeInformation.of(CopyFileInfo.class),
                                new CopyDataFileOperator(sourceCatalogConfig, targetCatalogConfig))
                        .setParallelism(parallelism);

        SingleOutputStreamOperator<CopyFileInfo> copyDataManifestFiles =
                dataManifestFilesStream
                        .transform(
                                "Copy Data Manifest Files",
                                TypeInformation.of(CopyFileInfo.class),
                                new CopyManifestFileOperator(
                                        sourceCatalogConfig, targetCatalogConfig))
                        .setParallelism(parallelism);

        SingleOutputStreamOperator<CopyFileInfo> copyDataFile =
                copyDataManifestFiles
                        .transform(
                                "Copy Data Files",
                                TypeInformation.of(CopyFileInfo.class),
                                new CopyDataFileOperator(sourceCatalogConfig, targetCatalogConfig))
                        .setParallelism(parallelism);

        DataStream<CopyFileInfo> combinedStream = copyDataFile.union(copyIndexFiles);

        SingleOutputStreamOperator<CopyFileInfo> snapshotHintOperator =
                FlinkStreamPartitioner.partition(
                                combinedStream, new SnapshotHintChannelComputer(), parallelism)
                        .transform(
                                "Recreate Snapshot Hint",
                                TypeInformation.of(CopyFileInfo.class),
                                new SnapshotHintOperator(targetCatalogConfig))
                        .setParallelism(parallelism);

        snapshotHintOperator.sinkTo(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Copy Files job");
    }
}
