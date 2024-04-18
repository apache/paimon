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

import org.apache.paimon.clone.CloneFileInfo;
import org.apache.paimon.clone.CloneTypeAction;
import org.apache.paimon.flink.clone.CheckCloneResultAndCopySnapshotOperator;
import org.apache.paimon.flink.clone.CloneFileInfoTypeInfo;
import org.apache.paimon.flink.clone.CopyFileOperator;
import org.apache.paimon.flink.clone.PickFilesForCloneBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StringUtils.isBlank;

/** Define some common funtion for different clone actions. */
public abstract class AbstractCloneAction extends TableActionBase {

    protected String parallelismStr;
    protected CloneTypeAction cloneTypeAction;

    public AbstractCloneAction(
            String warehouse, Map<String, String> catalogConfig, String parallelismStr) {
        super(warehouse, catalogConfig);
        this.parallelismStr = parallelismStr;
    }

    public AbstractCloneAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String parallelismStr) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.parallelismStr = parallelismStr;
    }

    protected FileStoreTable checkTableAndCast(Table table) {
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "table type error. The table type is '%s'.",
                            table.getClass().getName()));
        }
        return (FileStoreTable) table;
    }

    protected void checkFlinkRuntimeParameters() {
        checkArgument(
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.BATCH,
                "Clone only support batch mode yet. Please add -Dexecution.runtime-mode=BATCH.");
    }

    protected void buildFlinkBatchJob(
            FileStoreTable sourceFileStoreTable, FileStoreTable targetFileStoreTable) {
        Pair<DataStream<CloneFileInfo>, Integer> pickFilesForClone =
                new PickFilesForCloneBuilder(env, sourceFileStoreTable, cloneTypeAction).build();

        SingleOutputStreamOperator<CloneFileInfo> copyFiles =
                pickFilesForClone
                        .getLeft()
                        .transform(
                                "Copy Files",
                                new CloneFileInfoTypeInfo(),
                                new CopyFileOperator(sourceFileStoreTable, targetFileStoreTable))
                        .setParallelism(
                                isBlank(parallelismStr)
                                        ? env.getParallelism()
                                        : Integer.parseInt(parallelismStr));

        SingleOutputStreamOperator<CloneFileInfo> checkCloneResult =
                copyFiles
                        .transform(
                                "Check Copy Files Result",
                                new CloneFileInfoTypeInfo(),
                                new CheckCloneResultAndCopySnapshotOperator(
                                        sourceFileStoreTable,
                                        targetFileStoreTable,
                                        cloneTypeAction,
                                        pickFilesForClone.getRight()))
                        .setParallelism(1)
                        .setMaxParallelism(1);

        checkCloneResult.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }
}
