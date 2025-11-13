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

package org.apache.paimon.flink.utils;

import org.apache.paimon.flink.orphan.FlinkOrphanFilesClean;
import org.apache.paimon.operation.CleanOrphanFilesResult;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkState;

public class OrphanFilesCleanUtil {

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkOrphanFilesClean.class);

    public static void configureFlinkEnvironment(
            StreamExecutionEnvironment env, Integer parallelism) {
        Configuration flinkConf = new Configuration();
        flinkConf.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConf.set(ExecutionOptions.SORT_INPUTS, false);
        flinkConf.set(ExecutionOptions.USE_BATCH_STATE_BACKEND, false);
        if (parallelism != null) {
            flinkConf.set(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        }
        // Flink 1.17 introduced this config, use string to keep compatibility
        flinkConf.setString("execution.batch.adaptive.auto-parallelism.enabled", "false");
        env.configure(flinkConf);
    }

    public static boolean endInputForDeleted(
            int inputId,
            boolean buildEnd,
            long emittedFilesCount,
            long emittedFilesLen,
            Output<StreamRecord<CleanOrphanFilesResult>> output) {
        switch (inputId) {
            case 1:
                checkState(!buildEnd, "Should not build ended.");
                LOG.info("Finish build phase.");
                buildEnd = true;
                break;
            case 2:
                checkState(buildEnd, "Should build ended.");
                LOG.info("Finish probe phase.");
                LOG.info("Clean files count : {}", emittedFilesCount);
                LOG.info("Clean files size : {}", emittedFilesLen);
                output.collect(
                        new StreamRecord<>(
                                new CleanOrphanFilesResult(emittedFilesCount, emittedFilesLen)));
                break;
        }
        return buildEnd;
    }
}
