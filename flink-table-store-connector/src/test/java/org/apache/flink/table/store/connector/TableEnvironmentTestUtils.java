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

package org.apache.flink.table.store.connector;

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** Utility class for creating {@link TableEnvironment} in tests. */
public class TableEnvironmentTestUtils {

    public static TableEnvironment create(EnvironmentSettings settings) {
        TableEnvironment tEnv = TableEnvironment.create(settings);
        disableUnalignedCheckpoint(tEnv);
        return tEnv;
    }

    public static StreamTableEnvironment create(StreamExecutionEnvironment env) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        disableUnalignedCheckpoint(tEnv);
        return tEnv;
    }

    public static StreamTableEnvironment create(
            StreamExecutionEnvironment env, EnvironmentSettings settings) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        disableUnalignedCheckpoint(tEnv);
        return tEnv;
    }

    private static void disableUnalignedCheckpoint(TableEnvironment tEnv) {
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
    }
}
