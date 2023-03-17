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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;

/** Utility methods for {@link StreamExecutionEnvironment}. */
public class StreamExecutionEnvironmentUtils {

    public static ReadableConfig getConfiguration(StreamExecutionEnvironment env) {
        // For Flink 1.14 we have to use reflection to get configuration from execution environment.
        // See FLINK-26709 for more info.
        if (env.getClass()
                .getName()
                .equals("org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment")) {
            try {
                Field realExecEnvField = env.getClass().getDeclaredField("realExecEnv");
                realExecEnvField.setAccessible(true);
                env = (StreamExecutionEnvironment) realExecEnvField.get(env);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(
                        "Failed to get realExecEnv from DummyStreamExecutionEnvironment "
                                + "by Java reflection. This is unexpected.",
                        e);
            }
        }
        return env.getConfiguration();
    }
}
