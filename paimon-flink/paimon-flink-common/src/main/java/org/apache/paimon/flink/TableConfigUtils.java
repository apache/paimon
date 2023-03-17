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

package org.apache.paimon.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;

/** Utils for {@link TableConfig}. */
public class TableConfigUtils {

    public static Configuration extractConfiguration(ReadableConfig readableConfig) {
        Configuration to = new Configuration();
        copyConfiguration(readableConfig, to);
        return to;
    }

    private static void copyConfiguration(ReadableConfig from, Configuration to) {
        if (from instanceof Configuration) {
            to.addAll((Configuration) from);
            return;
        }

        if (!(from instanceof TableConfig)) {
            throw new RuntimeException("Unknown readableConfig type: " + from.getClass());
        }

        TableConfig tableConfig = (TableConfig) from;

        // copy root configuration first
        copyConfiguration(tableConfig.getRootConfiguration(), to);

        // copy table configuration
        to.addAll(tableConfig.getConfiguration());
    }
}
