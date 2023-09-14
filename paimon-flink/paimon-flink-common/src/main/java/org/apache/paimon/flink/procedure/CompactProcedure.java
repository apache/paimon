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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.CompactAction;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.table.procedures.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.ActionFactory.parseCommaSeparatedKeyValues;

/**
 * Compact procedure. Usage:
 *
 * <pre><code>
 *  -- compact a table (tableId should be 'database_name.table_name')
 *  CALL compact(tableId)
 *
 *  -- compact specific partitions ('pt1=A,pt2=a', 'pt1=B,pt2=b', ...)
 *  CALL compact(tableId, partition1, partition2, ...)
 * </code></pre>
 */
public class CompactProcedure implements Procedure {

    private final String warehouse;
    private final Map<String, String> catalogOptions;

    public CompactProcedure(String warehouse, Map<String, String> catalogOptions) {
        this.warehouse = warehouse;
        this.catalogOptions = catalogOptions;
    }

    public String[] call(ProcedureContext procedureContext, String tableId) throws Exception {
        return call(procedureContext, tableId, new String[0]);
    }

    public String[] call(
            ProcedureContext procedureContext, String tableId, String... partitionStrings)
            throws Exception {
        Identifier identifier = Identifier.fromString(tableId);
        CompactAction action =
                new CompactAction(
                        warehouse,
                        identifier.getDatabaseName(),
                        identifier.getObjectName(),
                        catalogOptions);

        if (partitionStrings.length != 0) {
            List<Map<String, String>> partitions = new ArrayList<>();
            for (String partition : partitionStrings) {
                partitions.add(parseCommaSeparatedKeyValues(partition));
            }
            action.withPartitions(partitions);
        }

        StreamExecutionEnvironment env = procedureContext.getExecutionEnvironment();
        action.build(env);

        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        String name = conf.getOptional(PipelineOptions.NAME).orElse("Compact job");
        if (conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING) {
            JobClient jobClient = env.executeAsync(name);
            return new String[] {"JobID=" + jobClient.getJobID()};
        } else {
            env.execute(name);
            return new String[] {"Success"};
        }
    }
}
