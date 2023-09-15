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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CompactAction}. */
public class CompactActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterTool params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);

        Map<String, String> catalogConfig = optionalConfigMap(params, "catalog-conf");

        CompactAction action;
        if (params.has("order-strategy")) {
            SortCompactAction sortCompactAction =
                    new SortCompactAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);

            String strategy = params.get("order-strategy");
            sortCompactAction.withOrderStrategy(strategy);

            if (params.has("order-by")) {
                String sqlOrderBy = params.get("order-by");
                if (sqlOrderBy == null) {
                    throw new IllegalArgumentException("Please specify \"order-by\".");
                }
                sortCompactAction.withOrderColumns(Arrays.asList(sqlOrderBy.split(",")));
            } else {
                throw new IllegalArgumentException(
                        "Please specify order columns in parameter --order-by.");
            }

            action = sortCompactAction;
        } else {
            action = new CompactAction(tablePath.f0, tablePath.f1, tablePath.f2, catalogConfig);
        }

        if (params.has("partition")) {
            List<Map<String, String>> partitions = getPartitions(params);
            action.withPartitions(partitions);
        }

        if (params.has(ExecutionOptions.RUNTIME_MODE.key())) {
            RuntimeExecutionMode runtimeExecutionMode =
                    RuntimeExecutionMode.valueOf(params.get(ExecutionOptions.RUNTIME_MODE.key()));
            action.withRuntimeMode(runtimeExecutionMode);
        }

        return Optional.of(action);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"compact\" runs a dedicated job for compacting specified table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> [--partition <partition-name>]"
                        + "[--order-strategy <order-strategy>]"
                        + "[--order-by <order-columns>]");
        System.out.println(
                "  compact --warehouse s3://path/to/warehouse --database <database-name> "
                        + "--table <table-name> [--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]]");
        System.out.println("  compact --path <table-path> [--partition <partition-name>]");
        System.out.println();

        System.out.println("Partition name syntax:");
        System.out.println("  key1=value1,key2=value2,...");

        System.out.println();
        System.out.println("Note:");
        System.out.println(
                "  order compact now only support append-only table with bucket=-1, please don't specify --order-strategy parameter if your table does not meet the request");
        System.out.println("  order-strategy now only support zorder in batch mode");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table");
        System.out.println(
                "  compact --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126,hh=08");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
        System.out.println(
                "  compact --warehouse s3:///path/to/warehouse "
                        + "--database test_db "
                        + "--table test_table "
                        + "--order-strategy zorder "
                        + "--order-by a,b,c "
                        + "--catalog-conf s3.endpoint=https://****.com "
                        + "--catalog-conf s3.access-key=***** "
                        + "--catalog-conf s3.secret-key=***** ");
    }
}
