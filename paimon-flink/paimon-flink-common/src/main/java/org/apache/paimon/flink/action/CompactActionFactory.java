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

import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.TimeUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Factory to create {@link CompactAction}. */
public class CompactActionFactory implements ActionFactory {

    public static final String IDENTIFIER = "compact";

    private static final String ORDER_STRATEGY = "order_strategy";
    private static final String ORDER_BY = "order_by";

    private static final String WHERE = "where";

    private static final String PARTITION_IDLE_TIME = "partition_idle_time";

    private static final String UNAWARE_APPEND_PER_TASK_DATA_SIZE =
            "unaware_append_per_task_data_size";
    private static final String BUCKETED_APPEND_PER_TASK_BUCKETS =
            "bucketed_append_per_task_buckets";
    private static final String APPEND_MAX_PARALLELISM = "append_max_parallelism";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        String database = params.getRequired(DATABASE);
        String table = params.getRequired(TABLE);
        Map<String, String> catalogConfig = catalogConfigMap(params);
        Map<String, String> tableConfig = optionalConfigMap(params, TABLE_CONF);

        CompactAction action;
        if (params.has(ORDER_STRATEGY)) {
            Preconditions.checkArgument(
                    !params.has(PARTITION_IDLE_TIME),
                    "sort compact do not support 'partition_idle_time'.");
            action =
                    new SortCompactAction(database, table, catalogConfig, tableConfig)
                            .withOrderStrategy(params.get(ORDER_STRATEGY))
                            .withOrderColumns(params.getRequired(ORDER_BY).split(","));
        } else {
            action = new CompactAction(database, table, catalogConfig, tableConfig);
            if (params.has(PARTITION_IDLE_TIME)) {
                action.withPartitionIdleTime(
                        TimeUtils.parseDuration(params.get(PARTITION_IDLE_TIME)));
            }
            String compactStrategy = params.get(COMPACT_STRATEGY);
            if (checkCompactStrategy(compactStrategy)) {
                action.withFullCompaction(compactStrategy.trim().equalsIgnoreCase(FULL));
            }
        }

        if (params.has(PARTITION)) {
            List<Map<String, String>> partitions = getPartitions(params);
            action.withPartitions(partitions);
        } else if (params.has(WHERE)) {
            action.withWhereSql(params.get(WHERE));
        }

        if (params.has(UNAWARE_APPEND_PER_TASK_DATA_SIZE)) {
            action.withUnawareAppendPerTaskDataSize(
                    MemorySize.parseBytes(params.get(UNAWARE_APPEND_PER_TASK_DATA_SIZE)));
        }

        if (params.has(BUCKETED_APPEND_PER_TASK_BUCKETS)) {
            action.withBucketedAppendPerTaskBuckets(
                    Integer.parseInt(params.get(BUCKETED_APPEND_PER_TASK_BUCKETS)));
        }

        if (params.has(APPEND_MAX_PARALLELISM)) {
            action.withAppendMaxParallelism(Integer.parseInt(params.get(APPEND_MAX_PARALLELISM)));
        }

        return Optional.of(action);
    }

    public static boolean checkCompactStrategy(String compactStrategy) {
        if (compactStrategy != null) {
            Preconditions.checkArgument(
                    compactStrategy.equalsIgnoreCase(MINOR)
                            || compactStrategy.equalsIgnoreCase(FULL),
                    String.format(
                            "The compact strategy only supports 'full' or 'minor', but '%s' is configured.",
                            compactStrategy));
            return true;
        }
        return false;
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"compact\" runs a dedicated job for compacting specified table.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  compact --warehouse <warehouse_path> --database <database_name> \n"
                        + "--table <table_name> [--partition <partition_name>] \n"
                        + "[--order_strategy <order_strategy>] \n"
                        + "[--table_conf <key>=<value>] \n"
                        + "[--order_by <order_columns>] \n"
                        + "[--partition_idle_time <partition_idle_time>] \n"
                        + "[--compact_strategy <compact_strategy>] \n"
                        + "[--append_per_task_data_size <append_per_task_data_size>] \n"
                        + "[--append_max_parallelism <append_max_parallelism>] \n");
        System.out.println(
                "  compact --warehouse s3://path/to/warehouse --database <database_name> "
                        + "--table <table_name> [--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]]");
        System.out.println();

        System.out.println("Partition name syntax:");
        System.out.println("  key1=value1,key2=value2,...");

        System.out.println();
        System.out.println("Note:");
        System.out.println(
                "  order compact now only support append-only table with bucket=-1, please don't specify --order_strategy parameter if your table does not meet the request");
        System.out.println("  order_strategy now only support zorder in batch mode");
        System.out.println(
                "partition_idle_time now can not be used with order_strategy, please don't specify --partition_idle_time when use order compact");
        System.out.println("  partition_idle_time now is only supported in batch mode");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition_idle_time 10s");
        System.out.println(
                "--compact_strategy determines how to pick files to be merged, the default is determined by the runtime execution mode. "
                        + "`full` : Only supports batch mode. All files will be selected for merging."
                        + "`minor`: Pick the set of files that need to be merged based on specified conditions.");
        System.out.println(
                "  compact --warehouse s3:///path/to/warehouse \n"
                        + "--database test_db \n"
                        + "--table test_table \n"
                        + "--order_strategy zorder \n"
                        + "--order_by a,b,c \n"
                        + "--table_conf sink.parallelism=9 \n"
                        + "--catalog_conf s3.endpoint=https://****.com \n"
                        + "--catalog_conf s3.access-key=***** \n"
                        + "--catalog_conf s3.secret-key=***** ");
    }
}
