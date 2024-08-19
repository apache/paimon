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

import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.TimeUtils;

import org.apache.flink.api.java.tuple.Tuple3;

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

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Tuple3<String, String, String> tablePath = getTablePath(params);

        Map<String, String> catalogConfig = optionalConfigMap(params, CATALOG_CONF);

        CompactAction action;
        if (params.has(ORDER_STRATEGY)) {
            Preconditions.checkArgument(
                    !params.has(PARTITION_IDLE_TIME),
                    "sort compact do not support 'partition_idle_time'.");
            action =
                    new SortCompactAction(
                                    tablePath.f0,
                                    tablePath.f1,
                                    tablePath.f2,
                                    catalogConfig,
                                    optionalConfigMap(params, TABLE_CONF))
                            .withOrderStrategy(params.get(ORDER_STRATEGY))
                            .withOrderColumns(getRequiredValue(params, ORDER_BY).split(","));
        } else {
            action =
                    new CompactAction(
                            tablePath.f0,
                            tablePath.f1,
                            tablePath.f2,
                            catalogConfig,
                            optionalConfigMap(params, TABLE_CONF));
            if (params.has(PARTITION_IDLE_TIME)) {
                action.withPartitionIdleTime(
                        TimeUtils.parseDuration(params.get(PARTITION_IDLE_TIME)));
            }
        }

        if (params.has(PARTITION)) {
            List<Map<String, String>> partitions = getPartitions(params);
            action.withPartitions(partitions);
        } else if (params.has(WHERE)) {
            action.withWhereSql(params.get(WHERE));
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
                "  compact --warehouse <warehouse_path> --database <database_name> "
                        + "--table <table_name> [--partition <partition_name>]"
                        + "[--order_strategy <order_strategy>]"
                        + "[--table_conf <key>=<value>]"
                        + "[--order_by <order_columns>]"
                        + "[--partition_idle_time <partition_idle_time>]");
        System.out.println(
                "  compact --warehouse s3://path/to/warehouse --database <database_name> "
                        + "--table <table_name> [--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]]");
        System.out.println("  compact --path <table_path> [--partition <partition_name>]");
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
                "  compact --path hdfs:///path/to/warehouse/test_db.db/test_table --partition dt=20221126,hh=08");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition dt=20221126,hh=08 --partition dt=20221127,hh=09");
        System.out.println(
                "  compact --warehouse hdfs:///path/to/warehouse --database test_db --table test_table "
                        + "--partition_idle_time 10s");
        System.out.println(
                "  compact --warehouse s3:///path/to/warehouse "
                        + "--database test_db "
                        + "--table test_table "
                        + "--order_strategy zorder "
                        + "--order_by a,b,c "
                        + "--table_conf sink.parallelism=9 "
                        + "--catalog_conf s3.endpoint=https://****.com "
                        + "--catalog_conf s3.access-key=***** "
                        + "--catalog_conf s3.secret-key=***** ");
    }
}
