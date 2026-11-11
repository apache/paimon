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

package org.apache.paimon.spark.globalindex.sorted;

import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises adaptive planning and both shuffle paths with real local Spark jobs. */
class NullBitmapAdaptiveTest {

    @Test
    void testAdaptivePlanningPreservesRowsAndPartitionOrder() throws Exception {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("null-bitmap-adaptive")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .config("spark.shuffle.sort.bypassMergeThreshold", "0")
                        .config("spark.sql.execution.rangeExchange.sampleSizePerPartition", "10")
                        .getOrCreate();
        try {
            for (int percent : new int[] {0, 2, 50, 79, 80, 90, 100}) {
                verify(
                        input(spark, 1000, percent, "CAST(id % 37 AS INT)", "INT"),
                        4,
                        true,
                        percent >= 80);
            }
            String[][] keys = {
                {
                    "concat('key-', repeat('x', CAST(id % 31 AS INT)), CAST(id % 37 AS STRING))",
                    "STRING"
                },
                {"CAST((id % 37 - 18) / 10.0 AS DECIMAL(12, 2))", "DECIMAL(12, 2)"},
                {"date_add(DATE '2020-01-01', CAST(id % 37 AS INT))", "DATE"}
            };
            for (String[] key : keys) {
                for (int percent : new int[] {2, 90}) {
                    verify(input(spark, 1000, percent, key[0], key[1]), 4, true, percent >= 80);
                }
            }
            verify(input(spark, 0, 0, "CAST(id AS INT)", "INT"), 4, true, false);
            verify(input(spark, 1000, 100, "CAST(id AS INT)", "INT"), 1, true, false);
            verify(input(spark, 1000, 100, "CAST(id AS INT)", "INT"), 4, false, false);
            verify(skewedInput(spark), 4, true, false);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    private static Dataset<Row> input(
            SparkSession spark, int count, int percent, String key, String type) {
        return spark.range(0, count, 1, 4)
                .selectExpr(
                        "CAST(pmod(id, 3) AS BIGINT) AS task_id",
                        "CASE WHEN pmod(id * 31, 100) < "
                                + percent
                                + " THEN CAST(NULL AS "
                                + type
                                + ") ELSE "
                                + key
                                + " END AS index_key",
                        "id AS `" + SpecialFields.ROW_ID.name() + "`");
    }

    private static Dataset<Row> skewedInput(SparkSession spark) {
        JavaRDD<Row> rows =
                spark.range(0, 10, 1, 10)
                        .javaRDD()
                        .mapPartitionsWithIndex(
                                (partition, ignored) -> {
                                    List<Row> source = new ArrayList<>();
                                    int count =
                                            partition == 0 ? 7900 : (partition <= 3 ? 234 : 233);
                                    for (int i = 0; i < count; i++) {
                                        source.add(
                                                RowFactory.create(
                                                        (long) (i % 3),
                                                        partition == 0 ? null : i % 37,
                                                        partition * 10000L + i));
                                    }
                                    return source.iterator();
                                },
                                false);
        StructType schema =
                new StructType()
                        .add("task_id", DataTypes.LongType, false)
                        .add("index_key", DataTypes.IntegerType, true)
                        .add(SpecialFields.ROW_ID.name(), DataTypes.LongType, false);
        // The 7,900-null source is oversized for sampling and may be scanned again.
        return spark.createDataFrame(rows, schema);
    }

    private static void verify(
            Dataset<Row> input, int partitions, boolean candidate, boolean expectedCompression)
            throws Exception {
        NullBitmapShuffle.BuildPlan plan = NullBitmapShuffle.plan(input, partitions, candidate);
        assertThat(plan.compress).isEqualTo(expectedCompression);
        Partitioner transferred =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(plan.partitioner),
                        Thread.currentThread().getContextClassLoader());
        List<List<Row>> actual =
                partitionRows(NullBitmapShuffle.sort(input, transferred, plan.compress));
        List<List<Row>> baseline =
                partitionRows(NullBitmapShuffle.sort(input, plan.partitioner, false));
        assertThat(actual).hasSize(plan.partitioner.numPartitions());
        assertThat(actual).isEqualTo(baseline);

        List<Row> restored = new ArrayList<>();
        for (List<Row> partition : actual) {
            restored.addAll(partition);
        }
        List<Row> source = new ArrayList<>(input.collectAsList());
        Comparator<Row> byRowId = Comparator.comparingLong(row -> row.getLong(2));
        restored.sort(byRowId);
        source.sort(byRowId);
        assertThat(restored).isEqualTo(source);
    }

    private static List<List<Row>> partitionRows(Dataset<Row> sorted) {
        return sorted.javaRDD()
                .mapPartitions(
                        rows -> {
                            List<Row> partition = new ArrayList<>();
                            rows.forEachRemaining(partition::add);
                            return Collections.singletonList(partition).iterator();
                        })
                .collect();
    }
}
