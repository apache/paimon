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

import org.apache.paimon.spark.globalindex.sorted.SortedIndexTopoBuilder.BuildTaskLookup;
import org.apache.paimon.spark.globalindex.sorted.SortedIndexTopoBuilder.SortedBuildTask;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.Range;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortedIndexTopoBuilder}. */
public class SortedIndexTopoBuilderTest {

    private static final String TASK_ID = "task_id";
    private static final String INDEX_KEY = "index_key";

    @Test
    void testBuildTopologyAcrossPartitions() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("sorted-index-topology-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            Dataset<Row> input = taskInput(spark, 0);

            Dataset<Row> topology =
                    SortedIndexTopoBuilder.sortBuildTaskInput(input, 3, TASK_ID, INDEX_KEY, false);

            assertThat(topology.queryExecution().logical()).isInstanceOf(Sort.class);
            Sort sort = (Sort) topology.queryExecution().logical();
            assertThat(sort.global()).isFalse();
            assertThat(sort.order().size()).isEqualTo(3);
            assertThat(sort.child()).isInstanceOf(RepartitionByExpression.class);

            RepartitionByExpression repartition = (RepartitionByExpression) sort.child();
            assertThat(repartition.numPartitions()).isEqualTo(3);
            assertThat(repartition.shuffle()).isTrue();
            assertThat(repartition.partitioning()).isInstanceOf(RangePartitioning.class);
            assertThat(repartition.partitionExpressions().size()).isEqualTo(3);
            assertThat(topology.queryExecution().executedPlan().toString())
                    .contains("rangepartitioning");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void testBuildTaskLookupForDisjointRanges() {
        List<SortedBuildTask> tasks =
                Arrays.asList(
                        new SortedBuildTask(0, new Range(9, 10), new byte[0]),
                        new SortedBuildTask(1, new Range(0, 6), new byte[0]),
                        new SortedBuildTask(2, new Range(11, 11), new byte[0]));
        BuildTaskLookup lookup = BuildTaskLookup.create(tasks);
        assertThat(lookup).isNotNull();
        assertThat(lookup.call(0L)).isEqualTo(1L);
        assertThat(lookup.call(6L)).isEqualTo(1L);
        assertThat(lookup.call(9L)).isEqualTo(0L);
        assertThat(lookup.call(10L)).isEqualTo(0L);
        assertThat(lookup.call(11L)).isEqualTo(2L);
        assertThatThrownBy(() -> lookup.call(8L)).isInstanceOf(IllegalStateException.class);
        assertThat(
                        BuildTaskLookup.create(
                                Arrays.asList(
                                        tasks.get(1),
                                        new SortedBuildTask(3, new Range(6, 7), new byte[0]))))
                .isNull();
    }

    @Test
    void testCalculateParallelismByTotalRowsInsteadOfRangeCount() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 10L, i * 10L + 9), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 4096)).isEqualTo(1);
    }

    @Test
    void testCalculateParallelismHonorsMaxParallelism() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new SortedBuildTask(i, new Range(i * 1000L, i * 1000L + 999), new byte[0]));
        }

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(16);
    }

    @Test
    void testCalculateParallelismKeepsSingleRangeBehavior() {
        List<SortedBuildTask> tasks = new ArrayList<>();
        tasks.add(new SortedBuildTask(0, new Range(0, 1499), new byte[0]));

        assertThat(SortedIndexTopoBuilder.calculateParallelism(tasks, 1000L, 16)).isEqualTo(1);
    }

    private static Dataset<Row> taskInput(SparkSession spark, long taskId) {
        return spark.range(1)
                .select(
                        functions.lit(taskId).cast("long").alias(TASK_ID),
                        functions.lit(taskId).alias(INDEX_KEY),
                        functions.col("id").alias(SpecialFields.ROW_ID.name()));
    }
}
