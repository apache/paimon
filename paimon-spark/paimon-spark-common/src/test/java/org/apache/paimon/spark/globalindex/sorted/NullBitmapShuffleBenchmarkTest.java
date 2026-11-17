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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/** Measures the actual Spark exchange, including Spark's own shuffle compression. */
class NullBitmapShuffleBenchmarkTest {

    @Test
    void testShuffleCostAndPartitionRotation() throws Exception {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[2]")
                        .appName("null-bitmap-shuffle-comparison")
                        .config("spark.ui.enabled", "false")
                        .config("spark.sql.adaptive.enabled", "false")
                        .getOrCreate();
        AtomicLong[] metrics = new AtomicLong[6];
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = new AtomicLong();
        }
        spark.sparkContext()
                .addSparkListener(
                        new SparkListener() {
                            @Override
                            public void onTaskEnd(SparkListenerTaskEnd event) {
                                if (event.taskMetrics() != null) {
                                    metrics[0].addAndGet(
                                            event.taskMetrics()
                                                    .shuffleWriteMetrics()
                                                    .recordsWritten());
                                    metrics[1].addAndGet(
                                            event.taskMetrics()
                                                    .shuffleWriteMetrics()
                                                    .bytesWritten());
                                    metrics[2].addAndGet(event.taskMetrics().executorCpuTime());
                                    metrics[3].addAndGet(event.taskMetrics().jvmGCTime());
                                    metrics[4].addAndGet(event.taskMetrics().memoryBytesSpilled());
                                    metrics[5].addAndGet(event.taskMetrics().diskBytesSpilled());
                                }
                            }
                        });
        try {
            for (int scenario : new int[] {0, 2, 10, 50, 79, 80, 90, 100, -1}) {
                boolean variableWidth = scenario == -1;
                int percent = variableWidth ? 0 : scenario;
                int rowCount = percent <= 2 ? 2000000 : 200000;
                String key =
                        variableWidth
                                ? "concat(repeat('prefix-', 16), CAST(id % 1000 AS STRING))"
                                : "CAST(id % 1000 AS INT)";
                String keyType = variableWidth ? "STRING" : "INT";
                Dataset<Row> input =
                        spark.range(0, rowCount, 1, 4)
                                .selectExpr(
                                        "CAST(0 AS BIGINT) AS task_id",
                                        "CASE WHEN pmod(id * 31, 100) < "
                                                + percent
                                                + " THEN CAST(NULL AS "
                                                + keyType
                                                + ") ELSE "
                                                + key
                                                + " END AS index_key",
                                        "id AS `" + SpecialFields.ROW_ID.name() + "`");
                // Warm both paths; alternate order so the baseline does not always pay first.
                if (percent == 0 || percent == 80) {
                    measure(spark, input, false, metrics, percent, rowCount);
                    measure(spark, input, true, metrics, percent, rowCount);
                }
                long[][][] samples = new long[2][3][];
                for (int repeat = 0; repeat < 3; repeat++) {
                    for (int order = 0; order < 2; order++) {
                        int variant = (repeat + order) % 2;
                        samples[variant][repeat] =
                                measure(spark, input, variant == 1, metrics, percent, rowCount);
                    }
                }
                long[][] measured = new long[2][8];
                for (int variant = 0; variant < 2; variant++) {
                    for (int metric = 0; metric < 8; metric++) {
                        long[] values = new long[3];
                        for (int repeat = 0; repeat < 3; repeat++) {
                            values[repeat] = samples[variant][repeat][metric];
                        }
                        Arrays.sort(values);
                        measured[variant][metric] = values[1];
                    }
                }
                System.out.printf(
                        "NULL_ADAPTIVE percent=%d keyType=%s rows=%d baseline=%s adaptive=%s [records,bytes,rotationFiles,ms,cpuMs,gcMs,memorySpill,diskSpill]%n",
                        percent,
                        keyType,
                        rowCount,
                        Arrays.toString(measured[0]),
                        Arrays.toString(measured[1]));
                // Independent sampling has natural layout variance; the real-file comparison
                // uses identical sampled boundaries to verify file-count and read-cost invariance.
                if (percent >= 80) {
                    assertThat(measured[1][0]).isLessThan(measured[0][0]);
                    assertThat(measured[1][1]).isLessThan(measured[0][1]);
                } else {
                    assertThat(measured[1][0]).isEqualTo(measured[0][0]);
                    assertThat(measured[1][1]).isLessThanOrEqualTo((long) (measured[0][1] * 1.01));
                }
            }
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    private static long[] measure(
            SparkSession spark,
            Dataset<Row> input,
            boolean adaptive,
            AtomicLong[] metrics,
            int percent,
            int rowCount)
            throws Exception {
        spark.sparkContext().listenerBus().waitUntilEmpty(10000);
        for (AtomicLong metric : metrics) {
            metric.set(0);
        }
        long start = System.nanoTime();
        Dataset<Row> sorted =
                SortedIndexTopoBuilder.combineAndSortBuildTaskInputs(
                        Collections.singletonList(input), 4, "task_id", "index_key", adaptive);
        List<long[]> partitions =
                sorted.javaRDD()
                        .mapPartitions(
                                rows -> {
                                    Iterator<Row> expanded = NullBitmapShuffle.expandRows(rows);
                                    long count = 0;
                                    long sum = 0;
                                    long nulls = 0;
                                    while (expanded.hasNext()) {
                                        Row row = expanded.next();
                                        count++;
                                        sum += row.getLong(2);
                                        nulls += row.isNullAt(1) ? 1 : 0;
                                    }
                                    return Collections.singletonList(new long[] {count, sum, nulls})
                                            .iterator();
                                })
                        .collect();
        long elapsed = (System.nanoTime() - start) / 1000000;
        spark.sparkContext().listenerBus().waitUntilEmpty(10000);
        long[] totals = new long[4];
        for (long[] partition : partitions) {
            for (int i = 0; i < 3; i++) {
                totals[i] += partition[i];
            }
            totals[3] += (partition[0] + 11999) / 12000;
        }
        assertThat(totals[0]).isEqualTo(rowCount);
        assertThat(totals[1]).isEqualTo((long) rowCount * (rowCount - 1) / 2);
        assertThat(totals[2]).isEqualTo((long) percent * rowCount / 100);
        return new long[] {
            metrics[0].get(),
            metrics[1].get(),
            totals[3],
            elapsed,
            metrics[2].get() / 1000000,
            metrics[3].get(),
            metrics[4].get(),
            metrics[5].get()
        };
    }
}
