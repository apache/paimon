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

import org.apache.spark.Partitioner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class NullBitmapShuffleSerializerTest {
    @Test
    void testTransportDoesNotRequireApplicationKryoRegistrations() {
        for (String serializer :
                new String[] {
                    "org.apache.spark.serializer.JavaSerializer",
                    "org.apache.spark.serializer.KryoSerializer"
                }) {
            for (boolean strict : new boolean[] {false, true}) {
                SparkSession spark =
                        SparkSession.builder()
                                .master("local[2]")
                                .appName("null-bitmap-serializer")
                                .config("spark.ui.enabled", "false")
                                .config("spark.sql.adaptive.enabled", "false")
                                .config("spark.serializer", serializer)
                                .config("spark.kryo.registrationRequired", strict)
                                .getOrCreate();
                try {
                    for (boolean strings : new boolean[] {false, true}) {
                        for (int nullPercent : new int[] {0, 90, 100}) {
                            String key =
                                    strings
                                            ? "concat('value-', CAST(id % 100 AS STRING))"
                                            : "CAST(id % 100 AS INT)";
                            Dataset<Row> input =
                                    spark.range(0, 10000, 1, 4)
                                            .selectExpr(
                                                    "CAST(0 AS BIGINT) AS task_id",
                                                    "CASE WHEN id % 100 < "
                                                            + nullPercent
                                                            + " THEN NULL ELSE "
                                                            + key
                                                            + " END AS index_key",
                                                    "id AS `" + SpecialFields.ROW_ID.name() + "`");
                            Partitioner ranges = NullBitmapShuffle.createPartitioner(input, 4);
                            check(
                                    SortedIndexTopoBuilder.combineAndSortBuildTaskInputs(
                                            Collections.singletonList(input),
                                            4,
                                            "task_id",
                                            "index_key"),
                                    nullPercent,
                                    strings);
                            check(
                                    NullBitmapShuffle.sort(input, ranges, false),
                                    nullPercent,
                                    strings);
                            check(
                                    NullBitmapShuffle.sort(input, ranges, true),
                                    nullPercent,
                                    strings);
                            check(NullBitmapShuffle.sort(input, 4, true), nullPercent, strings);
                        }
                    }
                } finally {
                    spark.stop();
                    SparkSession.clearActiveSession();
                    SparkSession.clearDefaultSession();
                }
            }
        }
    }

    private static void check(Dataset<Row> result, int nullPercent, boolean strings) {
        long count = 0;
        // Return only registered primitive wrappers; collecting external Rows would test
        // result serialization as well as the exchange under review.
        for (Long partitionCount :
                result.javaRDD()
                        .mapPartitions(
                                rows -> {
                                    long n = 0;
                                    while (rows.hasNext()) {
                                        Row row = rows.next();
                                        long id = row.getLong(2);
                                        boolean expectedNull = id % 100 < nullPercent;
                                        if (id < 0
                                                || id >= 10000
                                                || row.isNullAt(1) != expectedNull) {
                                            throw new IllegalStateException(
                                                    "Invalid index row: " + id);
                                        }
                                        if (!expectedNull
                                                && !(strings
                                                        ? ("value-" + id % 100)
                                                                .equals(row.getString(1))
                                                        : row.getInt(1) == id % 100)) {
                                            throw new IllegalStateException(
                                                    "Invalid index key: " + id);
                                        }
                                        n++;
                                    }
                                    return Collections.singletonList(n).iterator();
                                })
                        .collect()) {
            count += partitionCount;
        }
        assertThat(count).isEqualTo(10000);
    }
}
