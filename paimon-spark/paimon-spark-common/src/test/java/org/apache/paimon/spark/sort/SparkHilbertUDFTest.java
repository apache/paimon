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

package org.apache.paimon.spark.sort;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SparkHilbertUDF}. */
public class SparkHilbertUDFTest {

    @Test
    void testBooleanColumnMapsNullFalseAndTrueToDistinctValues() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("spark-hilbert-udf-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            StructType schema =
                    new StructType(
                            new StructField[] {
                                new StructField("a", DataTypes.BooleanType, true, Metadata.empty())
                            });
            Dataset<Row> df =
                    spark.createDataFrame(
                            Arrays.asList(
                                    RowFactory.create(true),
                                    RowFactory.create(false),
                                    RowFactory.create((Boolean) null)),
                            schema);

            SparkHilbertUDF udf = new SparkHilbertUDF();
            List<Row> rows =
                    df.select(
                                    df.col("a"),
                                    udf.sortedLexicographically(df.col("a"), DataTypes.BooleanType)
                                            .as("hilbert"))
                            .collectAsList();

            Map<Boolean, Long> mapped = new HashMap<>();
            for (Row row : rows) {
                assertThat(row.isNullAt(1)).isFalse();
                mapped.put(row.isNullAt(0) ? null : row.getBoolean(0), row.getLong(1));
            }

            // A null boolean must reach the sentinel without unboxing, and TRUE must not share
            // it: Long.MAX_VALUE is what every type in this class uses for null.
            assertThat(mapped.get(null)).isEqualTo(Long.MAX_VALUE);
            assertThat(mapped.get(Boolean.TRUE)).isEqualTo(1L);
            assertThat(mapped.get(Boolean.FALSE)).isEqualTo(0L);
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }

    @Test
    void testTimestampNtzColumnIsSupported() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("spark-hilbert-udf-ntz-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
        try {
            StructType schema =
                    new StructType(
                            new StructField[] {
                                new StructField(
                                        "a", DataTypes.TimestampNTZType, true, Metadata.empty())
                            });
            Dataset<Row> df =
                    spark.createDataFrame(
                            Arrays.asList(
                                    RowFactory.create(LocalDateTime.of(2024, 1, 1, 0, 0, 0)),
                                    RowFactory.create(LocalDateTime.of(2024, 6, 1, 12, 0, 0)),
                                    RowFactory.create(LocalDateTime.of(2025, 1, 1, 0, 0, 0)),
                                    RowFactory.create((LocalDateTime) null)),
                            schema);

            SparkHilbertUDF udf = new SparkHilbertUDF();
            // A paimon TIMESTAMP column reads back as timestamp_ntz unless the legacy mapping is
            // on, and the dispatch used to reject it, so ordering by it threw "the type is
            // unsupported" instead of clustering.
            List<Row> rows =
                    df.select(
                                    df.col("a"),
                                    udf.sortedLexicographically(
                                                    df.col("a"), DataTypes.TimestampNTZType)
                                            .as("hilbert"))
                            .orderBy(df.col("a").asc_nulls_first())
                            .collectAsList();

            assertThat(rows).hasSize(4);
            assertThat(rows.get(0).isNullAt(0)).isTrue();
            assertThat(rows.get(0).getLong(1)).isEqualTo(Long.MAX_VALUE);

            List<Long> nonNull = new ArrayList<>();
            for (Row row : rows.subList(1, rows.size())) {
                nonNull.add(row.getLong(1));
            }
            for (int i = 1; i < nonNull.size(); i++) {
                assertThat(nonNull.get(i)).isGreaterThan(nonNull.get(i - 1));
            }
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
