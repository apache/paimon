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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SparkZOrderUDF}. */
public class SparkZOrderUDFTest {

    @Test
    void testBooleanColumnKeepsFalseOffTheNullSentinel() {
        SparkSession spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("spark-zorder-udf-test")
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

            SparkZOrderUDF udf = new SparkZOrderUDF(1, 8, Integer.MAX_VALUE);
            List<Row> rows =
                    df.select(
                                    df.col("a"),
                                    // The UDF hands back a per-column buffer it reuses for every
                                    // row, so the bytes are turned into hex inside Spark rather
                                    // than collected as arrays that all alias one another.
                                    functions
                                            .hex(
                                                    udf.sortedLexicographically(
                                                            df.col("a"), DataTypes.BooleanType))
                                            .as("zvalue"))
                            .collectAsList();

            Map<Boolean, String> mapped = new HashMap<>();
            for (Row row : rows) {
                mapped.put(row.isNullAt(0) ? null : row.getBoolean(0), row.getString(1));
            }

            // NULL is the all-zero sentinel, so FALSE has to be something else, and TRUE keeps
            // the high bit that puts it above both in unsigned order.
            assertThat(mapped.get(null)).isEqualTo("0000000000000000");
            assertThat(mapped.get(Boolean.FALSE)).isEqualTo("0100000000000000");
            assertThat(mapped.get(Boolean.TRUE)).isEqualTo("8100000000000000");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
