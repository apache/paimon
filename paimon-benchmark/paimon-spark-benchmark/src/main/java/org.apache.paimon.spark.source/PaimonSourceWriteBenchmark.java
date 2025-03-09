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

package org.apache.paimon.spark.source;

import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.SparkSQLProperties;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import static org.apache.spark.sql.functions.expr;

/**
 * Paimon source spark write benchmark.
 *
 * <p>Usage:
 *
 * <p>mvn clean install -pl ':paimon-spark-benchmark' -DskipTests=true
 *
 * <p>java -jar ./paimon-benchmark/paimon-spark-benchmark/target/paimon-spark-benchmark.jar
 * org.apache.paimon.spark.source.PaimonSourceWriteBenchmark -o
 * benchmark/paimon-source-write-result.txt
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class PaimonSourceWriteBenchmark {
    public static final String PAIMON_TABLE_NAME = "paimon_table";
    private static final int NUM_ROWS = 3_000_000;

    private final String warehousePath = warehousePath();

    private SparkSession spark;

    protected void setup() {
        spark =
                SparkSession.builder()
                        .config("spark.ui.enabled", false)
                        .master("local")
                        .config("spark.sql.catalog.paimon", SparkCatalog.class.getName())
                        .config("spark.sql.catalog.paimon.warehouse", warehousePath)
                        .getOrCreate();

        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");

        spark.sql(
                String.format(
                        "CREATE TABLE %s ("
                                + "intCol INT NOT NULL, "
                                + "longCol BIGINT NOT NULL, "
                                + "floatCol FLOAT NOT NULL, "
                                + "doubleCol DOUBLE NOT NULL, "
                                + "decimalCol DECIMAL(20, 5) NOT NULL, "
                                + "stringCol1 STRING NOT NULL, "
                                + "stringCol2 STRING NOT NULL, "
                                + "stringCol3 STRING NOT NULL"
                                + ") using paimon "
                                + "TBLPROPERTIES('primary-key'='intCol,stringCol2', 'bucket'='2')",
                        PAIMON_TABLE_NAME));
    }

    protected String warehousePath() {
        Path warehosuePath = new Path("/tmp", "paimon-warehouse-" + UUID.randomUUID());
        return warehosuePath.toString();
    }

    @Setup
    public void setupBenchmark() {
        setup();
    }

    @TearDown
    public void tearDown() {
        File warehouseDir = new File(warehousePath);
        deleteFile(warehouseDir);
    }

    public static boolean deleteFile(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteFile(child);
                }
            }
        }
        return file.delete();
    }

    @Benchmark
    @Threads(1)
    public void v1Write() throws NoSuchTableException {
        benchmarkData().writeTo(PAIMON_TABLE_NAME).append();
    }

    @Benchmark
    @Threads(1)
    public void v2Write() {
        Map<String, String> conf = ImmutableMap.of(SparkSQLProperties.USE_V2_WRITE, "true");
        withSQLConf(
                conf,
                () -> {
                    try {
                        benchmarkData().writeTo(PAIMON_TABLE_NAME).append();
                    } catch (NoSuchTableException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private Dataset<Row> benchmarkData() {
        return spark.range(NUM_ROWS)
                .withColumn("intCol", expr("CAST(id AS INT)"))
                .withColumn("longCol", expr("CAST(id AS LONG)"))
                .withColumn("floatCol", expr("CAST(id AS FLOAT)"))
                .withColumn("doubleCol", expr("CAST(id AS DOUBLE)"))
                .withColumn("decimalCol", expr("CAST(id AS DECIMAL(20, 5))"))
                .withColumn("stringCol1", expr("CAST(id AS STRING)"))
                .withColumn("stringCol2", expr("CAST(id AS STRING)"))
                .withColumn("stringCol3", expr("CAST(id AS STRING)"))
                .drop("id")
                .coalesce(1);
    }

    private void withSQLConf(Map<String, String> sparkSqlConf, Action action) {
        SQLConf sqlConf = SQLConf.get();

        Map<String, String> currentConfValues = Maps.newHashMap();
        sparkSqlConf
                .keySet()
                .forEach(
                        confKey -> {
                            if (sqlConf.contains(confKey)) {
                                String currentConfValue = sqlConf.getConfString(confKey);
                                currentConfValues.put(confKey, currentConfValue);
                            }
                        });

        sparkSqlConf.forEach(
                (confKey, confValue) -> {
                    if (SQLConf.isStaticConfigKey(confKey)) {
                        throw new RuntimeException(
                                "Cannot modify the value of a static config: " + confKey);
                    }
                    sqlConf.setConfString(confKey, confValue);
                });
        try {
            action.invoke();
        } finally {
            sparkSqlConf.forEach(
                    (confKey, confValue) -> {
                        if (currentConfValues.containsKey(confKey)) {
                            sqlConf.setConfString(confKey, currentConfValues.get(confKey));
                        } else {
                            sqlConf.unsetConf(confKey);
                        }
                    });
        }
    }

    /** Action functional interface. */
    @FunctionalInterface
    public interface Action {
        void invoke();
    }
}
