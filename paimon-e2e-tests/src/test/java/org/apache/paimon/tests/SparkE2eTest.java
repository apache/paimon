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

package org.apache.paimon.tests;

import org.apache.paimon.annotation.VisibleForTesting;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.condition.JRE.JAVA_11;

/** Tests for reading paimon from Spark3. */
@DisabledOnJre(JAVA_11)
public class SparkE2eTest extends E2eReaderTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkE2eTest.class);

    /**
     * Start of a Spark ERROR log line in the default log4j2 pattern ({@code %d{yy/MM/dd HH:mm:ss}
     * %p %c{1}: %m%n%ex}), which spark-sql writes to stdout. Anchoring on the timestamp prefix
     * avoids matching an "ERROR" word inside a result row.
     */
    private static final Pattern SPARK_ERROR_LOG_LINE =
            Pattern.compile("(?m)^\\d{2}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR ");

    public SparkE2eTest() {
        super(false, false, true);
    }

    @Test
    public void testFlinkWriteAndSparkRead() throws Exception {
        String warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID() + "_warehouse";
        final String table = "T";
        final String sparkTable = String.format("paimon.default.%s", table);
        runBatchSql(
                String.join(
                        "\n",
                        createCatalogSql("my_spark", warehousePath),
                        createTableSql(table),
                        createInsertSql(table)));
        checkQueryResults(sparkTable, sql -> executeSparkSql(warehousePath, sql));
    }

    @Test
    public void testFlinkCreateAndSparkReadChangelogEventMetadata() throws Exception {
        String warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID() + "_warehouse";
        final String table = "event_metadata";
        final String sparkTable = String.format("paimon.default.%s", table);

        runBatchSql(
                String.join(
                        "\n",
                        createCatalogSql("my_flink", warehousePath),
                        "CREATE TABLE "
                                + table
                                + " ("
                                + "  id INT,"
                                + "  data INT,"
                                + "  event_ts BIGINT,"
                                + "  PRIMARY KEY (id) NOT ENFORCED"
                                + ") WITH ("
                                + "  'bucket' = '1',"
                                + "  'changelog-producer' = 'lookup',"
                                + "  'sequence.field' = 'event_ts',"
                                + "  'changelog-producer.expose-field-as-metadata' = 'event_ts'"
                                + ");",
                        "INSERT INTO " + table + " VALUES (1, 10, 50);",
                        "INSERT INTO " + table + " VALUES (1, 20, 100);"));

        // Flink created the table without a METADATA FROM alias. Spark reads the generated field
        // using the physical metadata name stored in the table properties.
        checkQueryResult(
                sql -> executeSparkSql(warehousePath, sql),
                "SELECT id, data, event_ts, __internal__event_ts FROM "
                        + sparkTable
                        + " ORDER BY id",
                "1\t20\t100\t100\n");
    }

    private String executeSparkSql(String warehousePath, String sqlFile) throws Exception {
        Container.ExecResult execResult =
                getSpark()
                        .execInContainer(
                                "/spark/bin/spark-sql",
                                "--master",
                                "spark://spark-master:7077",
                                "--conf",
                                "spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
                                "--conf",
                                "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog",
                                "--conf",
                                "spark.sql.catalog.paimon.warehouse=file:" + warehousePath,
                                "-f",
                                TEST_DATA_DIR + "/" + sqlFile);
        if (execResult.getExitCode() != 0) {
            LOG.info(execResult.getStdout());
            LOG.info(execResult.getStderr());
            throw new AssertionError("Failed when running spark sql.");
        }
        String stdout = stripTrailingSparkErrorLogs(execResult.getStdout());
        return Arrays.stream(stdout.split("\n"))
                        .filter(s -> !s.contains("WARN"))
                        .collect(Collectors.joining("\n"))
                + "\n";
    }

    /**
     * Drops everything from the first Spark ERROR log line onwards. When spark-sql exits, the
     * driver shutdown races with RPC dispatch and may log after the query result, with exit code 0,
     * e.g. {@code ERROR Utils: Uncaught exception in thread dispatcher-CoarseGrainedScheduler} or
     * {@code ERROR TransportRequestHandler: Error while invoking RpcHandler#receive() for one-way
     * message.} followed by a stack trace.
     */
    @VisibleForTesting
    static String stripTrailingSparkErrorLogs(String stdout) {
        Matcher matcher = SPARK_ERROR_LOG_LINE.matcher(stdout);
        return matcher.find() ? stdout.substring(0, matcher.start()) : stdout;
    }

    private ContainerState getSpark() {
        return environment.getContainerByServiceName("spark-master-1").get();
    }
}
