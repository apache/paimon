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

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

/** Tests for reading paimon from Spark3. */
public class SparkE2eTest extends E2eReaderTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SparkE2eTest.class);

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
        checkQueryResults(
                sparkTable,
                sql -> {
                    Container.ExecResult execResult =
                            getSpark()
                                    .execInContainer(
                                            "/spark/bin/spark-sql",
                                            "--master",
                                            "spark://spark-master:7077",
                                            "--conf",
                                            "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog",
                                            "--conf",
                                            "spark.sql.catalog.paimon.warehouse=file:"
                                                    + warehousePath,
                                            "-f",
                                            TEST_DATA_DIR + "/" + sql);
                    if (execResult.getExitCode() != 0) {
                        LOG.info(execResult.getStdout());
                        LOG.info(execResult.getStderr());
                        throw new AssertionError("Failed when running spark sql.");
                    }
                    return Arrays.stream(execResult.getStdout().split("\n"))
                                    .filter(s -> !s.contains("WARN"))
                                    .collect(Collectors.joining("\n"))
                            + "\n";
                });
    }

    private ContainerState getSpark() {
        return environment.getContainerByServiceName("spark-master_1").get();
    }
}
