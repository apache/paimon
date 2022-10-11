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

package org.apache.flink.table.store.tests;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;

import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for reading table store from Spark3. */
@DisabledIfSystemProperty(named = "flink.version", matches = "1.14.*")
public class SparkE2eTest extends E2eTestBase {

    public SparkE2eTest() {
        super(false, false, true);
    }

    @Test
    public void testFlinkWriteAndSparkRead() throws Exception {
        String warehousePath = TEST_DATA_DIR + "/" + UUID.randomUUID().toString() + "_warehouse";
        String sql =
                String.join(
                        "\n",
                        "CREATE CATALOG my_spark WITH (",
                        "  'type' = 'table-store',",
                        "  'warehouse' = '" + warehousePath + "'",
                        ");",
                        "",
                        "USE CATALOG my_spark;",
                        "",
                        "CREATE TABLE T (",
                        "  a int,",
                        "  b bigint,",
                        "  c string",
                        ") WITH (",
                        "  'bucket' = '2'",
                        ");",
                        "",
                        "INSERT INTO T VALUES "
                                + "(1, 10, 'Hi'), "
                                + "(1, 100, 'Hi Again'), "
                                + "(2, 20, 'Hello'), "
                                + "(3, 30, 'Table'), "
                                + "(4, 40, 'Store');");
        runSql(sql);

        checkQueryResult(
                "SELECT * FROM tablestore.default.T ORDER BY b;",
                "1\t10\tHi\n"
                        + "2\t20\tHello\n"
                        + "3\t30\tTable\n"
                        + "4\t40\tStore\n"
                        + "1\t100\tHi Again\n",
                warehousePath);
        checkQueryResult(
                "SELECT b, a FROM tablestore.default.T ORDER BY b;",
                "10\t1\n" + "20\t2\n" + "30\t3\n" + "40\t4\n" + "100\t1\n",
                warehousePath);
        checkQueryResult(
                "SELECT * FROM tablestore.default.T WHERE a > 1 ORDER BY b;",
                "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n",
                warehousePath);
        checkQueryResult(
                "SELECT a, SUM(b), MIN(c) FROM tablestore.default.T GROUP BY a ORDER BY a;",
                "1\t110\tHi\n" + "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n",
                warehousePath);
        checkQueryResult(
                "SELECT T1.a, T1.b, T2.b FROM tablestore.default.T T1 JOIN tablestore.default.T T2 "
                        + "ON T1.a = T2.a WHERE T1.a <= 2 ORDER BY T1.a, T1.b, T2.b;",
                "1\t10\t10\n" + "1\t10\t100\n" + "1\t100\t10\n" + "1\t100\t100\n" + "2\t20\t20\n",
                warehousePath);
    }

    private void checkQueryResult(String query, String expected, String warehousePath)
            throws Exception {
        writeSharedFile("pk.hql", query);
        Container.ExecResult execResult =
                getSpark()
                        .execInContainer(
                                "/spark/bin/spark-sql",
                                "--master",
                                "spark://spark-master:7077",
                                "--conf",
                                "spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog",
                                "--conf",
                                "spark.sql.catalog.tablestore.warehouse=file:" + warehousePath,
                                "-f",
                                TEST_DATA_DIR + "/pk.hql");
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when running spark sql.");
        }
        assertThat(filterLog(execResult.getStdout())).isEqualTo(expected);
    }

    private String filterLog(String result) {
        return StringUtils.join(
                Arrays.stream(StringUtils.splitByWholeSeparator(result, "\n"))
                        .filter(v -> !StringUtils.contains(v, " WARN "))
                        .toArray(),
                "\n");
    }

    private ContainerState getSpark() {
        return environment.getContainerByServiceName("spark-master_1").get();
    }
}
