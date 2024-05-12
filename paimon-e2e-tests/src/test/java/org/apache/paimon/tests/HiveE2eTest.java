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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Tests for reading paimon from Hive.
 *
 * <p>NOTE: This test runs a complete Hadoop cluster in Docker, which requires a lot of memory. If
 * you're running this test locally, make sure that the memory limit of your Docker is at least 8GB.
 */
public class HiveE2eTest extends E2eReaderTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(HiveE2eTest.class);

    public HiveE2eTest() {
        super(false, true, false);
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        setupHiveConnector();
    }

    @Test
    public void testReadExternalTable() throws Exception {
        final String table = "paimon_pk";
        String paimonPkPath = HDFS_ROOT + "/" + UUID.randomUUID() + ".store";
        String paimonPkDdl =
                String.format(
                        "CREATE TABLE IF NOT EXISTS %s (\n"
                                + "  a int,\n"
                                + "  b bigint,\n"
                                + "  c string,\n"
                                + "  PRIMARY KEY (a, b) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'bucket' = '2'\n"
                                + ");",
                        table);
        runBatchSql(createInsertSql(table), createCatalogSql("paimon", paimonPkPath), paimonPkDdl);

        String externalTablePkDdl =
                String.format(
                        "CREATE EXTERNAL TABLE IF NOT EXISTS %s\n"
                                + "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'\n"
                                + "LOCATION '%s/default.db/%s';\n",
                        table, paimonPkPath, table);

        checkQueryResults(table, this::executeQuery, externalTablePkDdl);
    }

    @Test
    public void testFlinkWriteAndHiveRead() throws Exception {
        final String warehouse = HDFS_ROOT + "/" + UUID.randomUUID() + ".warehouse";
        final String table = "t";
        runBatchSql(
                String.join(
                        "\n",
                        createCatalogSql(
                                "my_hive",
                                warehouse,
                                "'metastore' = 'hive'",
                                "'uri' = 'thrift://hive-metastore:9083'"),
                        createTableSql(table),
                        createInsertSql(table)));
        checkQueryResults(table, this::executeQuery);
    }

    @Test
    public void testHiveWrite() throws Exception {
        final String table = "hive_test";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + table + " (",
                                "a bigint" + " COMMENT 'The a field',",
                                "b bigint" + " COMMENT 'The b field',",
                                "c string" + " COMMENT 'The c field'",
                                ")",
                                "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler';"));
        String hql1 = "hiveddl.hql";
        writeSharedFile(hql1, hiveSql);
        executeQuery(hql1);

        String hql2 = "hivedml.hql";
        String insertSql =
                String.format(
                        "INSERT INTO %s VALUES "
                                + "(1, 10, 'Hi'), "
                                + "(1, 100, 'Hi Again'), "
                                + "(2, 20, 'Hello'), "
                                + "(3, 30, 'Table'), "
                                + "(4, 40, 'Store');",
                        table);
        writeSharedFile(hql2, insertSql);
        executeQuery(hql2);

        checkQueryResults(table, this::executeQuery);
    }

    @Test
    public void testMetastorePartitionedTable() throws Exception {
        String warehouse = HDFS_ROOT + "/" + UUID.randomUUID() + ".warehouse";

        String createTableSql =
                String.join(
                        "\n",
                        "CREATE TABLE t (",
                        "    pta INT,",
                        "    ptb STRING,",
                        "    k BIGINT,",
                        "    v STRING,",
                        "    PRIMARY KEY (pta, ptb, k) NOT ENFORCED",
                        ") PARTITIONED BY (pta, ptb) WITH (",
                        "    'bucket' = '2',",
                        "    'metastore.partitioned-table' = 'true'",
                        ");");
        List<String> values = new ArrayList<>();
        for (int pta = 0; pta <= 1; pta++) {
            for (int ptb = 0; ptb <= 1; ptb++) {
                for (int k = 0; k <= 2; k++) {
                    values.add(
                            String.format("(%d, '%d', %d, '%d-%d-%d')", pta, ptb, k, pta, ptb, k));
                }
            }
        }

        runBatchSql(
                "INSERT INTO t VALUES " + String.join(", ", values) + ";",
                createCatalogSql(
                        "my_hive",
                        warehouse,
                        "'metastore' = 'hive'",
                        "'uri' = 'thrift://hive-metastore:9083'"),
                createTableSql);

        checkQueryResult(
                this::executeQuery,
                "SHOW PARTITIONS t;",
                "pta=0/ptb=0\n" + "pta=0/ptb=1\n" + "pta=1/ptb=0\n" + "pta=1/ptb=1\n");
        checkQueryResult(
                this::executeQuery,
                "SELECT v, ptb FROM t WHERE pta = 1 AND ptb >= 0 ORDER BY v;",
                "1-0-0\t0\n"
                        + "1-0-1\t0\n"
                        + "1-0-2\t0\n"
                        + "1-1-0\t1\n"
                        + "1-1-1\t1\n"
                        + "1-1-2\t1\n");
        checkQueryResult(
                this::executeQuery,
                "SELECT count(*) FROM t WHERE pta < 1 AND ptb >= 0 AND k % 2 = 0;",
                "4\n");
    }

    private String executeQuery(String sql) throws Exception {
        Container.ExecResult execResult =
                getHive()
                        .execInContainer(
                                "/opt/hive/bin/hive",
                                "--hiveconf",
                                "hive.root.logger=INFO,console",
                                "-f",
                                TEST_DATA_DIR + "/" + sql);
        LOG.info(execResult.getStdout());
        LOG.info(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when running hive sql.");
        }
        return execResult.getStdout();
    }
}
