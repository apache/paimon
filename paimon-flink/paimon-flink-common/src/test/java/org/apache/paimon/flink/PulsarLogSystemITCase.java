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

package org.apache.paimon.flink;

import org.apache.paimon.flink.pulsar.PulsarTableTestBase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ITCase for table with log system.
 */
public class PulsarLogSystemITCase extends PulsarTableTestBase {

    @BeforeEach
    public void before() throws IOException {
        String path = getTempDirPath();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON WITH (" + "'type'='paimon', 'warehouse'='%s')",
                        path));
        tEnv.useCatalog("PAIMON");
    }

    @Test
    public void testAppendOnlyWithEventual() throws Exception {
        createTopicIfNotExists("T", 1);
        // disable checkpointing to test eventual TODO why not work
        env.getCheckpointConfig().setCheckpointInterval(3 * 1000);
        env.setParallelism(1);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'log.system'='pulsar', "
                                + "'write-mode'='append-only', "
                                + "'log.consistency'='eventual', "
                                + "'pulsar.admin.admin-url'='%s', "
                                + "'pulsar.client.service-url'='%s', "
                                + "'pulsar.topic'='T')",
                        getHttpServiceUrl(), getPulsarBrokerUrl()));
        tEnv.executeSql("CREATE TEMPORARY TABLE gen (i INT, j INT) WITH ('connector'='datagen')");
        TableResult write = tEnv.executeSql("INSERT INTO T SELECT * FROM gen");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(tEnv.executeSql("SELECT * FROM T").collect());
        List<Row> collect = read.collect(10, 10, TimeUnit.DAYS);
        assertThat(collect).hasSize(10);
        write.getJobClient().get().cancel();
        read.close();
    }

    @Test
    public void testReadFromFile() throws Exception {
        createTopicIfNotExists("test-double-sink", 1);
        env.getCheckpointConfig().setCheckpointInterval(3 * 1000);
        env.setParallelism(1);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE pulsar_file_double_sink (\n"
                                + " word STRING ,\n"
                                + "    cnt BIGINT,\n"
                                + "      PRIMARY KEY (word) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (\n"
                                + " 'merge-engine' = 'aggregation',\n"
                                + "  'changelog-producer' = 'full-compaction',\n"
                                + "    'log.system' = 'pulsar',\n"
                                + "    'streaming-read-mode'='file',\n"
                                + "    'fields.cnt.aggregate-function' = 'sum',\n"
                                + "    'pulsar.admin.admin-url'='%s', "
                                + "    'pulsar.client.service-url'='%s', "
                                + "    'pulsar.topic' = 'test-double-sink'\n"
                                + "\n"
                                + ");",
                        getHttpServiceUrl(), getPulsarBrokerUrl()));
        TableResult write =
                tEnv.executeSql(
                        "INSERT INTO pulsar_file_double_sink values('a',1),('b',2),('c',3);");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(
                        tEnv.executeSql("SELECT * FROM pulsar_file_double_sink").collect());
        assertThat(read.collect(3))
                .containsExactlyInAnyOrder(Row.of("a", 1L), Row.of("b", 2L), Row.of("c", 3L));
        write.getJobClient().get().cancel();
        read.close();
    }

    @Test
    public void testReadFromLog() throws Exception {
        createTopicIfNotExists("test-single-sink", 1);
        // disable checkpointing to test eventual
        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);
        // 'fields.cnt.aggregate-function' = 'sum' is miss will throw
        // java.lang.UnsupportedOperationException: Aggregate function 'last_non_null_value' does
        // not support retraction
        // data will only be written to
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE pulsar_file_single_sink (\n"
                                + " word STRING ,\n"
                                + "    cnt BIGINT,\n"
                                + "      PRIMARY KEY (word) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (\n"
                                + " 'merge-engine' = 'aggregation',\n"
                                + "    'changelog-producer' = 'full-compaction',\n"
                                + "    'log.consistency' = 'eventual',\n"
                                + "    'log.system' = 'pulsar',\n"
                                + "    'streaming-read-mode'='log',\n"
                                + "    'pulsar.admin.admin-url'='%s', "
                                + "    'pulsar.client.service-url'='%s', "
                                + "    'pulsar.topic' = 'test-single-sink'\n"
                                + "\n"
                                + ");",
                        getHttpServiceUrl(), getPulsarBrokerUrl()));
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE word_table (\n"
                        + "    word STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'fields.word.length' = '1'\n"
                        + ");");
        TableResult write =
                tEnv.executeSql(
                        "INSERT INTO pulsar_file_single_sink SELECT word, COUNT(*) FROM word_table GROUP BY word;");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(
                        tEnv.executeSql("SELECT * FROM pulsar_file_single_sink").collect());
        List<Row> collect = read.collect(10);
        assertThat(collect).hasSize(10);
        write.getJobClient().get().cancel();
        read.close();
    }

    @Test
    public void testReadFromLogWithOutSteamingReadMode() throws Exception {
        createTopicIfNotExists("test-single-sink", 1);
        env.setParallelism(1);

        tEnv.executeSql(
                "CREATE TABLE pulsar_file_single_sink (\n"
                        + " word STRING ,\n"
                        + "    cnt BIGINT,\n"
                        + "      PRIMARY KEY (word) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH (\n"
                        + " 'merge-engine' = 'aggregation',\n"
                        + "    'changelog-producer' = 'full-compaction',\n"
                        + "    'streaming-read-mode'='log'\n"
                        + ");");
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE word_table (\n"
                        + "    word STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'fields.word.length' = '1'\n"
                        + ");");
        assertThatThrownBy(
                () ->
                        tEnv.executeSql(
                                "INSERT INTO pulsar_file_single_sink SELECT word, COUNT(*) FROM word_table GROUP BY word;"))
                .getRootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "File store continuous reading does not support the log streaming read mode.");
    }
}
