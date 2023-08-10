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

import org.apache.paimon.flink.kafka.KafkaTableTestBase;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for table with log system. */
public class LogSystemITCase extends KafkaTableTestBase {

    @BeforeEach
    public void before() throws IOException {
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON WITH (" + "'type'='paimon', 'warehouse'='%s')",
                        getTempDirPath()));
        tEnv.useCatalog("PAIMON");
    }

    @Test
    public void testAppendOnlyWithEventual() throws Exception {
        createTopicIfNotExists("T", 1);
        // disable checkpointing to test eventual
        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'log.system'='kafka', "
                                + "'write-mode'='append-only', "
                                + "'log.consistency'='eventual', "
                                + "'kafka.bootstrap.servers'='%s', "
                                + "'kafka.topic'='T')",
                        getBootstrapServers()));
        tEnv.executeSql("CREATE TEMPORARY TABLE gen (i INT, j INT) WITH ('connector'='datagen')");
        TableResult write = tEnv.executeSql("INSERT INTO T SELECT * FROM gen");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(tEnv.executeSql("SELECT * FROM T").collect());
        List<Row> collect = read.collect(10);
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
                        "CREATE TABLE kafka_file_double_sink (\n"
                                + " word STRING ,\n"
                                + "    cnt BIGINT,\n"
                                + "      PRIMARY KEY (word) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (\n"
                                + " 'merge-engine' = 'aggregation',\n"
                                + "  'changelog-producer' = 'full-compaction',\n"
                                + "    'log.system' = 'kafka',\n"
                                + "    'streaming-read-mode'='file',\n"
                                + "    'fields.cnt.aggregate-function' = 'sum',\n"
                                + "    'kafka.bootstrap.servers' = '%s',\n"
                                + "    'kafka.topic' = 'test-double-sink',\n"
                                + "    'kafka.transaction.timeout.ms'='30000'\n"
                                + "\n"
                                + ");",
                        getBootstrapServers()));
        TableResult write =
                tEnv.executeSql(
                        "INSERT INTO kafka_file_double_sink values('a',1),('b',2),('c',3);");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(
                        tEnv.executeSql("SELECT * FROM kafka_file_double_sink").collect());
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
        // data will only be written to kafka
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE kafka_file_single_sink (\n"
                                + " word STRING ,\n"
                                + "    cnt BIGINT,\n"
                                + "      PRIMARY KEY (word) NOT ENFORCED\n"
                                + ")\n"
                                + "WITH (\n"
                                + " 'merge-engine' = 'aggregation',\n"
                                + "    'changelog-producer' = 'full-compaction',\n"
                                + "    'log.consistency' = 'eventual',\n"
                                + "    'log.system' = 'kafka',\n"
                                + "    'streaming-read-mode'='log',\n"
                                + "    'kafka.bootstrap.servers' = '%s',\n"
                                + "    'kafka.topic' = 'test-single-sink',\n"
                                + "    'kafka.transaction.timeout.ms'='30000'\n"
                                + "\n"
                                + ");",
                        getBootstrapServers()));
        tEnv.executeSql(
                "CREATE TEMPORARY TABLE word_table (\n"
                        + "    word STRING\n"
                        + ") WITH (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'fields.word.length' = '1'\n"
                        + ");");
        TableResult write =
                tEnv.executeSql(
                        "INSERT INTO kafka_file_single_sink SELECT word, COUNT(*) FROM word_table GROUP BY word;");
        BlockingIterator<Row, Row> read =
                BlockingIterator.of(
                        tEnv.executeSql("SELECT * FROM kafka_file_single_sink").collect());
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
                "CREATE TABLE kafka_file_single_sink (\n"
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
                                        "INSERT INTO kafka_file_single_sink SELECT word, COUNT(*) FROM word_table GROUP BY word;"))
                .getRootCause()
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "File store continuous reading does not support the log streaming read mode.");
    }

    @Test
    @Timeout(60)
    public void testLogSystemAutoRegister() throws TableNotExistException {
        // enable log system auto registration
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON_REGISTER WITH ("
                                + "'type'='paimon', 'warehouse'='%s', 'log.system.auto-register'='true')",
                        getTempDirPath()));
        tEnv.useCatalog("PAIMON_REGISTER");

        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);

        // check register table with specified bootstrap server and partition num.
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'log.system'='kafka', "
                                + "'log.system.partitions'='2', "
                                + "'write-mode'='append-only', "
                                + "'kafka.bootstrap.servers'='%s', "
                                + "'kafka.topic'='T')",
                        getBootstrapServers()));

        checkTopicExists("T", 2, 1);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T2 (i INT, j INT) WITH ("
                                + "'log.system'='kafka', "
                                + "'bucket'='2', "
                                + "'write-mode'='append-only', "
                                + "'kafka.bootstrap.servers'='%s', "
                                + "'kafka.topic'='T2')",
                        getBootstrapServers()));

        checkTopicExists("T2", 2, 1);

        // check register a random kafka topic
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T1 (i INT, j INT) WITH ("
                                + "'log.system'='kafka', "
                                + "'log.system.partitions'='2', "
                                + "'write-mode'='append-only', "
                                + "'kafka.bootstrap.servers'='%s')",
                        getBootstrapServers()));

        CatalogBaseTable table =
                tEnv.getCatalog("PAIMON_REGISTER")
                        .get()
                        .getTable(ObjectPath.fromString("default.T"));
        checkTopicExists(table.getOptions().get("kafka.topic"), 2, 1);

        // check unregister topic when creating table fail
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "CREATE TABLE T (i INT, j INT) WITH ("
                                                        + "'log.system'='kafka', "
                                                        + "'log.system.partitions'='2', "
                                                        + "'write-mode'='append-only', "
                                                        + "'kafka.bootstrap.servers'='%s', "
                                                        + "'kafka.topic'='T1')",
                                                getBootstrapServers())))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessage("Could not execute CreateTable in path `PAIMON_REGISTER`.`default`.`T`")
                .cause()
                .isInstanceOf(
                        org.apache.flink.table.catalog.exceptions.TableAlreadyExistException.class)
                .hasMessage("Table (or view) default.T already exists in Catalog PAIMON_REGISTER.");

        checkTopicNotExist("T1");

        // tEnv.useDatabase("NOT_EXIST");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        String.format(
                                                "CREATE TABLE NOT_EXIST.T (i INT, j INT) WITH ("
                                                        + "'log.system'='kafka', "
                                                        + "'log.system.partitions'='2', "
                                                        + "'write-mode'='append-only', "
                                                        + "'kafka.bootstrap.servers'='%s', "
                                                        + "'kafka.topic'='T1')",
                                                getBootstrapServers())))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessage(
                        "Could not execute CreateTable in path `PAIMON_REGISTER`.`NOT_EXIST`.`T`")
                .cause()
                .isInstanceOf(
                        org.apache.flink.table.catalog.exceptions.DatabaseNotExistException.class)
                .hasMessage("Database NOT_EXIST does not exist in Catalog PAIMON_REGISTER.");

        checkTopicNotExist("T1");

        // check unregister topic when drop table
        tEnv.executeSql("DROP TABLE T");
        checkTopicNotExist("T");
    }

    @Test
    @Timeout(60)
    public void testLogSystemAutoRegisterWithDefaultOption() {
        // enable log system auto registration
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON_DEFAULT WITH ("
                                + "'type'='paimon', 'warehouse'='%s', "
                                + "'log.system.auto-register'='true', "
                                + "'table-default.kafka.bootstrap.servers'='%s',"
                                + "'table-default.log.system.partitions'='2')",
                        getTempDirPath(), getBootstrapServers()));
        tEnv.useCatalog("PAIMON_DEFAULT");

        env.getCheckpointConfig().disableCheckpointing();
        env.setParallelism(1);

        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T (i INT, j INT) WITH ("
                                + "'log.system'='kafka', "
                                + "'write-mode'='append-only', "
                                + "'kafka.topic'='T')",
                        getBootstrapServers()));

        checkTopicExists("T", 2, 1);
    }
}
