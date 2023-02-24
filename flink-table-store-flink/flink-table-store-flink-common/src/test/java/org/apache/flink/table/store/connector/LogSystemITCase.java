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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for table with log system. */
public class LogSystemITCase extends KafkaTableTestBase {

    @BeforeEach
    public void before() throws IOException {
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG TABLE_STORE WITH ("
                                + "'type'='table-store', 'warehouse'='%s')",
                        getTempDirPath()));
        tEnv.useCatalog("TABLE_STORE");
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
}
