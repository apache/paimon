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

import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link FlinkCatalog}. */
public class FileSystemCatalogITCase extends KafkaTableTestBase {

    @Before
    public void before() throws IOException {
        String path = TEMPORARY_FOLDER.newFolder().toURI().toString();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG fs WITH ('type'='table-store', 'warehouse'='%s')", path));
        tEnv.useCatalog("fs");
        env.setParallelism(1);
    }

    @Test
    public void testWriteRead() throws Exception {
        tEnv.executeSql("CREATE TABLE T (a STRING, b STRING, c STRING)");
        innerTestWriteRead();
    }

    @Test
    public void testLogWriteRead() throws Exception {
        String topic = UUID.randomUUID().toString();
        createTopicIfNotExists(topic, 1);

        try {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE T (a STRING, b STRING, c STRING) WITH ("
                                    + "'log.system'='kafka', "
                                    + "'kafka.bootstrap.servers'='%s',"
                                    + "'kafka.topic'='%s'"
                                    + ")",
                            getBootstrapServers(), topic));
            innerTestWriteRead();
        } finally {
            deleteTopicIfExists(topic);
        }
    }

    @Test
    public void testLogWriteReadWithVirtual() throws Exception {
        String topic = UUID.randomUUID().toString();
        createTopicIfNotExists(topic, 1);

        try {
            tEnv.executeSql(
                    String.format(
                            "CREATE TABLE T ("
                                    + "a STRING, "
                                    + "b STRING, "
                                    + "c STRING, "
                                    + "d AS CAST(c as INT) + 1"
                                    + ") WITH ("
                                    + "'log.system'='kafka', "
                                    + "'kafka.bootstrap.servers'='%s',"
                                    + "'kafka.topic'='%s'"
                                    + ")",
                            getBootstrapServers(), topic));
            BlockingIterator<Row, Row> iterator =
                    BlockingIterator.of(tEnv.from("T").execute().collect());
            tEnv.executeSql("INSERT INTO T VALUES ('1', '2', '3'), ('4', '5', '6')").await();
            List<Row> result = iterator.collectAndClose(2);
            assertThat(result)
                    .containsExactlyInAnyOrder(Row.of("1", "2", "3", 4), Row.of("4", "5", "6", 7));
        } finally {
            deleteTopicIfExists(topic);
        }
    }

    private void innerTestWriteRead() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(tEnv.from("T").execute().collect());
        tEnv.executeSql("INSERT INTO T VALUES ('1', '2', '3'), ('4', '5', '6')").await();
        List<Row> result = iterator.collectAndClose(2);
        assertThat(result).containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
    }
}
