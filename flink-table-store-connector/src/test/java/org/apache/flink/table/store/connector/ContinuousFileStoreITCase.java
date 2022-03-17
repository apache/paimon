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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.store.file.FileStoreOptions.FILE_PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL ITCase for continuous file store. */
public class ContinuousFileStoreITCase extends AbstractTestBase {

    private TableEnvironment bEnv;
    private TableEnvironment sEnv;

    @Before
    public void before() throws IOException {
        bEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        sEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));
        String path = TEMPORARY_FOLDER.newFolder().toURI().toString();
        prepareEnv(bEnv, path);
        prepareEnv(sEnv, path);
    }

    private void prepareEnv(TableEnvironment env, String path) {
        Configuration config = env.getConfig().getConfiguration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        config.setString(TABLE_STORE_PREFIX + FILE_PATH.key(), path);
        env.executeSql("CREATE TABLE IF NOT EXISTS T1 (a STRING, b STRING, c STRING)");
        env.executeSql(
                "CREATE TABLE IF NOT EXISTS T2 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)");
    }

    @Test
    public void testWithoutPrimaryKey() throws ExecutionException, InterruptedException {
        testSimple("T1");
    }

    @Test
    public void testWithPrimaryKey() throws ExecutionException, InterruptedException {
        testSimple("T2");
    }

    @Test
    public void testProjectionWithoutPrimaryKey() throws ExecutionException, InterruptedException {
        testProjection("T1");
    }

    @Test
    public void testProjectionWithPrimaryKey() throws ExecutionException, InterruptedException {
        testProjection("T2");
    }

    private void testSimple(String table) throws ExecutionException, InterruptedException {
        CloseableIterator<Row> iterator = sEnv.executeSql("SELECT * FROM " + table).collect();

        bEnv.executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table))
                .await();
        assertThat(collectFromUnbounded(iterator, 2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        bEnv.executeSql(String.format("INSERT INTO %s VALUES ('7', '8', '9')", table)).await();
        assertThat(collectFromUnbounded(iterator, 1))
                .containsExactlyInAnyOrder(Row.of("7", "8", "9"));
    }

    private void testProjection(String table) throws ExecutionException, InterruptedException {
        CloseableIterator<Row> iterator = sEnv.executeSql("SELECT b, c FROM " + table).collect();

        bEnv.executeSql(
                        String.format(
                                "INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table))
                .await();
        assertThat(collectFromUnbounded(iterator, 2))
                .containsExactlyInAnyOrder(Row.of("2", "3"), Row.of("5", "6"));

        bEnv.executeSql(String.format("INSERT INTO %s VALUES ('7', '8', '9')", table)).await();
        assertThat(collectFromUnbounded(iterator, 1)).containsExactlyInAnyOrder(Row.of("8", "9"));
    }

    @Test
    public void testContinuousLatest() throws ExecutionException, InterruptedException {
        bEnv.executeSql("INSERT INTO T1 VALUES ('1', '2', '3'), ('4', '5', '6')").await();

        CloseableIterator<Row> iterator =
                sEnv.executeSql("SELECT * FROM T1 /*+ OPTIONS('log.scan'='latest') */").collect();

        bEnv.executeSql("INSERT INTO T1 VALUES ('7', '8', '9'), ('10', '11', '12')").await();
        assertThat(collectFromUnbounded(iterator, 2))
                .containsExactlyInAnyOrder(Row.of("7", "8", "9"), Row.of("10", "11", "12"));
    }

    @Test
    public void testUnsupportedUpsert() {
        assertThatThrownBy(
                () ->
                        sEnv.executeSql(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.changelog-mode'='upsert') */")
                                .collect(),
                "File store continuous reading dose not support upsert changelog mode");
    }

    @Test
    public void testUnsupportedEventual() {
        assertThatThrownBy(
                () ->
                        sEnv.executeSql(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.consistency'='eventual') */")
                                .collect(),
                "File store continuous reading dose not support eventual consistency mode");
    }

    @Test
    public void testUnsupportedStartupTimestamp() {
        assertThatThrownBy(
                () ->
                        sEnv.executeSql(
                                        "SELECT * FROM T1 /*+ OPTIONS('log.scan'='from-timestamp') */")
                                .collect(),
                "File store continuous reading dose not support from_timestamp scan mode, "
                        + "you can add timestamp filters instead.");
    }

    private List<Row> collectFromUnbounded(CloseableIterator<Row> iterator, int numElements) {
        if (numElements == 0) {
            return Collections.emptyList();
        }

        List<Row> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());

            if (result.size() == numElements) {
                return result;
            }
        }

        throw new IllegalArgumentException(
                String.format(
                        "The stream ended before reaching the requested %d records. Only %d records were received.",
                        numElements, result.size()));
    }
}
