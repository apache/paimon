/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.store.connector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.table.store.kafka.KafkaTableTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.store.file.FileStoreOptions.PATH;
import static org.apache.flink.table.store.file.FileStoreOptions.TABLE_STORE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Test cases to verify the log store for append-only table. */
public class AppendOnlyLogTableTest extends KafkaTableTestBase {
    private static final int NO_LIMIT = -1;

    private TableEnvironment tEnv;
    protected TableEnvironment sEnv;

    @Before
    @Override
    public void setup() {
        super.setup();

        try {
            tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
            sEnv =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance().inStreamingMode().build());
            sEnv.getConfig().getConfiguration().set(CHECKPOINTING_INTERVAL, Duration.ofMillis(100));

            String path = TEMPORARY_FOLDER.newFolder().toURI().toString();
            prepareEnv(tEnv, path);
            prepareEnv(sEnv, path);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void prepareEnv(TableEnvironment env, String path) {
        Configuration config = env.getConfig().getConfiguration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        config.setString(TABLE_STORE_PREFIX + PATH.key(), path);
    }

    @Test
    public void testBatchWriteWithLogEnabled() {
        batch(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ("
                        + " 'write-mode'='append-only', "
                        + " 'log.system'='kafka', "
                        + " 'log.kafka.bootstrap.servers'='%s')",
                getBootstrapServers());

        batch("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'AAA'), (1, 'BBB'), (1, 'AAA')");

        List<Row> rows = batch("SELECT * FROM append_table");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "AAA"), Row.of(1, "BBB"), Row.of(1, "AAA"));
    }

    @Test
    public void testFullLogScan() {
        stream(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ("
                        + " 'write-mode'='append-only', "
                        + " 'log.system'='kafka', "
                        + " 'log.kafka.bootstrap.servers'='%s')",
                getBootstrapServers());

        stream("INSERT INTO append_table VALUES (1, 'AAA'), (2, 'AAA'), (1, 'BBB'), (1, 'AAA')");

        // 1st full log scan.
        List<Row> rows = stream(4, "SELECT * FROM append_table /*+ OPTIONS ('log.scan'='full') */");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"), Row.of(2, "AAA"), Row.of(1, "BBB"), Row.of(1, "AAA"));

        // 2st full log scan.
        stream("INSERT INTO append_table VALUES (3, 'CCC'), (4, 'DDD')");
        rows = stream(6, "SELECT * FROM append_table /*+ OPTIONS('log.scan'='full') */");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1, "AAA"),
                        Row.of(2, "AAA"),
                        Row.of(1, "BBB"),
                        Row.of(1, "AAA"),
                        Row.of(3, "CCC"),
                        Row.of(4, "DDD"));

        // Full log scan with a projection.
        rows = stream(6, "SELECT id FROM append_table /*+ OPTIONS('log.scan'='full') */");
        assertThat(rows)
                .containsExactlyInAnyOrder(
                        Row.of(1), Row.of(2), Row.of(1), Row.of(1), Row.of(3), Row.of(4));

        // Full log scan with a filter.
        stream("INSERT INTO append_table VALUES (5, 'EEE'), (6, 'FFF')");
        rows =
                stream(
                        3,
                        "SELECT * FROM append_table /*+ OPTIONS('log.scan'='full') */ WHERE data >= 'DDD'");
        assertThat(rows)
                .containsExactlyInAnyOrder(Row.of(4, "DDD"), Row.of(5, "EEE"), Row.of(6, "FFF"));
    }

    @Test
    public void testLatestLogScan() throws Exception {
        stream(
                "CREATE TABLE IF NOT EXISTS append_table (id INT, data STRING) WITH ("
                        + " 'write-mode'='append-only', "
                        + " 'log.system'='kafka', "
                        + " 'log.kafka.bootstrap.servers'='%s')",
                getBootstrapServers());

        // Test latest log scan.
        try (CloseableIterator<Row> it =
                streamIter("SELECT * FROM append_table /*+ OPTIONS('log.scan'='latest') */")) {
            stream("INSERT INTO append_table VALUES (3, 'AAA')");

            List<Row> rows = BlockingIterator.of(it).collect(1, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(3, "AAA"));

            stream("INSERT INTO append_table VALUES (4, 'BBB'), (5, 'CCC')");
            rows = BlockingIterator.of(it).collect(2, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(4, "BBB"), Row.of(5, "CCC"));

            stream("INSERT INTO append_table VALUES (6, 'DDD'), (7, 'CCC')");
            rows = BlockingIterator.of(it).collect(2, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactlyInAnyOrder(Row.of(6, "DDD"), Row.of(7, "CCC"));
        }

        // Test latest log scan with a projection.
        try (CloseableIterator<Row> it =
                streamIter(
                        "SELECT id FROM append_table /*+ OPTIONS('log.scan'='latest') */ WHERE id >= 3")) {
            stream("INSERT INTO append_table VALUES (1, 'AAA'), (3, 'CCC')");

            List<Row> rows = BlockingIterator.of(it).collect(1, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(3));

            stream("INSERT INTO append_table VALUES (2, 'BBB'), (3, 'CCC'), (4, 'DDD')");
            rows = BlockingIterator.of(it).collect(2, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(3), Row.of(4));
        }

        // Test latest log scan with a filter.
        try (CloseableIterator<Row> it =
                streamIter(
                        "SELECT * FROM append_table /*+ OPTIONS('log.scan'='latest') */ WHERE data >= 'CCC'")) {
            stream("INSERT INTO append_table VALUES (1, 'AAA'), (3, 'CCC')");

            List<Row> rows = BlockingIterator.of(it).collect(1, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(3, "CCC"));

            stream("INSERT INTO append_table VALUES (2, 'BBB'), (3, 'CCC'), (4, 'DDD')");
            rows = BlockingIterator.of(it).collect(2, 5L, TimeUnit.SECONDS);
            assertThat(rows).containsExactly(Row.of(3, "CCC"), Row.of(4, "DDD"));
        }
    }

    private List<Row> sql(boolean batch, int limit, String query, Object... args) {
        TableEnvironment env = batch ? tEnv : sEnv;
        TableResult tableResult = env.executeSql(String.format(query, args));

        try (CloseableIterator<Row> iter = tableResult.collect()) {
            return limit == NO_LIMIT
                    ? ImmutableList.copyOf(iter)
                    : BlockingIterator.of(iter).collect(limit, 5L, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect the table result.", e);
        }
    }

    private CloseableIterator<Row> streamIter(String query, Object... args) {
        return sEnv.executeSql(String.format(query, args)).collect();
    }

    private List<Row> stream(String query, Object... args) {
        return stream(NO_LIMIT, query, args);
    }

    private List<Row> stream(int limit, String query, Object... args) {
        return sql(false, limit, query, args);
    }

    protected List<Row> batch(String query, Object... args) {
        return sql(true, NO_LIMIT, query, args);
    }
}
