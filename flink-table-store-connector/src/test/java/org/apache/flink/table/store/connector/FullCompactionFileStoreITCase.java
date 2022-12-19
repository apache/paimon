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
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL ITCase for continuous file store. */
public class FullCompactionFileStoreITCase extends CatalogITCaseBase {
    private final String table = "T";

    @Override
    @Before
    public void before() throws IOException {
        super.before();
        String options =
                " WITH('changelog-producer'='full-compaction', 'changelog-producer.compaction-interval' = '1s')";
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS T (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + options);
    }

    @Test
    public void testStreamingRead() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        sql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        sql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("7", "8", "9"));
    }

    @Test
    public void testCompactedScanMode() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT * FROM %s /*+ OPTIONS('scan.mode'='compacted-full') */",
                                table));

        sql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '6')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));

        sql("INSERT INTO %s VALUES ('7', '8', '9')", table);
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("7", "8", "9"));

        assertThat(sql("SELECT * FROM T /*+ OPTIONS('scan.mode'='compacted-full') */"))
                .containsExactlyInAnyOrder(
                        Row.of("1", "2", "3"), Row.of("4", "5", "6"), Row.of("7", "8", "9"));
    }

    @Test
    public void testUpdate() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        sql("INSERT INTO %s VALUES ('1', '2', '3')", table);
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "1", "2", "3"));

        sql("INSERT INTO %s VALUES ('1', '4', '5')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, "1", "2", "3"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "1", "4", "5"));

        iterator.close();
    }

    @Test
    public void testUpdateAuditLog() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s$audit_log", table));

        sql("INSERT INTO %s VALUES ('1', '2', '3')", table);
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "+I", "1", "2", "3"));

        sql("INSERT INTO %s VALUES ('1', '4', '5')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, "-U", "1", "2", "3"),
                        Row.ofKind(RowKind.INSERT, "+U", "1", "4", "5"));

        iterator.close();

        // BATCH mode
        assertThat(sql("SELECT * FROM %s$audit_log", table))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "+I", "1", "4", "5"));
    }
}
