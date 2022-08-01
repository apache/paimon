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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** SQL ITCase for continuous file store with full-compaction changelog producer. */
public class ContinuousFullChangelogITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        String commonOptions =
                "'changelog-producer'='full-compaction', "
                        + "'bucket'='2', "
                        + "'log.changelog-mode'='all', "
                        + "'num-sorted-run.compaction-trigger'='2', "
                        + "'commit.force-compact'='true'";
        return Arrays.asList(
                format(
                        "CREATE TABLE IF NOT EXISTS T1 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                                + " WITH (%s)",
                        commonOptions),
                format(
                        "CREATE TABLE IF NOT EXISTS T2 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                                + " WITH ('merge-engine'='partial-update', %s)",
                        commonOptions));
    }

    @Test
    public void testUpsert() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM T1"));

        batchSql("INSERT INTO T1 VALUES ('1', '1', '1'), ('2', '2', '2'), ('4', '4', '4')");
        batchSql("INSERT INTO T1 VALUES ('1', '2', '2')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "2"), Row.of("2", "2", "2"));

        batchSql("INSERT INTO T1 VALUES ('1', '3', '3')");
        batchSql("INSERT INTO T1 VALUES ('1', '4', '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(UPDATE_BEFORE, "1", "2", "2"),
                        Row.ofKind(UPDATE_AFTER, "1", "4", "4"));

        batchSql("INSERT INTO T1 VALUES ('4', '5', '5')");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("4", "5", "5"));
        iterator.close();
    }

    @Test
    public void testPartialUpdate() throws Exception {
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM T2"));

        batchSql(
                "INSERT INTO T2 VALUES ('1', '1', CAST(NULL AS STRING)), ('2', '2', CAST(NULL AS STRING)), ('4', '4', CAST(NULL AS STRING))");
        batchSql("INSERT INTO T2 VALUES ('1', '2', CAST(NULL AS STRING))");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", null), Row.of("2", "2", null));

        batchSql("INSERT INTO T2 VALUES ('1', CAST(NULL AS STRING), '3')");
        batchSql("INSERT INTO T2 VALUES ('1', CAST(NULL AS STRING), '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(UPDATE_BEFORE, "1", "2", null),
                        Row.ofKind(UPDATE_AFTER, "1", "2", "4"));

        batchSql("INSERT INTO T2 VALUES ('4', CAST(NULL AS STRING), '5')");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of("4", "4", "5"));
        iterator.close();
    }
}
