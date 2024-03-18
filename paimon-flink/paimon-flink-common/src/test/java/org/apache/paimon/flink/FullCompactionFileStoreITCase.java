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

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL ITCase for continuous file store. */
public class FullCompactionFileStoreITCase extends CatalogITCaseBase {
    private final String table = "T";
    private final String options =
            " WITH('changelog-producer'='full-compaction', 'changelog-producer.compaction-interval' = '1s')";

    @Override
    @BeforeEach
    public void before() throws IOException {
        super.before();
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

    /** Test streaming read with array and row nested data type. */
    @Test
    public void testStreamingReadOfArray() throws Exception {
        String table = "T_ARRAY";
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + table
                        + "("
                        + "ID INT PRIMARY KEY NOT ENFORCED,\n"
                        + "NAMES ARRAY<ROW<NAME STRING, MARK STRING>>\n"
                        + ")"
                        + options);
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        sql(
                "INSERT INTO %s VALUES (1, ARRAY[('c','mark1'), ('d','mark2'), ('e','mark3')]);",
                table);
        assertThat(iterator.collect(1).stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("+I[1, [+I[c, mark1], +I[d, mark2], +I[e, mark3]]]");
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testUpdate(boolean changelogRowDeduplicate) throws Exception {
        sql(
                "ALTER TABLE %s SET ('changelog-producer.row-deduplicate' = '%s')",
                table, changelogRowDeduplicate);

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        sql("INSERT INTO %s VALUES ('1', '2', '3'), ('4', '5', '1')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "1"));

        sql("INSERT INTO %s VALUES ('1', '4', '5')", table);
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, "1", "2", "3"),
                        Row.ofKind(RowKind.UPDATE_AFTER, "1", "4", "5"));

        // insert duplicate record
        for (int i = 1; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('1', '4', '5'), ('4', '5', '%s')", table, i + 1);
            if (changelogRowDeduplicate) {
                assertThat(iterator.collect(2))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.UPDATE_BEFORE, "4", "5", String.valueOf(i)),
                                Row.ofKind(RowKind.UPDATE_AFTER, "4", "5", String.valueOf(i + 1)));
            } else {
                assertThat(iterator.collect(4))
                        .containsExactlyInAnyOrder(
                                Row.ofKind(RowKind.UPDATE_BEFORE, "1", "4", "5"),
                                Row.ofKind(RowKind.UPDATE_AFTER, "1", "4", "5"),
                                Row.ofKind(RowKind.UPDATE_BEFORE, "4", "5", String.valueOf(i)),
                                Row.ofKind(RowKind.UPDATE_AFTER, "4", "5", String.valueOf(i + 1)));
            }
        }

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

    @Test
    public void testRowDeduplicateWithArrayRow() throws Exception {
        String table = "T_ARRAY_ROW";
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS "
                        + table
                        + "("
                        + "ID INT PRIMARY KEY NOT ENFORCED,\n"
                        + "NAMES ARRAY<ROW<NAME STRING, MARK STRING>>\n"
                        + ") WITH ("
                        + "'changelog-producer'='full-compaction',"
                        + "'changelog-producer.compaction-interval' = '1s',"
                        + "'changelog-producer.row-deduplicate' = 'true')");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM %s", table));

        sql("INSERT INTO %s VALUES (1, ARRAY[('a','mark1')]);", table);
        assertThat(iterator.collect(1).stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactlyInAnyOrder("+I[1, [+I[a, mark1]]]");

        sql("INSERT INTO %s VALUES (1, ARRAY[('b', 'mark2')])", table);
        assertThat(iterator.collect(2).stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactly("-U[1, [+I[a, mark1]]]", "+U[1, [+I[b, mark2]]]");

        sql("INSERT INTO %s VALUES (1, ARRAY[('b', 'mark2')]), (2, ARRAY[('c', 'mark3')])", table);
        assertThat(iterator.collect(1).stream().map(Row::toString).collect(Collectors.toList()))
                .containsExactly("+I[2, [+I[c, mark3]]]");
    }
}
