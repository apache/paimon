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

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for first row merge engine. */
public class FirstRowITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'file.format'='avro', 'changelog-producer' = 'lookup');",
                "CREATE TABLE IF NOT EXISTS T_seq ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'changelog-producer' = 'lookup', 'sequence.field' = 'b');");
    }

    @Test
    public void testIllegal() {
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE ILLEGAL_T (a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                                                + " WITH ('merge-engine'='first-row')"))
                .hasRootCauseMessage(
                        "Only support 'lookup' changelog-producer on FIRST_MERGE merge engine");
    }

    @Test
    public void testBatchQuery() {
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2')");
        List<Row> result = batchSql("SELECT * FROM T");
        assertThat(result).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 1, 1, "1"));

        result = batchSql("SELECT c FROM T");
        assertThat(result).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "1"));
    }

    @Test
    public void testStreamingRead() throws Exception {
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T");

        sql("INSERT INTO T VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (1, 4, '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "2"));

        sql(
                "INSERT INTO T VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (3, 3, '3'), (4, 4, '4'), (5, 5, '5'), (6, 6, '6'), (7, 7, '7')");
        assertThat(iterator.collect(5))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 3, 3, "3"),
                        Row.ofKind(RowKind.INSERT, 4, 4, "4"),
                        Row.ofKind(RowKind.INSERT, 5, 5, "5"),
                        Row.ofKind(RowKind.INSERT, 6, 6, "6"),
                        Row.ofKind(RowKind.INSERT, 7, 7, "7"));

        sql("INSERT INTO T VALUES(7, 7, '8'), (8, 8, '8')");
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 8, 8, "8"));
    }

    @Test
    public void testBatchQueryFirstRowWithSequenceFieldEnabled() {
        batchSql("INSERT INTO T_seq VALUES (1, 2, '2'), (1, 1, '1')");
        List<Row> result = batchSql("SELECT * FROM T_seq");
        assertThat(result).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 1, 1, "1"));

        result = batchSql("SELECT c FROM T_seq");
        assertThat(result).containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, "1"));
    }

    @Test
    public void testStreamingReadWithSequenceFieldEnabled() throws Exception {
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter("SELECT * FROM T_seq /*+ OPTIONS('scan.snapshot-id'='1') */");

        sql("INSERT INTO T_seq VALUES(1, 3, '3')");
        sql("INSERT INTO T_seq VALUES(2, 2, '2')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 3, "3"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "2"));
        sql("INSERT INTO T_seq VALUES(1, 1, '1')");
        sql("INSERT INTO T_seq VALUES(2, 4, '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 3, "3"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 1, "1"));
    }

    @Test
    public void testLocalMerge() {
        sql(
                "CREATE TABLE IF NOT EXISTS T1 ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a, b) NOT ENFORCED)"
                        + " PARTITIONED BY (b) WITH ('merge-engine'='first-row', 'local-merge-buffer-size' = '1m',"
                        + " 'file.format'='avro', 'changelog-producer' = 'lookup');");
        batchSql("INSERT INTO T1 VALUES (1, 1, '1'), (1, 1, '2'), (2, 3, '3')");
        List<Row> result = batchSql("SELECT * FROM T1");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 2, 3, "3"));
    }

    @Test
    public void testLocalMergeWithSequenceFieldEnabled() {
        sql(
                "CREATE TABLE T_seq_local ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'local-merge-buffer-size' = '1m',"
                        + " 'sequence.field'= 'b', 'changelog-producer' = 'lookup');");
        batchSql("INSERT INTO T_seq_local VALUES (1, 3, '1'), (1, 2, '2'), (2, 3, '3')");
        List<Row> result = batchSql("SELECT * FROM T_seq_local");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 2, "2"),
                        Row.ofKind(RowKind.INSERT, 2, 3, "3"));
        batchSql("INSERT INTO T_seq_local VALUES (1, 1, '1'), (2, 2, '2')");
        result = batchSql("SELECT * FROM T_seq_local");
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "2"));
    }

    @Test
    public void testIgnoreDelete() {
        sql(
                "CREATE TABLE IF NOT EXISTS T1 ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'ignore-delete' = 'true',"
                        + " 'changelog-producer' = 'lookup');");

        List<Row> input =
                ImmutableList.of(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, 1, "1"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 2, "2"));

        String id = TestValuesTableFactory.registerData(input);
        sql(
                "CREATE TEMPORARY TABLE source (a INT, b INT, c STRING) WITH "
                        + "('connector'='values', 'bounded'='true', 'data-id'='%s')",
                id);

        batchSql("INSERT INTO T1 SELECT * FROM source");
        List<Row> result = batchSql("SELECT * FROM T1");

        assertThat(result).containsExactly(Row.ofKind(RowKind.INSERT, 1, 1, "1"));
    }
}
