/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for first row merge engine. */
public class FirstRowITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row');",
                "CREATE TABLE IF NOT EXISTS T1 ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'changelog-producer' = 'lookup');",
                "CREATE TABLE IF NOT EXISTS T2 ("
                        + "a INT, b INT, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='first-row', 'changelog-producer' = 'full-compaction', 'full-compaction.delta-commits' = '3');");
    }

    @Test
    public void testBatchQuery() {
        batchSql("INSERT INTO T VALUES (1, 1, '1'), (1, 2, '2')");
        List<Row> result = batchSql("SELECT * FROM T");
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 1, "1"));

        result = batchSql("SELECT c FROM T");
        assertThat(result).containsExactlyInAnyOrder(Row.of("1"));
    }

    @Test
    public void testStreamingReadOnFullCompaction() throws Exception {
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T2");

        sql("INSERT INTO T2 VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (1, 4, '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "2"));

        sql("INSERT INTO T2 VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (3, 3, '3')");
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 3, 3, "3"));
    }

    @Test
    public void testStreamingReadOnLookup() throws Exception {
        BlockingIterator<Row, Row> iterator = streamSqlBlockIter("SELECT * FROM T1");

        sql("INSERT INTO T1 VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (1, 4, '4')");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(
                        Row.ofKind(RowKind.INSERT, 1, 1, "1"),
                        Row.ofKind(RowKind.INSERT, 2, 2, "2"));

        sql("INSERT INTO T1 VALUES(1, 1, '1'), (2, 2, '2'), (1, 3, '3'), (3, 3, '3')");
        assertThat(iterator.collect(1))
                .containsExactlyInAnyOrder(Row.ofKind(RowKind.INSERT, 3, 3, "3"));
    }
}
