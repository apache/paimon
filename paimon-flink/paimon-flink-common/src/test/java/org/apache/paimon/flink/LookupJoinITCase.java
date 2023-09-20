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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for lookup join. */
public class LookupJoinITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE T (i INT, `proctime` AS PROCTIME())",
                "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('continuous.discovery-interval'='1 ms')",
                "CREATE TABLE PARTITIONED_DIM (i INT, j INT, k1 INT, k2 INT, PRIMARY KEY (i, j) NOT ENFORCED) "
                        + "PARTITIONED BY (`i`) WITH ('continuous.discovery-interval'='1 ms')");
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testLookupEmptyTable() throws Exception {
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");

        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null, null),
                        Row.of(2, null, null, null),
                        Row.of(3, null, null, null));

        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (4)");
        result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(4, null, null, null));
        iterator.close();
    }

    @Test
    public void testLookup() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, null, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 44, 444, 4444),
                        Row.of(3, 33, 333, 3333),
                        Row.of(4, null, null, null));

        iterator.close();
    }

    @Test
    public void testLookupWithLatest() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM /*+ OPTIONS('scan.mode'='latest') */"
                        + " for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, null, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 44, 444, 4444),
                        Row.of(3, 33, 333, 3333),
                        Row.of(4, null, null, null));

        iterator.close();
    }

    @Test
    public void testLookupProjection() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 44, 444),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testLookupFilterPk() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i AND D.i > 2";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null), Row.of(2, null, null), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null),
                        Row.of(2, null, null),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testLookupFilterSelect() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i AND D.k1 > 111";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null), Row.of(2, 22, 222), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null),
                        Row.of(2, 44, 444),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testLookupFilterUnSelect() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i AND D.k2 > 1111";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null), Row.of(2, 22, 222), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null),
                        Row.of(2, 44, 444),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testLookupFilterUnSelectAndUpdate() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i AND D.k2 < 4444";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, null, null),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testNonPkLookup() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT D.i, T.i, D.k1, D.k2 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, 22, 333, 3333),
                        Row.of(null, 33, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(null, 22, null, null),
                        Row.of(3, 33, 333, 3333),
                        Row.of(2, 44, 444, 4444));

        iterator.close();
    }

    @Test
    public void testNonPkLookupProjection() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT T.i, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, 111), Row.of(22, 222), Row.of(22, 333), Row.of(33, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, 111), Row.of(22, null), Row.of(33, 333), Row.of(44, 444));

        iterator.close();
    }

    @Test
    public void testNonPkLookupFilterPk() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT T.i, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j AND D.i > 2";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of(11, null), Row.of(22, 333), Row.of(33, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, null), Row.of(22, null), Row.of(33, 333), Row.of(44, null));

        iterator.close();
    }

    @Test
    public void testNonPkLookupFilterSelect() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT T.i, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j AND D.k1 > 111";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, null), Row.of(22, 222), Row.of(22, 333), Row.of(33, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, null), Row.of(22, null), Row.of(33, 333), Row.of(44, 444));

        iterator.close();
    }

    @Test
    public void testNonPkLookupFilterUnSelect() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT T.i, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j AND D.k2 > 1111";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, null), Row.of(22, 222), Row.of(22, 333), Row.of(33, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, null), Row.of(22, null), Row.of(33, 333), Row.of(44, 444));

        iterator.close();
    }

    @Test
    public void testNonPkLookupFilterUnSelectAndUpdate() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222), (3, 22, 333, 3333)");

        String query =
                "SELECT T.i, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.j AND D.k2 < 4444";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (11), (22), (33)");
        List<Row> result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, 111), Row.of(22, 222), Row.of(22, 333), Row.of(33, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (11), (22), (33), (44)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(11, 111), Row.of(22, null), Row.of(33, 333), Row.of(44, null));

        iterator.close();
    }

    @Test
    public void testRepeatRefresh() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, null, null));

        sql("INSERT INTO DIM VALUES (2, 44, 444, 4444)");
        sql("INSERT INTO DIM VALUES (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 44, 444),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();
    }

    @Test
    public void testLookupPartialUpdateIllegal() {
        sql(
                "CREATE TABLE DIM2 (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('merge-engine'='partial-update','continuous.discovery-interval'='1 ms')");
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM2 for system_time as of T.proctime AS D ON T.i = D.i";
        assertThatThrownBy(() -> sEnv.executeSql(query))
                .hasRootCauseMessage(
                        "Partial update streaming"
                                + " reading is not supported. "
                                + "You can use 'lookup' or 'full-compaction' changelog producer to support streaming reading. "
                                + "('input' changelog producer is also supported, but only returns input records.)");
    }

    @Test
    public void testLookupPartialUpdate() throws Exception {
        sql(
                "CREATE TABLE DIM2 (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('merge-engine'='partial-update',"
                        + " 'changelog-producer'='full-compaction',"
                        + " 'changelog-producer.compaction-interval'='1 s',"
                        + " 'continuous.discovery-interval'='10 ms')");
        sql("INSERT INTO DIM2 VALUES (1, CAST(NULL AS INT), 111, CAST(NULL AS INT))");
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM2 for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());
        sql("INSERT INTO T VALUES (1)");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(1, null, 111, null));

        sql("INSERT INTO DIM2 VALUES (1, 11, CAST(NULL AS INT), 1111)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1)");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(1, 11, 111, 1111));

        iterator.close();
    }

    @Test
    public void testRetryLookup() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
                        + " 'retry-strategy'='fixed_delay', 'fixed-delay'='1s','max-attempts'='60') */"
                        + " T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        Thread.sleep(2000); // wait
        sql("INSERT INTO DIM VALUES (3, 33, 333, 3333)");
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, 33, 333, 3333));

        iterator.close();
    }

    @Test
    public void testAsyncRetryLookup() throws Exception {
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
                        + " 'retry-strategy'='fixed_delay', 'output-mode'='allow_unordered', 'fixed-delay'='1s','max-attempts'='60') */"
                        + " T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM /*+ OPTIONS('lookup.async'='true') */ for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (3)");
        sql("INSERT INTO T VALUES (2)");
        sql("INSERT INTO T VALUES (1)");
        Thread.sleep(2000); // wait
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111, 1111), Row.of(2, 22, 222, 2222));

        sql("INSERT INTO DIM VALUES (3, 33, 333, 3333)");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(3, 33, 333, 3333));

        iterator.close();
    }

    @Test
    public void testLookupPartitionedTable() throws Exception {
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN PARTITIONED_DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");

        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null, null),
                        Row.of(2, null, null, null),
                        Row.of(3, null, null, null));

        sql("INSERT INTO PARTITIONED_DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (4)");
        result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(4, null, null, null));
        iterator.close();
    }

    @Test
    public void testLookupNonPkAppendTable() throws Exception {
        sql(
                "CREATE TABLE DIM_NO_PK (i INT, j INT, k1 INT, k2 INT) "
                        + "PARTITIONED BY (`i`) WITH ('continuous.discovery-interval'='1 ms')");
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM_NO_PK for system_time as of T.proctime AS D ON T.i "
                        + "= D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");

        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, null, null, null),
                        Row.of(2, null, null, null),
                        Row.of(3, null, null, null));

        sql(
                "INSERT INTO DIM_NO_PK VALUES (1, 11, 111, 1111), (1, 12, 112, 1112), (1, 11, 111, 1111)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (4)");
        result = iterator.collect(5);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(1, 11, 111, 1111),
                        Row.of(1, 12, 112, 1112),
                        Row.of(2, null, null, null),
                        Row.of(4, null, null, null));
        iterator.close();
    }
}
