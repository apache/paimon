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

import org.apache.paimon.flink.FlinkConnectorOptions.LookupCacheMode;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for lookup join. */
public class LookupJoinITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Collections.singletonList("CREATE TABLE T (i INT, `proctime` AS PROCTIME())");
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    private void initTable(LookupCacheMode cacheMode) {
        String dim =
                "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('continuous.discovery-interval'='1 ms' %s)";
        String partitioned =
                "CREATE TABLE PARTITIONED_DIM (i INT, j INT, k1 INT, k2 INT, PRIMARY KEY (i, j) NOT ENFORCED)"
                        + "PARTITIONED BY (`i`) WITH ('continuous.discovery-interval'='1 ms' %s)";

        String fullOption = ", 'lookup.cache' = 'full'";

        String lruOption = ", 'changelog-producer'='lookup'";

        switch (cacheMode) {
            case FULL:
                tEnv.executeSql(String.format(dim, fullOption));
                tEnv.executeSql(String.format(partitioned, fullOption));
                break;
            case AUTO:
                tEnv.executeSql(String.format(dim, lruOption));
                tEnv.executeSql(String.format(partitioned, lruOption));
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupEmptyTable(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookup(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupIgnoreScanOptions(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String scanOption;
        if (ThreadLocalRandom.current().nextBoolean()) {
            scanOption = "'scan.mode'='latest'";
        } else {
            scanOption = "'scan.snapshot-id'='2'";
        }
        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM /*+ OPTIONS("
                        + scanOption
                        + ") */"
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupProjection(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupFilterPk(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupFilterSelect(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupFilterUnSelect(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupFilterUnSelectAndUpdate(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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
        initTable(LookupCacheMode.AUTO);
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
        initTable(LookupCacheMode.FULL);
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
        initTable(LookupCacheMode.FULL);
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
        initTable(LookupCacheMode.FULL);
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
        initTable(LookupCacheMode.FULL);
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
        initTable(LookupCacheMode.FULL);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testRepeatRefresh(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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
        testLookupPartialUpdate("none");
        testLookupPartialUpdate("zstd");
    }

    private void testLookupPartialUpdate(String compression) throws Exception {
        sql(
                "CREATE TABLE DIM2 (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('merge-engine'='partial-update',"
                        + " 'changelog-producer'='full-compaction',"
                        + " 'changelog-producer.compaction-interval'='1 s',"
                        + String.format(" 'lookup.cache-spill-compression'='%s',", compression)
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

        sql("DROP TABLE DIM2");
        sql("TRUNCATE TABLE T");
    }

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testRetryLookup(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testAsyncRetryLookup(LookupCacheMode cacheMode) throws Exception {
        initTable(cacheMode);
        sql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
                        + " 'retry-strategy'='fixed_delay', 'output-mode'='allow_unordered', 'fixed-delay'='3s','max-attempts'='30') */"
                        + " T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM /*+ OPTIONS('lookup.async'='true') */ for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (3)");
        sql("INSERT INTO T VALUES (2)");
        sql("INSERT INTO T VALUES (1)");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111, 1111), Row.of(2, 22, 222, 2222));

        sql("INSERT INTO DIM VALUES (3, 33, 333, 3333)");
        assertThat(iterator.collect(1, 10, TimeUnit.MINUTES))
                .containsExactlyInAnyOrder(Row.of(3, 33, 333, 3333));

        iterator.close();
    }

    @Test
    public void testLookupPartitionedTable() throws Exception {
        initTable(LookupCacheMode.AUTO);
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

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testLookupMaxPtPartitionedTable(LookupCacheMode mode) throws Exception {
        boolean testDynamicBucket = ThreadLocalRandom.current().nextBoolean();
        String primaryKeys;
        String bucket;
        if (testDynamicBucket) {
            primaryKeys = "k";
            bucket = "-1";
        } else {
            primaryKeys = "pt, k";
            bucket = "1";
        }
        sql(
                "CREATE TABLE PARTITIONED_DIM (pt STRING, k INT, v INT, PRIMARY KEY (%s) NOT ENFORCED)"
                        + "PARTITIONED BY (`pt`) WITH ("
                        + "'bucket' = '%s', "
                        + "'lookup.dynamic-partition' = 'max_pt()', "
                        + "'lookup.dynamic-partition.refresh-interval' = '1 ms', "
                        + "'lookup.cache' = '%s', "
                        + "'continuous.discovery-interval'='1 ms')",
                primaryKeys, bucket, mode);
        String query =
                "SELECT T.i, D.v FROM T LEFT JOIN PARTITIONED_DIM for system_time as of T.proctime AS D ON T.i = D.k";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO PARTITIONED_DIM VALUES ('1', 1, 2)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1)");
        List<Row> result = iterator.collect(1);
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2));

        sql("INSERT INTO PARTITIONED_DIM VALUES ('2', 1, 3)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1)");
        result = iterator.collect(1);
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 3));

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

    @Test
    public void testWithSequenceFieldTable() throws Exception {
        sql(
                "CREATE TABLE DIM_WITH_SEQUENCE (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('continuous.discovery-interval'='1 ms', 'sequence.field' = 'j')");
        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM_WITH_SEQUENCE for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, null, null, null));

        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (2, 11, 444, 4444), (3, 33, 333, 3333)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222), // not change
                        Row.of(3, 33, 333, 3333),
                        Row.of(4, null, null, null));

        iterator.close();
    }

    @Test
    public void testAsyncRetryLookupWithSequenceField() throws Exception {
        sql(
                "CREATE TABLE DIM_WITH_SEQUENCE (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('continuous.discovery-interval'='1 ms', 'sequence.field' = 'j')");
        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
                        + " 'retry-strategy'='fixed_delay', 'output-mode'='allow_unordered', 'fixed-delay'='3s','max-attempts'='60') */"
                        + " T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM_WITH_SEQUENCE /*+ OPTIONS('lookup.async'='true') */ for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (3)");
        sql("INSERT INTO T VALUES (2)");
        sql("INSERT INTO T VALUES (1)");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111, 1111), Row.of(2, 22, 222, 2222));

        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (3, 33, 333, 3333)");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(3, 33, 333, 3333));

        iterator.close();
    }

    @Test
    public void testAsyncRetryLookupSecKeyWithSequenceField() throws Exception {
        sql(
                "CREATE TABLE DIM_WITH_SEQUENCE (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                        + " ('continuous.discovery-interval'='1 ms', 'sequence.field' = 'j')");
        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (1, 1, 111, 1111), (2, 2, 111, 2222)");

        String query =
                "SELECT /*+ LOOKUP('table'='D', 'retry-predicate'='lookup_miss',"
                        + " 'retry-strategy'='fixed_delay', 'output-mode'='allow_unordered', 'fixed-delay'='3s','max-attempts'='60') */"
                        + " T.i, D.i, D.j, D.k2 FROM T LEFT JOIN DIM_WITH_SEQUENCE /*+ OPTIONS('lookup.async'='true') */ for system_time as of T.proctime AS D ON T.i = D.k1";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (111)");
        sql("INSERT INTO T VALUES (333)");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(111, 1, 1, 1111), Row.of(111, 2, 2, 2222));

        sql("INSERT INTO DIM_WITH_SEQUENCE VALUES (2, 1, 111, 3333), (3, 3, 333, 3333)");
        sql("INSERT INTO T VALUES (111)");
        assertThat(iterator.collect(3))
                .containsExactlyInAnyOrder(
                        Row.of(111, 1, 1, 1111), Row.of(111, 2, 2, 2222), Row.of(333, 3, 3, 3333));

        iterator.close();
    }

    @ParameterizedTest
    @EnumSource(LookupCacheMode.class)
    public void testPartialCacheBucketKeyOrder(LookupCacheMode mode) throws Exception {
        sql(
                "CREATE TABLE DIM (k2 INT, k1 INT, j INT , i INT, PRIMARY KEY(i, j) NOT ENFORCED) WITH"
                        + " ('continuous.discovery-interval'='1 ms', 'lookup.cache'='%s', 'bucket' = '2', 'bucket-key' = 'j')",
                mode);

        sql("CREATE TABLE T2 (j INT, i INT, `proctime` AS PROCTIME())");

        sql("INSERT INTO DIM VALUES (1111, 111, 11, 1), (2222, 222, 22, 2)");

        String query =
                "SELECT T2.i, D.j, D.k1, D.k2 FROM T2 LEFT JOIN DIM for system_time as of T2.proctime AS D ON T2.i = D.i and T2.j = D.j";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T2 VALUES (11, 1), (22, 2), (33, 3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111, 1111),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, null, null, null));

        sql("INSERT INTO DIM VALUES (2222, 222, 11, 1), (3333, 333, 33, 3)");
        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T2 VALUES (11, 1), (22, 2), (33, 3), (44, 4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 222, 2222),
                        Row.of(2, 22, 222, 2222),
                        Row.of(3, 33, 333, 3333),
                        Row.of(4, null, null, null));

        iterator.close();
    }
}
