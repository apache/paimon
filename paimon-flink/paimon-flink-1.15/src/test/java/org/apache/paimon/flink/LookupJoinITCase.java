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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for (async) lookup join. */
@RunWith(Parameterized.class)
public class LookupJoinITCase extends CatalogITCaseBase {

    @Parameterized.Parameter public boolean asyncLookup;

    @Parameterized.Parameters(name = "asyncLookup={0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    @Override
    public List<String> ddl() {
        List<String> ddls = new ArrayList<>();
        ddls.add("CREATE TABLE T (i INT, `proctime` AS PROCTIME())");
        if (asyncLookup) {
            ddls.add(
                    "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                            + " ('continuous.discovery-interval'='1 ms', 'lookup.async'='true')");
        } else {
            ddls.add(
                    "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH"
                            + " ('continuous.discovery-interval'='1 ms')");
        }
        return ddls;
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testLookup() throws Exception {
        batchSql("INSERT INTO DIM VALUES (1, 11, 111, 1111), (2, 22, 222, 2222)");

        String query =
                "SELECT T.i, D.j, D.k1, D.k2 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        try (BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(sEnv.executeSql(query).collect())) {

            if (asyncLookup) {
                // for async lookup, we have to wait for refresh each time to ensure we can get the
                // correct results for testing.
                batchSql("INSERT INTO T VALUES (1)");
                Thread.sleep(2000); // wait refresh

                batchSql("INSERT INTO T VALUES (2)");
                Thread.sleep(2000); // wait refresh

                batchSql("INSERT INTO T VALUES (3)");
            } else {
                batchSql("INSERT INTO T VALUES (1), (2), (3)");
            }

            assertThat(iterator.collect(3))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 11, 111, 1111),
                            Row.of(2, 22, 222, 2222),
                            Row.of(3, null, null, null));

            batchSql("INSERT INTO DIM VALUES (2, 44, 444, 4444), (3, 33, 333, 3333)");

            Thread.sleep(2000); // wait refresh

            batchSql("INSERT INTO T VALUES (1), (2), (3), (4)");
            assertThat(iterator.collect(4))
                    .containsExactlyInAnyOrder(
                            Row.of(1, 11, 111, 1111),
                            Row.of(2, 44, 444, 4444),
                            Row.of(3, 33, 333, 3333),
                            Row.of(4, null, null, null));
        }
    }
}
