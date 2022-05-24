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

import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for partial update. */
public class AggregationITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {

        String ddl1 =
                "CREATE TABLE IF NOT EXISTS T3 ( "
                        + " a STRING, "
                        + " b BIGINT, "
                        + " c INT, "
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation' ,"
                        + " 'b.aggregate-function'='sum' ,"
                        + " 'c.aggregate-function'='sum' "
                        + " );";
        String ddl2 =
                "CREATE TABLE IF NOT EXISTS T4 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " c DOUBLE,"
                        + " PRIMARY KEY (a, b) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'c.aggregate-function' = 'sum'"
                        + " );";
        String ddl3 =
                "CREATE TABLE IF NOT EXISTS T5 ( "
                        + " a STRING,"
                        + " b INT,"
                        + " c DOUBLE,"
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation',"
                        + " 'b.aggregate-function' = 'sum'"
                        + " );";
        List<String> lists = new ArrayList<>();
        lists.add(ddl1);
        lists.add(ddl2);
        lists.add(ddl3);
        return lists;
    }

    @Test
    public void testCreateAggregateFunction() throws ExecutionException, InterruptedException {
        List<Row> result;

        // T5
        try {
            bEnv.executeSql("INSERT INTO T5 VALUES " + "('pk1',1, 2.0), " + "('pk1',1, 2.0)")
                    .await();
            throw new AssertionError("create table T5 should failed");
        } catch (IllegalArgumentException e) {
            assert ("should  set aggregate function for every column not part of primary key"
                    .equals(e.getLocalizedMessage()));
        }
    }

    @Test
    public void testMergeInMemory() throws ExecutionException, InterruptedException {
        List<Row> result;
        // T3
        bEnv.executeSql("INSERT INTO T3 VALUES " + "('pk1',1, 2), " + "('pk1',1, 2)").await();
        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 2L, 4));

        // T4
        bEnv.executeSql("INSERT INTO T4 VALUES " + "('pk1',1, 2.0), " + "('pk1',1, 2.0)").await();
        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 1, 4.0));
    }

    @Test
    public void testMergeRead() throws ExecutionException, InterruptedException {
        List<Row> result;
        // T3
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 2)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 4)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',2, 0)").await();
        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 4L, 6));

        // T4
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1',1, 2.0)").await();
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1',1, 4.0)").await();
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1',1, 0.0)").await();
        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 1, 6.0));
    }

    @Test
    public void testMergeCompaction() throws ExecutionException, InterruptedException {
        List<Row> result;

        // T3
        // Wait compaction
        bEnv.executeSql("ALTER TABLE T3 SET ('commit.force-compact'='true')");

        // key pk1
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 3, 1)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 4, 5)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1', 4, 6)").await();

        // key pk2
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 6,7)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 9,0)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk2', 4,4)").await();

        result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of("pk1", 11L, 12), Row.of("pk2", 19L, 11));

        // T4
        // Wait compaction
        bEnv.executeSql("ALTER TABLE T4 SET ('commit.force-compact'='true')");

        // key pk1_3
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1', 3, 1.0)").await();
        // key pk1_4
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1', 4, 5.0)").await();
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk1', 4, 6.0)").await();
        // key pk2_4
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk2', 4,4.0)").await();
        // key pk2_2
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk2', 2,7.0)").await();
        bEnv.executeSql("INSERT INTO T4 VALUES ('pk2', 2,0)").await();

        result = iteratorToList(bEnv.from("T4").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of("pk1", 3, 1.0),
                        Row.of("pk1", 4, 11.0),
                        Row.of("pk2", 4, 4.0),
                        Row.of("pk2", 2, 7.0));
    }
}
