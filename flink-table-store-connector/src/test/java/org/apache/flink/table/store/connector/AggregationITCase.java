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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.CollectionUtil.iteratorToList;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for partial update. */
public class AggregationITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T3 ( "
                        + " a STRING, "
                        + " b INT, "
                        + " c INT, "
                        + " PRIMARY KEY (a) NOT ENFORCED )"
                        + " WITH ("
                        + " 'merge-engine'='aggregation' ,"
                        + " 'b.aggregate-function'='sum' ,"
                        + " 'c.aggregate-function'='sum' "
                        + " );");
    }

    @Test
    public void testMergeInMemory() throws ExecutionException, InterruptedException {
        bEnv.executeSql("INSERT INTO T3 VALUES " + "('pk1',1, 2), " + "('pk1',1, 2)").await();
        List<Row> result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 2, 4));
    }

    @Test
    public void testMergeRead() throws ExecutionException, InterruptedException {
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 2)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',1, 4)").await();
        bEnv.executeSql("INSERT INTO T3 VALUES ('pk1',2, 0)").await();
        List<Row> result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 4, 6));
    }

    @Test
    public void testMergeCompaction() throws ExecutionException, InterruptedException {
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

        List<Row> result = iteratorToList(bEnv.from("T3").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of("pk1", 11, 12), Row.of("pk2", 19, 11));
    }
}
