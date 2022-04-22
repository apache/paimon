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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for partial update. */
public class PartialUpdateITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "j INT, k INT, a INT, b INT, c STRING, PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update');");
    }

    @Test
    public void testMergeInMemory() throws ExecutionException, InterruptedException {
        bEnv.executeSql(
                        "INSERT INTO T VALUES "
                                + "(1, 2, 3, CAST(NULL AS INT), '5'), "
                                + "(1, 2, CAST(NULL AS INT), 6, CAST(NULL AS STRING))")
                .await();
        List<Row> result = iteratorToList(bEnv.from("T").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, 3, 6, "5"));
    }

    @Test
    public void testMergeRead() throws ExecutionException, InterruptedException {
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 3, CAST(NULL AS INT), CAST(NULL AS STRING))")
                .await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 4, 5, CAST(NULL AS STRING))").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 4, CAST(NULL AS INT), '6')").await();

        List<Row> result = iteratorToList(bEnv.from("T").execute().collect());
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, 4, 5, "6"));
    }

    @Test
    public void testMergeCompaction() throws ExecutionException, InterruptedException {
        // Wait compaction
        bEnv.executeSql("ALTER TABLE T SET ('commit.force-compact'='true')");

        // key 1 2
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 3, CAST(NULL AS INT), CAST(NULL AS STRING))")
                .await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 4, 5, CAST(NULL AS STRING))").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 2, 4, CAST(NULL AS INT), '6')").await();

        // key 1 3
        bEnv.executeSql("INSERT INTO T VALUES (1, 3, CAST(NULL AS INT), 1, '1')").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 3, 2, 3, CAST(NULL AS STRING))").await();
        bEnv.executeSql("INSERT INTO T VALUES (1, 3, CAST(NULL AS INT), 4, CAST(NULL AS STRING))")
                .await();

        List<Row> result = iteratorToList(bEnv.from("T").execute().collect());
        assertThat(result)
                .containsExactlyInAnyOrder(Row.of(1, 2, 4, 5, "6"), Row.of(1, 3, 2, 4, "1"));
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T").execute().print(),
                "Partial update continuous reading is not supported");
    }
}
