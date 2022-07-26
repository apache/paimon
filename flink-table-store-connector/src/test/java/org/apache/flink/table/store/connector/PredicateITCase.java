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

import static org.assertj.core.api.Assertions.assertThat;

/** Predicate ITCase. */
public class PredicateITCase extends CatalogITCaseBase {

    @Test
    public void testPkFilterBucket() throws Exception {
        sql("CREATE TABLE T (a INT PRIMARY KEY NOT ENFORCED, b INT) WITH ('bucket' = '5')");
        innerTest();
    }

    @Test
    public void testNoPkFilterBucket() throws Exception {
        sql("CREATE TABLE T (a INT, b INT) WITH ('bucket' = '5', 'bucket-key'='a')");
        innerTest();
    }

    @Test
    public void testAppendFilterBucket() throws Exception {
        sql(
                "CREATE TABLE T (a INT, b INT) WITH ('bucket' = '5', 'bucket-key'='a', 'write-mode'='append-only')");
        innerTest();
    }

    private void innerTest() throws Exception {
        sql("INSERT INTO T VALUES (1, 2), (3, 4), (5, 6), (7, 8), (9, 10)");
        assertThat(sql("SELECT * FROM T WHERE a = 1")).containsExactlyInAnyOrder(Row.of(1, 2));
        assertThat(sql("SELECT * FROM T WHERE a = 3")).containsExactlyInAnyOrder(Row.of(3, 4));
        assertThat(sql("SELECT * FROM T WHERE a = 5")).containsExactlyInAnyOrder(Row.of(5, 6));
        assertThat(sql("SELECT * FROM T WHERE a = 7")).containsExactlyInAnyOrder(Row.of(7, 8));
        assertThat(sql("SELECT * FROM T WHERE a = 9")).containsExactlyInAnyOrder(Row.of(9, 10));
    }
}
