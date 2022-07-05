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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Predicate ITCase. */
public class PredicateITCase extends AbstractTestBase {

    private TableEnvironment tEnv;

    @Before
    public void before() throws IOException {
        tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG TABLE_STORE WITH ("
                                + "'type'='table-store', 'warehouse'='%s')",
                        TEMPORARY_FOLDER.newFolder().toURI()));
        tEnv.useCatalog("TABLE_STORE");
    }

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
        assertThat(sql("SELECT * FROM T WHERE a = 5")).containsExactlyInAnyOrder(Row.of(5, 6));
    }

    private List<Row> sql(String query, Object... args) throws Exception {
        try (CloseableIterator<Row> iter = tEnv.executeSql(String.format(query, args)).collect()) {
            return ImmutableList.copyOf(iter);
        }
    }
}
