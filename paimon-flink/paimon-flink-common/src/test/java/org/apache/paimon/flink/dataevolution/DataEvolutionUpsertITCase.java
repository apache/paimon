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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.flink.action.ActionITCaseBase;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;

/** ITCase for data evolution streaming upsert via {@code data-evolution.upsert-keys}. */
public class DataEvolutionUpsertITCase extends ActionITCaseBase {

    @BeforeEach
    public void setup() throws Exception {
        init(warehouse);
    }

    @Test
    public void testBasicUpsert() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        upsert("T", "id", "(1, 'a_new', 10.0)", "(3, 'c_new', 30.0)");

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "a_new", 10.0), Row.of(2, "b", 2.0), Row.of(3, "c_new", 30.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testInsertOnly() throws Exception {
        createTable("T", false);

        upsert("T", "id", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        List<Row> expected =
                Arrays.asList(Row.of(1, "a", 1.0), Row.of(2, "b", 2.0), Row.of(3, "c", 3.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testMixedInsertAndUpdate() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        upsert(
                "T",
                "id",
                "(2, 'b_new', 20.0)",
                "(3, 'c_new', 30.0)",
                "(4, 'd', 4.0)",
                "(5, 'e', 5.0)");

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "a", 1.0),
                        Row.of(2, "b_new", 20.0),
                        Row.of(3, "c_new", 30.0),
                        Row.of(4, "d", 4.0),
                        Row.of(5, "e", 5.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testMultipleUpserts() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        upsert("T", "id", "(1, 'a2', 10.0)", "(4, 'd', 4.0)");

        upsert("T", "id", "(2, 'b2', 20.0)", "(4, 'd2', 40.0)");

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "a2", 10.0),
                        Row.of(2, "b2", 20.0),
                        Row.of(3, "c", 3.0),
                        Row.of(4, "d2", 40.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testCompositeKey() throws Exception {
        Map<String, String> options = baseOptions();
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "sub_id INT", "name STRING"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options));

        bEnv.executeSql("INSERT INTO T VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')").await();

        sEnv.executeSql(
                        "INSERT INTO T /*+ OPTIONS('data-evolution.upsert-keys'='id,sub_id') */ "
                                + "VALUES (1, 2, 'b_new'), (2, 1, 'c_new'), (3, 1, 'd')")
                .await();

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, 1, "a"),
                        Row.of(1, 2, "b_new"),
                        Row.of(2, 1, "c_new"),
                        Row.of(3, 1, "d"));
        testBatchRead("SELECT * FROM T ORDER BY id, sub_id", expected);
    }

    @Test
    public void testPartialColumnUpdate() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        // Update only 'name' column; 'value' is NULL meaning "don't change"
        upsert(
                "T",
                "id",
                "(1, 'a_new', CAST(NULL AS DOUBLE))",
                "(3, 'c_new', CAST(NULL AS DOUBLE))");

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "a_new", 1.0), Row.of(2, "b", 2.0), Row.of(3, "c_new", 3.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testPartialColumnUpdateValueOnly() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)", "(3, 'c', 3.0)");

        // Update only 'value' column; 'name' is NULL meaning "don't change"
        upsert("T", "id", "(1, CAST(NULL AS STRING), 10.0)", "(2, CAST(NULL AS STRING), 20.0)");

        List<Row> expected =
                Arrays.asList(Row.of(1, "a", 10.0), Row.of(2, "b", 20.0), Row.of(3, "c", 3.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testPartialColumnUpdateThenFullUpdate() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)");

        // First: partial update (only name)
        upsert("T", "id", "(1, 'a_v2', CAST(NULL AS DOUBLE))");

        // Second: full update (all columns non-NULL)
        upsert("T", "id", "(2, 'b_v2', 22.0)");

        List<Row> expected = Arrays.asList(Row.of(1, "a_v2", 1.0), Row.of(2, "b_v2", 22.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testPartialColumnUpdateWithInsert() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)");

        // Mix partial update and new insert in the same upsert
        upsert("T", "id", "(1, 'a_new', CAST(NULL AS DOUBLE))", "(3, 'new_row', 3.0)");

        List<Row> expected =
                Arrays.asList(
                        Row.of(1, "a_new", 1.0), Row.of(2, "b", 2.0), Row.of(3, "new_row", 3.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    @Test
    public void testMultiplePartialUpdates() throws Exception {
        createTable("T", false);
        batchInsert("T", "(1, 'a', 1.0)", "(2, 'b', 2.0)");

        // First partial update: only name
        upsert("T", "id", "(1, 'a_v2', CAST(NULL AS DOUBLE))");

        // Second partial update: only value
        upsert("T", "id", "(1, CAST(NULL AS STRING), 100.0)");

        List<Row> expected = Arrays.asList(Row.of(1, "a_v2", 100.0), Row.of(2, "b", 2.0));
        testBatchRead("SELECT * FROM T ORDER BY id", expected);
    }

    private void createTable(String tableName, boolean partitioned) {
        List<String> fields = Arrays.asList("id INT", "name STRING", "`value` DOUBLE");
        List<String> partitionKeys =
                partitioned ? Collections.singletonList("dt") : Collections.emptyList();
        if (partitioned) {
            fields = Arrays.asList("id INT", "name STRING", "`value` DOUBLE", "dt STRING");
        }
        sEnv.executeSql(
                buildDdl(tableName, fields, Collections.emptyList(), partitionKeys, baseOptions()));
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(ROW_TRACKING_ENABLED.key(), "true");
        options.put(DATA_EVOLUTION_ENABLED.key(), "true");
        options.put("bucket", "-1");
        return options;
    }

    private void batchInsert(String tableName, String... records) throws Exception {
        bEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` VALUES %s", tableName, String.join(",", records)))
                .await();
    }

    private void upsert(String tableName, String upsertKeys, String... records) throws Exception {
        sEnv.executeSql(
                        String.format(
                                "INSERT INTO `%s` /*+ OPTIONS('data-evolution.upsert-keys'='%s') */ VALUES %s",
                                tableName, upsertKeys, String.join(",", records)))
                .await();
    }
}
