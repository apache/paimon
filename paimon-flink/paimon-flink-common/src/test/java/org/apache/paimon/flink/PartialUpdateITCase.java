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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.CommonTestUtils;

import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** ITCase for partial update. */
public class PartialUpdateITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "j INT, k INT, a INT, b INT, c STRING, PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update');",
                "CREATE TABLE IF NOT EXISTS dwd_orders ("
                        + "OrderID INT, OrderNumber INT, PersonID INT, LastName STRING, FirstName STRING, Age INT, PRIMARY KEY (OrderID) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update', 'ignore-delete'='true');",
                "CREATE TABLE IF NOT EXISTS ods_orders (OrderID INT, OrderNumber INT, PersonID INT, PRIMARY KEY (OrderID) NOT ENFORCED) WITH ('changelog-producer'='input', 'continuous.discovery-interval'='1s');",
                "CREATE TABLE IF NOT EXISTS dim_persons (PersonID INT, LastName STRING, FirstName STRING, Age INT, PRIMARY KEY (PersonID) NOT ENFORCED) WITH ('changelog-producer'='input', 'continuous.discovery-interval'='1s');");
    }

    @Test
    public void testMergeInMemory() {
        batchSql(
                "INSERT INTO T VALUES "
                        + "(1, 2, 3, CAST(NULL AS INT), '5'), "
                        + "(1, 2, CAST(NULL AS INT), 6, CAST(NULL AS STRING))");
        List<Row> result = batchSql("SELECT * FROM T");
        assertThat(result).containsExactlyInAnyOrder(Row.of(1, 2, 3, 6, "5"));
    }

    @Test
    public void testMergeRead() {
        batchSql("INSERT INTO T VALUES (1, 2, 3, CAST(NULL AS INT), CAST(NULL AS STRING))");
        batchSql("INSERT INTO T VALUES (1, 2, 4, 5, CAST(NULL AS STRING))");
        batchSql("INSERT INTO T VALUES (1, 2, 4, CAST(NULL AS INT), '6')");

        assertThat(batchSql("SELECT * FROM T")).containsExactlyInAnyOrder(Row.of(1, 2, 4, 5, "6"));

        // projection
        assertThat(batchSql("SELECT a FROM T")).containsExactlyInAnyOrder(Row.of(4));
    }

    @Test
    public void testMergeCompaction() {
        // Wait compaction
        batchSql("ALTER TABLE T SET ('commit.force-compact'='true')");

        // key 1 2
        batchSql("INSERT INTO T VALUES (1, 2, 3, CAST(NULL AS INT), CAST(NULL AS STRING))");
        batchSql("INSERT INTO T VALUES (1, 2, 4, 5, CAST(NULL AS STRING))");
        batchSql("INSERT INTO T VALUES (1, 2, 4, CAST(NULL AS INT), '6')");

        // key 1 3
        batchSql("INSERT INTO T VALUES (1, 3, CAST(NULL AS INT), 1, '1')");
        batchSql("INSERT INTO T VALUES (1, 3, 2, 3, CAST(NULL AS STRING))");
        batchSql("INSERT INTO T VALUES (1, 3, CAST(NULL AS INT), 4, CAST(NULL AS STRING))");

        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 2, 4, 5, "6"), Row.of(1, 3, 2, 4, "1"));
    }

    @Test
    public void testForeignKeyJoin() throws Exception {
        sEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                        ExecutionConfigOptions.UpsertMaterialize.NONE);
        CloseableIterator<Row> iter =
                streamSqlIter(
                        "INSERT INTO dwd_orders "
                                + "SELECT OrderID, OrderNumber, PersonID, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT) FROM ods_orders "
                                + "UNION ALL "
                                + "SELECT OrderID, CAST(NULL AS INT), dim_persons.PersonID, LastName, FirstName, Age FROM dim_persons JOIN ods_orders ON dim_persons.PersonID = ods_orders.PersonID;");

        batchSql("INSERT INTO ods_orders VALUES (1, 2, 3)");
        batchSql("INSERT INTO dim_persons VALUES (3, 'snow', 'jon', 23)");
        CommonTestUtils.waitUtil(
                () ->
                        rowsToList(batchSql("SELECT * FROM dwd_orders"))
                                .contains(Arrays.asList(1, 2, 3, "snow", "jon", 23)),
                Duration.ofSeconds(5),
                Duration.ofMillis(200));

        batchSql("INSERT INTO ods_orders VALUES (1, 4, 3)");
        batchSql("INSERT INTO dim_persons VALUES (3, 'snow', 'targaryen', 23)");
        CommonTestUtils.waitUtil(
                () ->
                        rowsToList(batchSql("SELECT * FROM dwd_orders"))
                                .contains(Arrays.asList(1, 4, 3, "snow", "targaryen", 23)),
                Duration.ofSeconds(5),
                Duration.ofMillis(200));

        iter.close();
    }

    private List<List<Object>> rowsToList(List<Row> rows) {
        return rows.stream().map(this::toList).collect(Collectors.toList());
    }

    private List<Object> toList(Row row) {
        assertThat(row.getKind()).isIn(RowKind.INSERT, RowKind.UPDATE_AFTER);
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < row.getArity(); i++) {
            result.add(row.getField(i));
        }
        return result;
    }

    @Test
    public void testStreamingRead() {
        assertThatThrownBy(
                () -> sEnv.from("T").execute().print(),
                "Partial update continuous reading is not supported");
    }

    @Test
    public void testStreamingReadChangelogInput() throws TimeoutException {
        sql(
                "CREATE TABLE INPUT_T ("
                        + "a INT, b INT, c INT, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update', 'changelog-producer'='input');");
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(streamSqlIter("SELECT * FROM INPUT_T"));
        sql("INSERT INTO INPUT_T VALUES (1, CAST(NULL AS INT), 1)");
        assertThat(iterator.collect(1)).containsExactlyInAnyOrder(Row.of(1, null, 1));
        sql("INSERT INTO INPUT_T VALUES (1, 1, CAST(NULL AS INT)), (2, 2, 2)");
        assertThat(iterator.collect(2))
                .containsExactlyInAnyOrder(Row.of(1, 1, null), Row.of(2, 2, 2));
    }

    @Test
    public void testSequenceGroup() {
        sql(
                "CREATE TABLE SG ("
                        + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.g_1.sequence-group'='a,b', "
                        + "'fields.g_2.sequence-group'='c,d');");

        sql("INSERT INTO SG VALUES (1, 1, 1, 1, 1, 1, 1)");

        // g_2 should not be updated
        sql("INSERT INTO SG VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT))");

        // select *
        assertThat(sql("SELECT * FROM SG")).containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 1, 1, 1));

        // projection
        assertThat(sql("SELECT c, d FROM SG")).containsExactlyInAnyOrder(Row.of(1, 1));

        // g_1 should not be updated
        sql("INSERT INTO SG VALUES (1, 3, 3, 1, 3, 3, 3)");

        assertThat(sql("SELECT * FROM SG")).containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 3, 3, 3));

        // d should be updated by null
        sql("INSERT INTO SG VALUES (1, 3, 3, 3, 2, 2, CAST(NULL AS INT))");
        sql("INSERT INTO SG VALUES (1, 4, 4, 4, 2, 2, CAST(NULL AS INT))");
        sql("INSERT INTO SG VALUES (1, 5, 5, 3, 5, CAST(NULL AS INT), 4)");

        assertThat(sql("SELECT a, b FROM SG")).containsExactlyInAnyOrder(Row.of(4, 4));
        assertThat(sql("SELECT c, d FROM SG")).containsExactlyInAnyOrder(Row.of(5, null));
    }

    @Test
    public void testSequenceGroupWithDefaultAggFunc() {
        sql(
                "CREATE TABLE SG ("
                        + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.g_1.sequence-group'='a,b', "
                        + "'fields.g_2.sequence-group'='c,d', "
                        + "'fields.default-aggregate-function'='last_non_null_value');");

        sql("INSERT INTO SG VALUES (1, 1, 1, 1, 1, 1, 1)");

        // g_2 should not be updated
        sql("INSERT INTO SG VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT))");

        // select *
        assertThat(sql("SELECT * FROM SG")).containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 1, 1, 1));

        // projection
        assertThat(sql("SELECT c, d FROM SG")).containsExactlyInAnyOrder(Row.of(1, 1));

        // g_1 should not be updated
        sql("INSERT INTO SG VALUES (1, 3, 3, 1, 3, 3, 3)");

        assertThat(sql("SELECT * FROM SG")).containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 3, 3, 3));

        // d should not be updated by null
        sql("INSERT INTO SG VALUES (1, 3, 3, 3, 2, 2, CAST(NULL AS INT))");
        sql("INSERT INTO SG VALUES (1, 4, 4, 4, 2, 2, CAST(NULL AS INT))");
        sql("INSERT INTO SG VALUES (1, 5, 5, 3, 5, CAST(NULL AS INT), 4)");

        assertThat(sql("SELECT a, b FROM SG")).containsExactlyInAnyOrder(Row.of(4, 4));
        assertThat(sql("SELECT c, d FROM SG")).containsExactlyInAnyOrder(Row.of(5, 3));
    }

    @Test
    public void testInvalidSequenceGroup() {
        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE SG ("
                                                + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                                                + " WITH ("
                                                + "'merge-engine'='partial-update', "
                                                + "'fields.g_0.sequence-group'='a,b', "
                                                + "'fields.g_2.sequence-group'='c,d');"))
                .hasRootCauseMessage("Field g_0 can not be found in table schema.");

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE SG ("
                                                + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                                                + " WITH ("
                                                + "'merge-engine'='partial-update', "
                                                + "'fields.g_1.sequence-group'='a1,b', "
                                                + "'fields.g_2.sequence-group'='c,d');"))
                .hasRootCauseMessage("Field a1 can not be found in table schema.");

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE SG ("
                                                + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                                                + " WITH ("
                                                + "'merge-engine'='partial-update', "
                                                + "'fields.g_1.sequence-group'='a,b', "
                                                + "'fields.g_2.sequence-group'='a,d');"))
                .hasRootCauseMessage(
                        "Field a is defined repeatedly by multiple groups: [g_1, g_2].");
    }

    @Test
    public void testProjectPushDownWithLookupChangelogProducer() {
        sql(
                "CREATE TABLE IF NOT EXISTS T_P ("
                        + "j INT, k INT, a INT, b INT, c STRING, PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update', 'changelog-producer' = 'lookup', "
                        + "'fields.a.sequence-group'='j', 'fields.b.sequence-group'='c');");
        batchSql("INSERT INTO T_P VALUES (1, 1, 1, 1, '1')");
        assertThat(sql("SELECT k, c FROM T_P")).containsExactlyInAnyOrder(Row.of(1, "1"));
    }

    @Test
    public void testLocalMerge() {
        sql(
                "CREATE TABLE T1 ("
                        + "k INT,"
                        + "v INT,"
                        + "d INT,"
                        + "PRIMARY KEY (k, d) NOT ENFORCED) PARTITIONED BY (d) "
                        + " WITH ('merge-engine'='partial-update', "
                        + "'local-merge-buffer-size'='1m'"
                        + ");");

        sql("INSERT INTO T1 VALUES (1, CAST(NULL AS INT), 1), (2, 1, 1), (1, 2, 1)");
        assertThat(batchSql("SELECT * FROM T1"))
                .containsExactlyInAnyOrder(Row.of(1, 2, 1), Row.of(2, 1, 1));
    }

    @Test
    public void testPartialUpdateWithAggregation() {
        sql(
                "CREATE TABLE AGG ("
                        + "k INT, a INT, b INT, g_1 INT, c VARCHAR, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.a.aggregate-function'='sum', "
                        + "'fields.g_1.sequence-group'='a', "
                        + "'fields.g_2.sequence-group'='c');");
        // a in group g_1 with sum agg
        // b not in group
        // c in group g_2 without agg

        sql("INSERT INTO AGG VALUES (1, 1, 1, 1, '1', 1)");

        // g_2 should not be updated
        sql("INSERT INTO AGG VALUES (1, 2, 2, 2, '2', CAST(NULL AS INT))");

        // select *
        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 3, 2, 2, "1", 1));

        // projection
        assertThat(sql("SELECT a, c FROM AGG")).containsExactlyInAnyOrder(Row.of(3, "1"));

        // g_1 should not be updated
        sql("INSERT INTO AGG VALUES (1, 3, 3, 1, '3', 3)");

        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 6, 3, 2, "3", 3));

        sql(
                "INSERT INTO AGG VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, CAST(NULL AS VARCHAR), 4)");

        // a keep the last accumulator
        // b is not updated to null
        // c updated to null
        assertThat(sql("SELECT a, b, c FROM AGG")).containsExactlyInAnyOrder(Row.of(6, 3, null));
    }

    @Test
    public void testPartialUpdateWithDefaultAndFieldAggregation() {
        sql(
                "CREATE TABLE AGG ("
                        + "k INT, a INT, b INT, g_1 INT, c VARCHAR, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.a.aggregate-function'='sum', "
                        + "'fields.g_1.sequence-group'='a', "
                        + "'fields.g_2.sequence-group'='c', "
                        + "'fields.default-aggregate-function'='last_non_null_value');");
        // a in group g_1 with sum agg
        // b not in group
        // c in group g_2 without agg

        sql("INSERT INTO AGG VALUES (1, 1, 1, 1, '1', 1)");

        // g_2 should not be updated
        sql("INSERT INTO AGG VALUES (1, 2, 2, 2, '2', CAST(NULL AS INT))");

        // select *
        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 3, 2, 2, "1", 1));

        // projection
        assertThat(sql("SELECT a, c FROM AGG")).containsExactlyInAnyOrder(Row.of(3, "1"));

        // g_1 should not be updated
        sql("INSERT INTO AGG VALUES (1, 3, 3, 1, '3', 3)");

        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 6, 3, 2, "3", 3));

        sql(
                "INSERT INTO AGG VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, CAST(NULL AS VARCHAR), 4)");

        // a keep the last accumulator
        // b is not updated to null
        // c is updated to "3" for default agg func last_non_null_value
        assertThat(sql("SELECT a, b, c FROM AGG")).containsExactlyInAnyOrder(Row.of(6, 3, "3"));
    }

    @Test
    public void testFirstValuePartialUpdate() {
        sql(
                "CREATE TABLE AGG ("
                        + "k INT, a INT, g_1 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.g_1.sequence-group'='a', "
                        + "'fields.a.aggregate-function'='first_value');");

        sql("INSERT INTO AGG VALUES (1, 1, 1), (1, 2, 2)");

        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 1, 2));

        // old sequence
        sql("INSERT INTO AGG VALUES (1, 0, 0)");

        assertThat(sql("SELECT * FROM AGG")).containsExactlyInAnyOrder(Row.of(1, 0, 2));
    }

    @Test
    public void testNoSinkMaterializer() {
        sEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE,
                        ExecutionConfigOptions.UpsertMaterialize.FORCE);
        sEnv.getConfig().set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        String sql =
                "INSERT INTO dwd_orders "
                        + "SELECT OrderID, OrderNumber, PersonID, CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT) FROM ods_orders "
                        + "UNION ALL "
                        + "SELECT OrderID, CAST(NULL AS INT), dim_persons.PersonID, LastName, FirstName, Age FROM dim_persons JOIN ods_orders ON dim_persons.PersonID = ods_orders.PersonID;";
        try {
            sEnv.executeSql(sql).await();
            fail("Expecting exception");
        } catch (Exception e) {
            assertThat(e)
                    .hasRootCauseMessage(
                            "Sink materializer must not be used with Paimon sink. "
                                    + "Please set 'table.exec.sink.upsert-materialize' to 'NONE' in Flink's config.");
        }
    }

    @Test
    public void testPartialUpdateProjectionPushDownWithDeleteMessage() throws Exception {
        List<Row> input = Arrays.asList(Row.ofKind(RowKind.INSERT, 1, 1, 1));

        String id = TestValuesTableFactory.registerData(input);
        // create temp table in stream table env
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE source (k INT, a INT, g_1 INT, PRIMARY KEY (k) NOT ENFORCED) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D,UA,UB')",
                        id));

        sql(
                "CREATE TABLE TEST ("
                        + "k INT, a INT, b INT, g_1 INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.g_1.sequence-group'='a', "
                        + "'fields.g_2.sequence-group'='b');");

        CloseableIterator<Row> insert1 =
                streamSqlIter(
                        "INSERT INTO TEST SELECT k, a, CAST(NULL AS INT) AS b, g_1,"
                                + " CAST(NULL AS INT) as g_2 FROM source");

        sqlAssertWithRetry(
                "SELECT * FROM TEST",
                list -> list.containsExactlyInAnyOrder(Row.of(1, 1, null, 1, null)));

        // insert the delete message
        input = Arrays.asList(Row.ofKind(RowKind.DELETE, 1, 1, 2));

        id = TestValuesTableFactory.registerData(input);

        // create temp table in stream table env
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE source2 (k INT, a INT, g_1 INT) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')",
                        id));

        CloseableIterator<Row> insert2 =
                streamSqlIter(
                        "INSERT INTO TEST SELECT k, a, CAST(NULL AS INT) AS b, g_1,"
                                + " CAST(NULL AS INT) as g_2 FROM source2");

        sqlAssertWithRetry(
                "SELECT * FROM TEST",
                list -> list.containsExactlyInAnyOrder(Row.of(1, null, null, 2, null)));

        assertThat(sql("SELECT COUNT(*) FROM TEST")).containsExactlyInAnyOrder(Row.of(1L));
        insert1.close();
        insert2.close();
    }

    @ParameterizedTest(name = "localMergeEnabled = {0}")
    @ValueSource(booleans = {true, false})
    public void testIgnoreDelete(boolean localMerge) throws Exception {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, a STRING, b STRING) WITH ("
                        + " 'merge-engine' = 'partial-update',"
                        + " 'ignore-delete' = 'true',"
                        + " 'changelog-producer' = 'lookup'"
                        + ")");
        if (localMerge) {
            sql("ALTER TABLE ignore_delete SET ('local-merge-buffer-size' = '256 kb')");
        }

        sql("INSERT INTO ignore_delete VALUES (1, CAST (NULL AS STRING), 'apple')");

        String id =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(Row.ofKind(RowKind.DELETE, 1, null, "apple")));
        streamSqlIter(
                        "CREATE TEMPORARY TABLE input (pk INT PRIMARY KEY NOT ENFORCED, a STRING, b STRING) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D')",
                        id)
                .close();
        sEnv.executeSql("INSERT INTO ignore_delete SELECT * FROM input").await();

        sql("INSERT INTO ignore_delete VALUES (1, 'A', CAST (NULL AS STRING))");

        // batch read
        assertThat(sql("SELECT * FROM ignore_delete"))
                .containsExactlyInAnyOrder(Row.of(1, "A", "apple"));

        // streaming read results has -U
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter(
                        "SELECT * FROM ignore_delete /*+ OPTIONS('scan.timestamp-millis' = '0') */");
        assertThat(iterator.collect(3))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1, null, "apple"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, null, "apple"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, "A", "apple"));
        iterator.close();
    }

    @Test
    public void testIgnoreDeleteCompatible() throws Exception {
        sql(
                "CREATE TABLE ignore_delete (pk INT PRIMARY KEY NOT ENFORCED, a STRING, b STRING) WITH ("
                        + " 'merge-engine' = 'deduplicate',"
                        + " 'write-only' = 'true')");
        sql("INSERT INTO ignore_delete VALUES (1, CAST (NULL AS STRING), 'apple')");
        // write delete records
        sql("DELETE FROM ignore_delete WHERE pk = 1");
        sql("INSERT INTO ignore_delete VALUES (1, 'A', CAST (NULL AS STRING))");
        assertThat(sql("SELECT * FROM ignore_delete"))
                .containsExactlyInAnyOrder(Row.of(1, "A", null));

        // force altering merge engine and read
        Map<String, String> newOptions = new HashMap<>();
        newOptions.put(
                CoreOptions.MERGE_ENGINE.key(), CoreOptions.MergeEngine.PARTIAL_UPDATE.toString());
        newOptions.put(CoreOptions.IGNORE_DELETE.key(), "true");
        SchemaUtils.forceCommit(
                new SchemaManager(LocalFileIO.create(), new Path(path, "default.db/ignore_delete")),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "pk", DataTypes.INT().notNull()),
                                new DataField(1, "a", DataTypes.STRING()),
                                new DataField(2, "b", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.singletonList("pk"),
                        newOptions,
                        null));
        assertThat(sql("SELECT * FROM ignore_delete"))
                .containsExactlyInAnyOrder(Row.of(1, "A", "apple"));
    }
}
