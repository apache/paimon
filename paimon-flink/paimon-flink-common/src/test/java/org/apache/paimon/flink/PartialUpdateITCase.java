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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobViewStruct;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.CommonTestUtils;

import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

        // filter
        assertThat(batchSql("SELECT * FROM T where b = 5 and c = '6'"))
                .containsExactlyInAnyOrder(Row.of(1, 2, 4, 5, "6"));
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

    protected List<List<Object>> rowsToList(List<Row> rows) {
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
    public void testMultiFieldsSequenceGroup() {
        sql(
                "CREATE TABLE SG ("
                        + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, g_3 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.g_1.sequence-group'='a,b', "
                        + "'fields.g_2,g_3.sequence-group'='c,d');");

        sql("INSERT INTO SG VALUES (1, 1, 1, 1, 1, 1, 1, 1)");

        // g_2, g_3 should not be updated
        sql("INSERT INTO SG VALUES (1, 2, 2, 2, 2, 2, 1, CAST(NULL AS INT))");

        // select *
        assertThat(sql("SELECT * FROM SG"))
                .containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 1, 1, 1, 1));

        // projection
        assertThat(sql("SELECT c, d FROM SG")).containsExactlyInAnyOrder(Row.of(1, 1));

        // g_1 should not be updated
        sql("INSERT INTO SG VALUES (1, 3, 3, 1, 3, 3, 3, 1)");

        assertThat(sql("SELECT * FROM SG"))
                .containsExactlyInAnyOrder(Row.of(1, 2, 2, 2, 3, 3, 3, 1));

        // d should be updated by null
        sql("INSERT INTO SG VALUES (1, 3, 3, 3, 2, 2, CAST(NULL AS INT), 1)");
        sql("INSERT INTO SG VALUES (1, 4, 4, 4, 2, 2, CAST(NULL AS INT), 1)");
        sql("INSERT INTO SG VALUES (1, 5, 5, 3, 5, CAST(NULL AS INT), 4, 1)");

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
                .hasRootCauseMessage("Field a1 can not be found in table schema");

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE SG ("
                                                + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, PRIMARY KEY (k) NOT ENFORCED)"
                                                + " WITH ("
                                                + "'merge-engine'='partial-update', "
                                                + "'fields.g_1.sequence-group'='a,b', "
                                                + "'fields.g_2.sequence-group'='a,d');"))
                .rootCause()
                .hasMessageContaining("Field a is defined repeatedly by multiple groups");

        Assertions.assertThatThrownBy(
                        () ->
                                sql(
                                        "CREATE TABLE SG ("
                                                + "k INT, a INT, b INT, g_1 INT, c INT, d INT, g_2 INT, g_3 INT, PRIMARY KEY (k) NOT ENFORCED)"
                                                + " WITH ("
                                                + "'merge-engine'='partial-update', "
                                                + "'fields.g_1.sequence-group'='a,b', "
                                                + "'fields.g_2,g_3.sequence-group'='a,d');"))
                .rootCause()
                .hasMessageContaining("Field a is defined repeatedly by multiple groups");
    }

    @Test
    public void testProjectPushDownWithLookupChangelogProducer() {
        sql(
                "CREATE TABLE IF NOT EXISTS T_P ("
                        + "j INT, k INT, a INT, b INT, c STRING, PRIMARY KEY (j,k) NOT ENFORCED)"
                        + " WITH ('merge-engine'='partial-update', 'changelog-producer' = 'lookup', "
                        + "'fields.a.sequence-group'='b,c');");
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
                        + "'local-merge-buffer-size'='5m'"
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
    public void testMultiFieldsSequencePartialUpdateWithAggregation() {
        sql(
                "CREATE TABLE AGG ("
                        + "k INT, a INT, b INT, g_1 INT, c VARCHAR, g_2 INT, g_3 INT, PRIMARY KEY (k) NOT ENFORCED)"
                        + " WITH ("
                        + "'merge-engine'='partial-update', "
                        + "'fields.a.aggregate-function'='sum', "
                        + "'fields.g_1,g_3.sequence-group'='a', "
                        + "'fields.g_2.sequence-group'='c');");
        // a in group g_1, g_3 with sum agg
        // b not in group
        // c in group g_2 without agg

        sql("INSERT INTO AGG VALUES (1, 1, 1, 1, '1', 1, 1)");

        // g_2 should not be updated
        sql("INSERT INTO AGG VALUES (1, 2, 2, 2, '2', CAST(NULL AS INT), 2)");

        // select *
        assertThat(sql("SELECT * FROM AGG"))
                .containsExactlyInAnyOrder(Row.of(1, 3, 2, 2, "1", 1, 2));

        // projection
        assertThat(sql("SELECT a, c FROM AGG")).containsExactlyInAnyOrder(Row.of(3, "1"));

        // g_1 should not be updated
        sql("INSERT INTO AGG VALUES (1, 3, 3, 2, '3', 3, 1)");

        assertThat(sql("SELECT * FROM AGG"))
                .containsExactlyInAnyOrder(Row.of(1, 6, 3, 2, "3", 3, 2));

        sql(
                "INSERT INTO AGG VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, CAST(NULL AS VARCHAR), 4, 2)");

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
            sql("ALTER TABLE ignore_delete SET ('local-merge-buffer-size' = '5m')");
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
    public void testRemoveRecordOnDeleteWithoutSequenceGroup() throws Exception {
        sql(
                "CREATE TABLE remove_record_on_delete (pk INT PRIMARY KEY NOT ENFORCED, a STRING, b STRING) WITH ("
                        + " 'merge-engine' = 'partial-update',"
                        + " 'partial-update.remove-record-on-delete' = 'true'"
                        + ")");

        sql("INSERT INTO remove_record_on_delete VALUES (1, CAST (NULL AS STRING), 'apple')");

        // delete record
        sql("DELETE FROM remove_record_on_delete WHERE pk = 1");

        // batch read
        assertThat(sql("SELECT * FROM remove_record_on_delete")).isEmpty();

        // insert records
        sql("INSERT INTO remove_record_on_delete VALUES (1, CAST (NULL AS STRING), 'apache')");
        sql("INSERT INTO remove_record_on_delete VALUES (1, 'A', CAST (NULL AS STRING))");

        // batch read
        assertThat(sql("SELECT * FROM remove_record_on_delete"))
                .containsExactlyInAnyOrder(Row.of(1, "A", "apache"));

        // delete record with changelog stream
        String id =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(Row.ofKind(RowKind.DELETE, 1, "A", null)));
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE delete_source1 (pk INT, a STRING, b STRING) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D,UA,UB')",
                        id));
        sEnv.executeSql("INSERT INTO remove_record_on_delete SELECT * FROM delete_source1").await();
        assertThat(sql("SELECT * FROM remove_record_on_delete")).isEmpty();
    }

    @Test
    public void testRemoveRecordOnDeleteWithSequenceGroup() throws Exception {
        sql(
                "CREATE TABLE remove_record_on_delete_sequence_group"
                        + " (pk INT PRIMARY KEY NOT ENFORCED, a STRING, seq_a INT, b STRING, seq_b INT) WITH ("
                        + " 'merge-engine' = 'partial-update',"
                        + " 'fields.seq_a.sequence-group' = 'a',"
                        + " 'fields.seq_b.sequence-group' = 'b',"
                        + " 'partial-update.remove-record-on-sequence-group' = 'seq_a'"
                        + ")");

        sql("INSERT INTO remove_record_on_delete_sequence_group VALUES (1, 'apple', 2, 'a', 1)");
        sql("INSERT INTO remove_record_on_delete_sequence_group VALUES (1, 'banana', 1, 'b', 2)");
        assertThat(sql("SELECT * FROM remove_record_on_delete_sequence_group"))
                .containsExactlyInAnyOrder(Row.of(1, "apple", 2, "b", 2));

        // delete with seq_b won't delete record but retract b
        String id =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(
                                Row.ofKind(RowKind.DELETE, 1, null, null, "b", 2)));
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE delete_source1 (pk INT, a STRING, seq_a INT, b STRING, seq_b INT) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D,UA,UB')",
                        id));
        sEnv.executeSql(
                        "INSERT INTO remove_record_on_delete_sequence_group SELECT * FROM delete_source1")
                .await();
        assertThat(sql("SELECT * FROM remove_record_on_delete_sequence_group"))
                .containsExactlyInAnyOrder(Row.of(1, "apple", 2, null, 2));

        // delete record with seq_a
        String id2 =
                TestValuesTableFactory.registerData(
                        Collections.singletonList(
                                Row.ofKind(RowKind.DELETE, 1, "apple", 2, null, null)));
        sEnv.executeSql(
                String.format(
                        "CREATE TEMPORARY TABLE delete_source2 (pk INT, a STRING, seq_a INT, b STRING, seq_b INT) "
                                + "WITH ('connector'='values', 'bounded'='true', 'data-id'='%s', "
                                + "'changelog-mode' = 'I,D,UA,UB')",
                        id2));
        sEnv.executeSql(
                        "INSERT INTO remove_record_on_delete_sequence_group SELECT * FROM delete_source2")
                .await();
        assertThat(sql("SELECT * FROM remove_record_on_delete_sequence_group")).isEmpty();

        // batch delete record
        sql(
                "INSERT INTO remove_record_on_delete_sequence_group VALUES (2, 'flink', 2, 'paimon', 1)");
        sql("DELETE FROM remove_record_on_delete_sequence_group WHERE pk = 2");
        assertThat(sql("SELECT * FROM remove_record_on_delete_sequence_group")).isEmpty();
    }

    @Test
    public void testRemoveRecordOnDeleteLookup() throws Exception {
        sql(
                "CREATE TABLE remove_record_on_delete (pk INT PRIMARY KEY NOT ENFORCED, a STRING, b STRING) WITH ("
                        + " 'merge-engine' = 'partial-update',"
                        + " 'partial-update.remove-record-on-delete' = 'true',"
                        + " 'changelog-producer' = 'lookup'"
                        + ")");

        sql("INSERT INTO remove_record_on_delete VALUES (1, CAST (NULL AS STRING), 'apple')");

        // delete record
        sql("DELETE FROM remove_record_on_delete WHERE pk = 1");

        // batch read
        assertThat(sql("SELECT * FROM remove_record_on_delete")).isEmpty();

        // insert records
        sql("INSERT INTO remove_record_on_delete VALUES (1, CAST (NULL AS STRING), 'apache')");
        sql("INSERT INTO remove_record_on_delete VALUES (1, 'A', CAST (NULL AS STRING))");

        // batch read
        assertThat(sql("SELECT * FROM remove_record_on_delete"))
                .containsExactlyInAnyOrder(Row.of(1, "A", "apache"));

        // streaming read results has -U
        BlockingIterator<Row, Row> iterator =
                streamSqlBlockIter(
                        "SELECT * FROM remove_record_on_delete /*+ OPTIONS('scan.timestamp-millis' = '0') */");
        assertThat(iterator.collect(5))
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, 1, null, "apple"),
                        Row.ofKind(RowKind.DELETE, 1, null, "apple"),
                        Row.ofKind(RowKind.INSERT, 1, null, "apache"),
                        Row.ofKind(RowKind.UPDATE_BEFORE, 1, null, "apache"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, "A", "apache"));
        iterator.close();
    }

    @Test
    public void testSequenceGroupWithDefaultAgg() {
        sql(
                "CREATE TABLE seq_default_agg ("
                        + " pk INT PRIMARY KEY NOT ENFORCED,"
                        + " seq INT,"
                        + " v INT) WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'fields.seq.sequence-group'='v',"
                        + " 'fields.default-aggregate-function'='sum'"
                        + ")");

        sql("INSERT INTO seq_default_agg VALUES (0, 1, 1)");
        sql("INSERT INTO seq_default_agg VALUES (0, 2, 2)");

        assertThat(sql("SELECT * FROM seq_default_agg")).containsExactly(Row.of(0, 2, 3));
    }

    @Test
    public void testBlobDescriptorPartialUpdate() throws Exception {
        byte[] first = "video-v1".getBytes();
        byte[] second = "video-v2".getBytes();
        String firstUri = writeExternalBlob("pu_blob_v1", first);
        String secondUri = writeExternalBlob("pu_blob_v2", second);

        sql(
                "CREATE TABLE pu_blob ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " name STRING,"
                        + " payload BYTES"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-descriptor-field'='payload',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");

        sql("INSERT INTO pu_blob VALUES (1, 'a', sys.path_to_descriptor('" + firstUri + "'))");
        sql("INSERT INTO pu_blob VALUES (1, 'b', CAST(NULL AS BYTES))");
        assertThat(sql("SELECT id, name, payload FROM pu_blob"))
                .containsExactly(Row.of(1, "b", first));

        sql(
                "INSERT INTO pu_blob VALUES (1, CAST(NULL AS STRING), sys.path_to_descriptor('"
                        + secondUri
                        + "'))");
        assertThat(sql("SELECT id, name, payload FROM pu_blob"))
                .containsExactly(Row.of(1, "b", second));

        assertThat((long) sql("SELECT COUNT(*) FROM `pu_blob$files`").get(0).getField(0))
                .isGreaterThan(1);
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.pu_blob', compact_strategy => 'full')");
        Snapshot snapshot = findLatestSnapshot("pu_blob");
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(sql("SELECT COUNT(*) FROM `pu_blob$files`")).containsExactly(Row.of(1L));
        assertThat(sql("SELECT id, name, payload FROM pu_blob"))
                .containsExactly(Row.of(1, "b", second));

        sql("ALTER TABLE pu_blob SET ('blob-as-descriptor'='true')");
        byte[] descriptorBytes = (byte[]) sql("SELECT payload FROM pu_blob").get(0).getField(0);
        BlobDescriptor descriptor = BlobDescriptor.deserialize(descriptorBytes);
        assertThat(descriptor.uri()).isEqualTo(secondUri);
        assertThat(descriptor.offset()).isEqualTo(0);
        // path_to_descriptor may encode whole-file length as -1.
        assertThat(descriptor.length()).isIn(-1L, (long) second.length);
    }

    @Test
    public void testBlobDescriptorPartialUpdateSequenceGroup() throws Exception {
        byte[] first = "seq-v1".getBytes();
        byte[] second = "seq-v2".getBytes();
        String firstUri = writeExternalBlob("pu_blob_seq_v1", first);
        String secondUri = writeExternalBlob("pu_blob_seq_v2", second);

        sql(
                "CREATE TABLE pu_blob_seq ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " name STRING,"
                        + " payload BYTES,"
                        + " ts INT"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-descriptor-field'='payload',"
                        + " 'fields.ts.sequence-group'='name,payload',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");

        sql(
                "INSERT INTO pu_blob_seq VALUES (1, 'a', sys.path_to_descriptor('"
                        + firstUri
                        + "'), 1)");
        // null sequence should not overwrite name/payload
        sql(
                "INSERT INTO pu_blob_seq VALUES (1, 'b', sys.path_to_descriptor('"
                        + secondUri
                        + "'), CAST(NULL AS INT))");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_blob_seq"))
                .containsExactly(Row.of(1, "a", first, 1));

        sql(
                "INSERT INTO pu_blob_seq VALUES (1, 'c', sys.path_to_descriptor('"
                        + secondUri
                        + "'), 2)");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_blob_seq"))
                .containsExactly(Row.of(1, "c", second, 2));

        // equal sequence overwrites the entire group, including with null
        sql("INSERT INTO pu_blob_seq VALUES " + "(1, 'd', CAST(NULL AS BYTES), 2)");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_blob_seq"))
                .containsExactly(Row.of(1, "d", null, 2));

        // older sequence must not overwrite
        sql(
                "INSERT INTO pu_blob_seq VALUES (1, 'e', sys.path_to_descriptor('"
                        + secondUri
                        + "'), 1)");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_blob_seq"))
                .containsExactly(Row.of(1, "d", null, 2));

        assertThat((long) sql("SELECT COUNT(*) FROM `pu_blob_seq$files`").get(0).getField(0))
                .isGreaterThan(1);
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.pu_blob_seq', compact_strategy => 'full')");
        Snapshot snapshot = findLatestSnapshot("pu_blob_seq");
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(sql("SELECT COUNT(*) FROM `pu_blob_seq$files`")).containsExactly(Row.of(1L));
        assertThat(sql("SELECT id, name, payload, ts FROM pu_blob_seq"))
                .containsExactly(Row.of(1, "d", null, 2));

        sql("ALTER TABLE pu_blob_seq SET ('blob-as-descriptor'='true')");
        assertThat(sql("SELECT payload FROM pu_blob_seq")).containsExactly(Row.of((Object) null));
    }

    @Test
    public void testManagedBlobPartialUpdate() throws Exception {
        byte[] first = "blob-v1".getBytes();
        byte[] second = "blob-v2".getBytes();

        sql(
                "CREATE TABLE pu_managed_blob ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " name STRING,"
                        + " payload BYTES"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-field'='payload',"
                        + " 'changelog-producer'='none',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");

        sql("INSERT INTO pu_managed_blob VALUES (1, 'a', " + toHexLiteral(first) + ")");
        sql("INSERT INTO pu_managed_blob VALUES (1, 'b', CAST(NULL AS BYTES))");
        assertThat(sql("SELECT id, name, payload FROM pu_managed_blob"))
                .containsExactly(Row.of(1, "b", first));

        sql(
                "INSERT INTO pu_managed_blob VALUES (1, CAST(NULL AS STRING), "
                        + toHexLiteral(second)
                        + ")");
        assertThat(sql("SELECT id, name, payload FROM pu_managed_blob"))
                .containsExactly(Row.of(1, "b", second));

        assertThat((long) sql("SELECT COUNT(*) FROM `pu_managed_blob$files`").get(0).getField(0))
                .isGreaterThan(1);
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.pu_managed_blob', compact_strategy => 'full')");
        Snapshot snapshot = findLatestSnapshot("pu_managed_blob");
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(sql("SELECT COUNT(*) FROM `pu_managed_blob$files`")).containsExactly(Row.of(1L));
        assertThat(sql("SELECT id, name, payload FROM pu_managed_blob"))
                .containsExactly(Row.of(1, "b", second));
    }

    @Test
    public void testManagedBlobPartialUpdateSequenceGroup() throws Exception {
        byte[] first = "blob-seq-v1".getBytes();
        byte[] second = "blob-seq-v2".getBytes();
        sql(
                "CREATE TABLE pu_managed_blob_seq ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " name STRING,"
                        + " payload BYTES,"
                        + " ts INT"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-field'='payload',"
                        + " 'changelog-producer'='none',"
                        + " 'fields.ts.sequence-group'='name,payload',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");

        sql("INSERT INTO pu_managed_blob_seq VALUES (1, 'first', " + toHexLiteral(first) + ", 2)");
        sql("INSERT INTO pu_managed_blob_seq VALUES (1, 'older', " + toHexLiteral(second) + ", 1)");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_managed_blob_seq"))
                .containsExactly(Row.of(1, "first", first, 2));

        sql("INSERT INTO pu_managed_blob_seq VALUES (1, 'cleared', CAST(NULL AS BYTES), 3)");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_managed_blob_seq"))
                .containsExactly(Row.of(1, "cleared", null, 3));

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.compact("
                        + "`table` => 'default.pu_managed_blob_seq', "
                        + "compact_strategy => 'full')");
        assertThat(sql("SELECT id, name, payload, ts FROM pu_managed_blob_seq"))
                .containsExactly(Row.of(1, "cleared", null, 3));
    }

    @Test
    public void testBlobViewPartialUpdate() throws Exception {
        createBlobViewPartialUpdateTables();

        assertThat(sql("SELECT id, label, image_ref FROM pu_blob_view ORDER BY id"))
                .containsExactly(Row.of(1, "row1", new byte[] {72, 101, 108, 108, 111}));

        sql("INSERT INTO pu_blob_view VALUES (1, 'updated', CAST(NULL AS BYTES))");
        assertThat(sql("SELECT id, label, image_ref FROM pu_blob_view"))
                .containsExactly(Row.of(1, "updated", new byte[] {72, 101, 108, 108, 111}));

        assertThat((long) sql("SELECT COUNT(*) FROM `pu_blob_view$files`").get(0).getField(0))
                .isGreaterThan(1);
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("CALL sys.compact(`table` => 'default.pu_blob_view', compact_strategy => 'full')");
        Snapshot snapshot = findLatestSnapshot("pu_blob_view");
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(sql("SELECT COUNT(*) FROM `pu_blob_view$files`")).containsExactly(Row.of(1L));
        assertThat(sql("SELECT id, label, image_ref FROM pu_blob_view"))
                .containsExactly(Row.of(1, "updated", new byte[] {72, 101, 108, 108, 111}));
    }

    @Test
    public void testBlobViewPartialUpdateForwardReference() throws Exception {
        String fullTableName = createBlobViewPartialUpdateTables();
        sql("INSERT INTO pu_blob_view VALUES (1, 'updated', CAST(NULL AS BYTES))");
        sql(
                "CREATE TABLE pu_blob_view_forward ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " label STRING,"
                        + " image_ref BYTES"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-view-field'='image_ref'"
                        + ")");
        sql(
                "INSERT INTO pu_blob_view_forward"
                        + " SELECT id, label, image_ref"
                        + " FROM pu_blob_view"
                        + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */");

        assertThat(sql("SELECT id, label, image_ref FROM pu_blob_view_forward"))
                .containsExactly(Row.of(1, "updated", new byte[] {72, 101, 108, 108, 111}));

        byte[] originalReference =
                (byte[])
                        sql("SELECT image_ref FROM pu_blob_view"
                                        + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */")
                                .get(0)
                                .getField(0);
        byte[] forwardedReference =
                (byte[])
                        sql("SELECT image_ref FROM pu_blob_view_forward"
                                        + " /*+ OPTIONS('blob-view.resolve.enabled'='false') */")
                                .get(0)
                                .getField(0);
        assertThat(forwardedReference).isEqualTo(originalReference);
        assertThat(BlobViewStruct.deserialize(forwardedReference).identifier().getFullName())
                .isEqualTo(fullTableName);
    }

    @Test
    public void testBlobViewPartialUpdateSequenceGroup() throws Exception {
        String fullTableName = createBlobViewUpstream();
        sql(
                "CREATE TABLE pu_blob_view_seq ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " label STRING,"
                        + " image_ref BYTES,"
                        + " ts INT"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-view-field'='image_ref',"
                        + " 'fields.ts.sequence-group'='label,image_ref',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");
        sql(
                String.format(
                        "INSERT INTO pu_blob_view_seq"
                                + " SELECT id, name, sys.blob_view('%s', 'picture', _ROW_ID), 2"
                                + " FROM `pu_upstream_blob$row_tracking`",
                        fullTableName));

        sql("INSERT INTO pu_blob_view_seq VALUES (1, 'older', CAST(NULL AS BYTES), 1)");
        assertThat(sql("SELECT id, label, image_ref, ts FROM pu_blob_view_seq"))
                .containsExactly(Row.of(1, "row1", new byte[] {72, 101, 108, 108, 111}, 2));

        sql("INSERT INTO pu_blob_view_seq VALUES (1, 'cleared', CAST(NULL AS BYTES), 3)");
        assertThat(sql("SELECT id, label, image_ref, ts FROM pu_blob_view_seq"))
                .containsExactly(Row.of(1, "cleared", null, 3));

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql(
                "CALL sys.compact("
                        + "`table` => 'default.pu_blob_view_seq', "
                        + "compact_strategy => 'full')");
        assertThat(sql("SELECT id, label, image_ref, ts FROM pu_blob_view_seq"))
                .containsExactly(Row.of(1, "cleared", null, 3));
    }

    private String createBlobViewPartialUpdateTables() {
        String fullTableName = createBlobViewUpstream();
        sql(
                "CREATE TABLE pu_blob_view ("
                        + " id INT PRIMARY KEY NOT ENFORCED,"
                        + " label STRING,"
                        + " image_ref BYTES"
                        + ") WITH ("
                        + " 'merge-engine'='partial-update',"
                        + " 'blob-view-field'='image_ref',"
                        + " 'bucket'='1',"
                        + " 'write-only'='true',"
                        + " 'num-sorted-run.compaction-trigger'='100'"
                        + ")");
        sql(
                String.format(
                        "INSERT INTO pu_blob_view"
                                + " SELECT id, name, sys.blob_view('%s', 'picture', _ROW_ID)"
                                + " FROM `pu_upstream_blob$row_tracking`",
                        fullTableName));
        return fullTableName;
    }

    private String createBlobViewUpstream() {
        sql(
                "CREATE TABLE pu_upstream_blob ("
                        + " id INT, name STRING, picture BYTES"
                        + ") WITH ("
                        + " 'row-tracking.enabled'='true',"
                        + " 'data-evolution.enabled'='true',"
                        + " 'blob-field'='picture'"
                        + ")");
        sql("INSERT INTO pu_upstream_blob VALUES (1, 'row1', X'48656C6C6F')");

        String fullTableName = tEnv.getCurrentDatabase() + ".pu_upstream_blob";
        return fullTableName;
    }

    private static String toHexLiteral(byte[] data) {
        StringBuilder builder = new StringBuilder("X'");
        for (byte value : data) {
            builder.append(String.format("%02X", value));
        }
        builder.append("'");
        return builder.toString();
    }

    private String writeExternalBlob(String name, byte[] data) throws Exception {
        FileIO fileIO = new LocalFileIO();
        String uri = "file://" + path + "/" + name;
        try (OutputStream outputStream =
                fileIO.newOutputStream(new org.apache.paimon.fs.Path(uri), true)) {
            outputStream.write(data);
        }
        return uri;
    }
}
