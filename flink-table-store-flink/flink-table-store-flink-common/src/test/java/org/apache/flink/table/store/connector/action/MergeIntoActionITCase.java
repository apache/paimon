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

package org.apache.flink.table.store.connector.action;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.utils.BlockingIterator;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.flink.table.store.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.createTable;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.init;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.flink.table.store.connector.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** IT cases for {@link MergeIntoAction}. */
public class MergeIntoActionITCase extends ActionITCaseBase {

    private static final List<Row> initialRecords =
            Arrays.asList(
                    changelogRow("+I", 1, "v_1", "creation", "02-27"),
                    changelogRow("+I", 2, "v_2", "creation", "02-27"),
                    changelogRow("+I", 3, "v_3", "creation", "02-27"),
                    changelogRow("+I", 4, "v_4", "creation", "02-27"),
                    changelogRow("+I", 5, "v_5", "creation", "02-28"),
                    changelogRow("+I", 6, "v_6", "creation", "02-28"),
                    changelogRow("+I", 7, "v_7", "creation", "02-28"),
                    changelogRow("+I", 8, "v_8", "creation", "02-28"),
                    changelogRow("+I", 9, "v_9", "creation", "02-28"),
                    changelogRow("+I", 10, "v_10", "creation", "02-28"));

    @BeforeEach
    public void setUp() throws Exception {
        init(warehouse);

        // prepare table S
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("k INT", "v STRING", "dt STRING"),
                        Arrays.asList("k", "dt"),
                        Collections.singletonList("dt"),
                        new HashMap<>()));

        insertInto(
                "S",
                "(1, 'v_1', '02-27')",
                "(4, CAST (NULL AS STRING), '02-27')",
                "(7, 'Seven', '02-28')",
                "(8, CAST (NULL AS STRING), '02-28')",
                "(8, 'v_8', '02-29')",
                "(11, 'v_11', '02-29')",
                "(12, 'v_12', '02-29')");
    }

    @ParameterizedTest(name = "changelog-producer = {0}")
    @MethodSource("producerTestData")
    public void testVariousChangelogProducer(
            CoreOptions.ChangelogProducer producer, List<Row> expected) throws Exception {
        // prepare table T
        prepareTable(producer);

        // similar to:
        // MERGE INTO T
        // USING S
        // ON T.k = S.k AND T.dt = S.dt
        // WHEN MATCHED AND (T.v <> S.v AND S.v IS NOT NULL) THEN UPDATE
        //   SET v = S.v, last_action = 'matched_upsert'
        // WHEN MATCHED AND S.v IS NULL THEN DELETE
        // WHEN NOT MATCHED THEN INSERT VALUES (S.k, S.v, 'insert', S.dt)
        // WHEN NOT MATCHED BY SOURCE AND (dt < '02-28') THEN UPDATE
        //   SET v = v || '_nmu', last_action = 'not_matched_upsert'
        // WHEN NOT MATCHED BY SOURCE AND (dt >= '02-28') THEN DELETE
        MergeIntoAction action = new MergeIntoAction(warehouse, database, "T");
        action.withSourceTable("S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withMatchedUpsert(
                        "T.v <> S.v AND S.v IS NOT NULL", "v = S.v, last_action = 'matched_upsert'")
                .withMatchedDelete("S.v IS NULL")
                .withNotMatchedInsert(null, "S.k, S.v, 'insert', S.dt")
                .withNotMatchedBySourceUpsert(
                        "dt < '02-28'", "v = v || '_nmu', last_action = 'not_matched_upsert'")
                .withNotMatchedBySourceDelete("dt >= '02-28'");

        validateActionRunResult(
                action,
                expected,
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                        changelogRow("+I", 8, "v_8", "insert", "02-29"),
                        changelogRow("+I", 11, "v_11", "insert", "02-29"),
                        changelogRow("+I", 12, "v_12", "insert", "02-29")));
    }

    @Test
    public void testUsingSource() throws Exception {
        // prepare table T
        prepareTable(CoreOptions.ChangelogProducer.NONE);

        // similar to:
        // MERGE INTO T
        // USING (SELECT * FROM S WHERE k < 12) AS SS
        // ON T.k = SS.k AND T.dt = SS.dt
        // WHEN MATCHED AND (T.v <> SS.v AND SS.v IS NOT NULL) THEN UPDATE
        //   SET v = SS.v, last_action = 'matched_upsert'
        // WHEN MATCHED AND SS.v IS NULL THEN DELETE
        // WHEN NOT MATCHED THEN INSERT VALUES (SS.k, SS.v, 'insert', SS.dt)
        // WHEN NOT MATCHED BY SOURCE AND (dt < '02-28') THEN UPDATE
        //   SET v = v || '_nmu', last_action = 'not_matched_upsert'
        // WHEN NOT MATCHED BY SOURCE AND (dt >= '02-28') THEN DELETE
        MergeIntoAction action = new MergeIntoAction(warehouse, database, "T");
        action.withSource("SELECT * FROM S WHERE k < 12", "SS")
                .withMergeCondition("T.k = SS.k AND T.dt = SS.dt")
                .withMatchedUpsert(
                        "T.v <> SS.v AND SS.v IS NOT NULL",
                        "v = SS.v, last_action = 'matched_upsert'")
                .withMatchedDelete("SS.v IS NULL")
                .withNotMatchedInsert(null, "SS.k, SS.v, 'insert', SS.dt")
                .withNotMatchedBySourceUpsert(
                        "dt < '02-28'", "v = v || '_nmu', last_action = 'not_matched_upsert'")
                .withNotMatchedBySourceDelete("dt >= '02-28'");

        validateActionRunResult(
                action,
                Arrays.asList(
                        changelogRow("-U", 7, "v_7", "creation", "02-28"),
                        changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                        changelogRow("-D", 4, "v_4", "creation", "02-27"),
                        changelogRow("-D", 8, "v_8", "creation", "02-28"),
                        changelogRow("+I", 8, "v_8", "insert", "02-29"),
                        changelogRow("+I", 11, "v_11", "insert", "02-29"),
                        changelogRow("-U", 2, "v_2", "creation", "02-27"),
                        changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("-U", 3, "v_3", "creation", "02-27"),
                        changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("-D", 5, "v_5", "creation", "02-28"),
                        changelogRow("-D", 6, "v_6", "creation", "02-28"),
                        changelogRow("-D", 9, "v_9", "creation", "02-28"),
                        changelogRow("-D", 10, "v_10", "creation", "02-28")),
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                        changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                        changelogRow("+I", 8, "v_8", "insert", "02-29"),
                        changelogRow("+I", 11, "v_11", "insert", "02-29")));
    }

    @Test
    public void testMatchedUpsertSetAll() throws Exception {
        // prepare table T
        prepareTable(CoreOptions.ChangelogProducer.NONE);

        // build MergeIntoAction
        MergeIntoAction action = new MergeIntoAction(warehouse, database, "T");
        action.withSource("SELECT k, v, 'unknown', dt FROM S", "SS")
                .withMergeCondition("T.k = SS.k AND T.dt = SS.dt")
                .withMatchedUpsert(null, "*");

        validateActionRunResult(
                action,
                Arrays.asList(
                        changelogRow("-U", 1, "v_1", "creation", "02-27"),
                        changelogRow("+U", 1, "v_1", "unknown", "02-27"),
                        changelogRow("-U", 4, "v_4", "creation", "02-27"),
                        changelogRow("+U", 4, null, "unknown", "02-27"),
                        changelogRow("-U", 7, "v_7", "creation", "02-28"),
                        changelogRow("+U", 7, "Seven", "unknown", "02-28"),
                        changelogRow("-U", 8, "v_8", "creation", "02-28"),
                        changelogRow("+U", 8, null, "unknown", "02-28")),
                Arrays.asList(
                        changelogRow("+U", 1, "v_1", "unknown", "02-27"),
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+U", 4, null, "unknown", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+U", 7, "Seven", "unknown", "02-28"),
                        changelogRow("+U", 8, null, "unknown", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28")));
    }

    @Test
    public void testNotMatchedInsertAll() throws Exception {
        // prepare table T
        prepareTable(CoreOptions.ChangelogProducer.NONE);

        // build MergeIntoAction
        MergeIntoAction action = new MergeIntoAction(warehouse, database, "T");
        action.withSource("SELECT k, v, 'unknown', dt FROM S", "SS")
                .withMergeCondition("T.k = SS.k AND T.dt = SS.dt")
                .withNotMatchedInsert("SS.k < 12", "*");

        validateActionRunResult(
                action,
                Arrays.asList(
                        changelogRow("+I", 8, "v_8", "unknown", "02-29"),
                        changelogRow("+I", 11, "v_11", "unknown", "02-29")),
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+I", 4, "v_4", "creation", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+I", 7, "v_7", "creation", "02-28"),
                        changelogRow("+I", 8, "v_8", "creation", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28"),
                        changelogRow("+I", 8, "v_8", "unknown", "02-29"),
                        changelogRow("+I", 11, "v_11", "unknown", "02-29")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Negative tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testInsertChangesActionWithNonPkTable() {
        String nonPkTable =
                createTable(
                        Collections.singletonList("k int"),
                        Collections.emptyList(),
                        Collections.emptyList());

        assertThatThrownBy(() -> new MergeIntoAction(warehouse, database, nonPkTable))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "merge-into action doesn't support table with no primary keys defined.");
    }

    @Test
    public void testIncompatibleSchema() throws Exception {
        // prepare table T
        prepareTable(CoreOptions.ChangelogProducer.NONE);

        // build MergeIntoAction
        MergeIntoAction action = new MergeIntoAction(warehouse, database, "T");
        action.withSourceTable("S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withNotMatchedInsert(null, "S.k, S.v, 0, S.dt");

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The schema of result in action 'not-matched-insert' is invalid.\n"
                                + "Result schema:   [INT NOT NULL, STRING, INT NOT NULL, STRING NOT NULL]\n"
                                + "Expected schema: [INT NOT NULL, STRING, STRING, STRING NOT NULL]");
    }

    private void validateActionRunResult(
            MergeIntoAction action, List<Row> streamingExpected, List<Row> batchExpected)
            throws Exception {
        BlockingIterator<Row, Row> iterator =
                testStreamingRead(buildSimpleQuery("T"), initialRecords);
        action.run();
        // test streaming read
        validateStreamingReadResult(iterator, streamingExpected);
        iterator.close();
        // test batch read
        testBatchRead(buildSimpleQuery("T"), batchExpected);
    }

    private void prepareTable(CoreOptions.ChangelogProducer producer) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("k INT", "v STRING", "last_action STRING", "dt STRING"),
                        Arrays.asList("k", "dt"),
                        Collections.singletonList("dt"),
                        new HashMap<String, String>() {
                            {
                                put(CHANGELOG_PRODUCER.key(), producer.toString());
                            }
                        }));

        insertInto(
                "T",
                "(1, 'v_1', 'creation', '02-27')",
                "(2, 'v_2', 'creation', '02-27')",
                "(3, 'v_3', 'creation', '02-27')",
                "(4, 'v_4', 'creation', '02-27')",
                "(5, 'v_5', 'creation', '02-28')",
                "(6, 'v_6', 'creation', '02-28')",
                "(7, 'v_7', 'creation', '02-28')",
                "(8, 'v_8', 'creation', '02-28')",
                "(9, 'v_9', 'creation', '02-28')",
                "(10, 'v_10', 'creation', '02-28')");
    }

    private static List<Arguments> producerTestData() {
        return Arrays.asList(
                arguments(
                        CoreOptions.ChangelogProducer.NONE,
                        Arrays.asList(
                                changelogRow("-U", 7, "v_7", "creation", "02-28"),
                                changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                                changelogRow("-D", 4, "v_4", "creation", "02-27"),
                                changelogRow("-D", 8, "v_8", "creation", "02-28"),
                                changelogRow("+I", 8, "v_8", "insert", "02-29"),
                                changelogRow("+I", 11, "v_11", "insert", "02-29"),
                                changelogRow("+I", 12, "v_12", "insert", "02-29"),
                                changelogRow("-U", 2, "v_2", "creation", "02-27"),
                                changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("-U", 3, "v_3", "creation", "02-27"),
                                changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("-D", 5, "v_5", "creation", "02-28"),
                                changelogRow("-D", 6, "v_6", "creation", "02-28"),
                                changelogRow("-D", 9, "v_9", "creation", "02-28"),
                                changelogRow("-D", 10, "v_10", "creation", "02-28"))),
                arguments(
                        CoreOptions.ChangelogProducer.INPUT,
                        Arrays.asList(
                                changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                                changelogRow("-D", 4, "v_4", "creation", "02-27"),
                                changelogRow("-D", 8, "v_8", "creation", "02-28"),
                                changelogRow("+I", 8, "v_8", "insert", "02-29"),
                                changelogRow("+I", 11, "v_11", "insert", "02-29"),
                                changelogRow("+I", 12, "v_12", "insert", "02-29"),
                                changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("-D", 5, "v_5", "creation", "02-28"),
                                changelogRow("-D", 6, "v_6", "creation", "02-28"),
                                changelogRow("-D", 9, "v_9", "creation", "02-28"),
                                changelogRow("-D", 10, "v_10", "creation", "02-28"))),
                arguments(
                        CoreOptions.ChangelogProducer.FULL_COMPACTION,
                        Arrays.asList(
                                changelogRow("-U", 7, "v_7", "creation", "02-28"),
                                changelogRow("+U", 7, "Seven", "matched_upsert", "02-28"),
                                changelogRow("-D", 4, "v_4", "creation", "02-27"),
                                changelogRow("-D", 8, "v_8", "creation", "02-28"),
                                changelogRow("+I", 8, "v_8", "insert", "02-29"),
                                changelogRow("+I", 11, "v_11", "insert", "02-29"),
                                changelogRow("+I", 12, "v_12", "insert", "02-29"),
                                changelogRow("-U", 2, "v_2", "creation", "02-27"),
                                changelogRow("+U", 2, "v_2_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("-U", 3, "v_3", "creation", "02-27"),
                                changelogRow("+U", 3, "v_3_nmu", "not_matched_upsert", "02-27"),
                                changelogRow("-D", 5, "v_5", "creation", "02-28"),
                                changelogRow("-D", 6, "v_6", "creation", "02-28"),
                                changelogRow("-D", 9, "v_9", "creation", "02-28"),
                                changelogRow("-D", 10, "v_10", "creation", "02-28"))));
    }
}
