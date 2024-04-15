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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.MATCHED_DELETE;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.MATCHED_UPSERT;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_BY_SOURCE_DELETE;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_BY_SOURCE_UPSERT;
import static org.apache.paimon.flink.action.MergeIntoActionFactory.NOT_MATCHED_INSERT;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildSimpleQuery;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.createTable;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.validateStreamingReadResult;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
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

        // prepare target table T
        prepareTargetTable(CoreOptions.ChangelogProducer.NONE);

        // prepare source table S
        prepareSourceTable();
    }

    @ParameterizedTest(name = "changelog-producer = {0}")
    @MethodSource("producerTestData")
    public void testVariousChangelogProducer(
            CoreOptions.ChangelogProducer producer, List<Row> expected) throws Exception {
        // re-create target table with given producer
        sEnv.executeSql("DROP TABLE T");
        prepareTargetTable(producer);

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
        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, database, "T");
        // here test if it works when table S is in default and qualified both
        action.withSourceTable("default.S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withMatchedUpsert(
                        "T.v <> S.v AND S.v IS NOT NULL", "v = S.v, last_action = 'matched_upsert'")
                .withMatchedDelete("S.v IS NULL")
                .withNotMatchedInsert(null, "S.k, S.v, 'insert', S.dt")
                .withNotMatchedBySourceUpsert(
                        "dt < '02-28'", "v = v || '_nmu', last_action = 'not_matched_upsert'")
                .withNotMatchedBySourceDelete("dt >= '02-28'");

        validateActionRunResult(
                action.build(),
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

    @ParameterizedTest(name = "in-default = {0}")
    @ValueSource(booleans = {true, false})
    public void testTargetAlias(boolean inDefault) throws Exception {
        MergeIntoActionBuilder action;

        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable(CoreOptions.ChangelogProducer.NONE);

            action = new MergeIntoActionBuilder(warehouse, "test_db", "T");
        } else {
            action = new MergeIntoActionBuilder(warehouse, database, "T");
        }

        action.withTargetAlias("TT")
                .withSourceTable("S")
                .withMergeCondition("TT.k = S.k AND TT.dt = S.dt")
                .withMatchedDelete("S.v IS NULL");

        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('%s.T', 'TT', '', 'S', 'TT.k = S.k AND TT.dt = S.dt', 'S.v IS NULL')",
                        inDefault ? database : "test_db");

        List<Row> streamingExpected =
                Arrays.asList(
                        changelogRow("-D", 4, "v_4", "creation", "02-27"),
                        changelogRow("-D", 8, "v_8", "creation", "02-28"));

        List<Row> batchExpected =
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+I", 7, "v_7", "creation", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28"));

        if (ThreadLocalRandom.current().nextBoolean()) {
            validateActionRunResult(action.build(), streamingExpected, batchExpected);
        } else {
            validateProcedureResult(procedureStatement, streamingExpected, batchExpected);
        }
    }

    @ParameterizedTest(name = "in-default = {0}")
    @ValueSource(booleans = {true, false})
    public void testSourceName(boolean inDefault) throws Exception {
        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, "default", "T");
        String sourceTableName = "S";

        if (!inDefault) {
            // create source table in a new database
            sEnv.executeSql("DROP TABLE S");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareSourceTable();
            sourceTableName = "test_db.S";
        }

        action.withSourceTable(sourceTableName)
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withMatchedDelete("S.v IS NULL");

        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('default.T', '', '', '%s', 'T.k = S.k AND T.dt = S.dt', 'S.v IS NULL')",
                        sourceTableName);

        if (!inDefault) {
            sEnv.executeSql("USE `default`");
            bEnv.executeSql("USE `default`");
        }

        List<Row> streamingExpected =
                Arrays.asList(
                        changelogRow("-D", 4, "v_4", "creation", "02-27"),
                        changelogRow("-D", 8, "v_8", "creation", "02-28"));

        List<Row> batchExpected =
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+I", 7, "v_7", "creation", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28"));

        if (ThreadLocalRandom.current().nextBoolean()) {
            validateActionRunResult(action.build(), streamingExpected, batchExpected);
        } else {
            validateProcedureResult(procedureStatement, streamingExpected, batchExpected);
        }
    }

    @ParameterizedTest(name = "useCatalog = {0}")
    @ValueSource(booleans = {true, false})
    public void testSqls(boolean useCatalog) throws Exception {
        // drop table S
        sEnv.executeSql("DROP TABLE S");

        String catalog =
                String.format(
                        "CREATE CATALOG test_cat WITH ('type' = 'paimon', 'warehouse' = '%s')",
                        getTempDirPath());
        String escapeCatalog = catalog.replaceAll("'", "''");
        String id =
                TestValuesTableFactory.registerData(
                        Arrays.asList(
                                changelogRow("+I", 1, "v_1", "02-27"),
                                changelogRow("+I", 4, null, "02-27"),
                                changelogRow("+I", 8, null, "02-28")));
        String ddl =
                String.format(
                        "CREATE TEMPORARY TABLE %s (k INT, v STRING, dt STRING)\n"
                                + "WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '%s')",
                        useCatalog ? "S" : "test_cat.`default`.S", id);
        String escapeDdl = ddl.replaceAll("'", "''");

        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, database, "T");

        if (useCatalog) {
            action.withSourceSqls(catalog, "USE CATALOG test_cat", ddl);
            // test current catalog and current database
            action.withSourceTable("S");
        } else {
            action.withSourceSqls(catalog, ddl);
            action.withSourceTable("test_cat.default.S");
        }

        action.withMergeCondition("T.k = S.k AND T.dt = S.dt").withMatchedDelete("S.v IS NULL");

        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('%s.T', '', '%s', '%s', 'T.k = S.k AND T.dt = S.dt', 'S.v IS NULL')",
                        database,
                        useCatalog
                                ? String.format(
                                        "%s;%s;%s",
                                        escapeCatalog, "USE CATALOG test_cat", escapeDdl)
                                : String.format("%s;%s", escapeCatalog, escapeDdl),
                        useCatalog ? "S" : "test_cat.default.S");

        List<Row> streamingExpected =
                Arrays.asList(
                        changelogRow("-D", 4, "v_4", "creation", "02-27"),
                        changelogRow("-D", 8, "v_8", "creation", "02-28"));

        List<Row> batchExpected =
                Arrays.asList(
                        changelogRow("+I", 1, "v_1", "creation", "02-27"),
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+I", 7, "v_7", "creation", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28"));

        if (ThreadLocalRandom.current().nextBoolean()) {
            validateActionRunResult(action.build(), streamingExpected, batchExpected);
        } else {
            validateProcedureResult(procedureStatement, streamingExpected, batchExpected);
        }
    }

    @ParameterizedTest(name = "source-qualified = {0}")
    @ValueSource(booleans = {true, false})
    public void testMatchedUpsertSetAll(boolean qualified) throws Exception {
        // build MergeIntoAction
        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, database, "T");
        action.withSourceSqls("CREATE TEMPORARY VIEW SS AS SELECT k, v, 'unknown', dt FROM S")
                .withSourceTable(qualified ? "default.SS" : "SS")
                .withMergeCondition("T.k = SS.k AND T.dt = SS.dt")
                .withMatchedUpsert(null, "*");

        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('%s.T', '', '%s', '%s', 'T.k = SS.k AND T.dt = SS.dt', '', '*')",
                        database,
                        "CREATE TEMPORARY VIEW SS AS SELECT k, v, ''unknown'', dt FROM S",
                        qualified ? "default.SS" : "SS");

        List<Row> streamingExpected =
                Arrays.asList(
                        changelogRow("-U", 1, "v_1", "creation", "02-27"),
                        changelogRow("+U", 1, "v_1", "unknown", "02-27"),
                        changelogRow("-U", 4, "v_4", "creation", "02-27"),
                        changelogRow("+U", 4, null, "unknown", "02-27"),
                        changelogRow("-U", 7, "v_7", "creation", "02-28"),
                        changelogRow("+U", 7, "Seven", "unknown", "02-28"),
                        changelogRow("-U", 8, "v_8", "creation", "02-28"),
                        changelogRow("+U", 8, null, "unknown", "02-28"));

        List<Row> batchExpected =
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
                        changelogRow("+I", 10, "v_10", "creation", "02-28"));

        if (ThreadLocalRandom.current().nextBoolean()) {
            validateActionRunResult(action.build(), streamingExpected, batchExpected);
        } else {
            validateProcedureResult(procedureStatement, streamingExpected, batchExpected);
        }
    }

    @ParameterizedTest(name = "source-qualified = {0}")
    @ValueSource(booleans = {true, false})
    public void testNotMatchedInsertAll(boolean qualified) throws Exception {
        // build MergeIntoAction
        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, database, "T");
        action.withSourceSqls("CREATE TEMPORARY VIEW SS AS SELECT k, v, 'unknown', dt FROM S")
                .withSourceTable(qualified ? "default.SS" : "SS")
                .withMergeCondition("T.k = SS.k AND T.dt = SS.dt")
                .withNotMatchedInsert("SS.k < 12", "*");

        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('%s.T', '', '%s', '%s', 'T.k = SS.k AND T.dt = SS.dt', '', '', 'SS.k < 12', '*', '')",
                        database,
                        "CREATE TEMPORARY VIEW SS AS SELECT k, v, ''unknown'', dt FROM S",
                        qualified ? "default.SS" : "SS");

        List<Row> streamingExpected =
                Arrays.asList(
                        changelogRow("+I", 8, "v_8", "unknown", "02-29"),
                        changelogRow("+I", 11, "v_11", "unknown", "02-29"));

        List<Row> batchExpected =
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
                        changelogRow("+I", 11, "v_11", "unknown", "02-29"));

        if (ThreadLocalRandom.current().nextBoolean()) {
            validateActionRunResult(action.build(), streamingExpected, batchExpected);
        } else {
            validateProcedureResult(procedureStatement, streamingExpected, batchExpected);
        }
    }

    @Test
    public void testProcedureWithDeleteConditionTrue() throws Exception {
        String procedureStatement =
                String.format(
                        "CALL sys.merge_into('%s.T', '', '', 'S', 'T.k = S.k AND T.dt = S.dt', 'TRUE')",
                        database);

        validateProcedureResult(
                procedureStatement,
                Arrays.asList(
                        changelogRow("-D", 1, "v_1", "creation", "02-27"),
                        changelogRow("-D", 4, "v_4", "creation", "02-27"),
                        changelogRow("-D", 7, "v_7", "creation", "02-28"),
                        changelogRow("-D", 8, "v_8", "creation", "02-28")),
                Arrays.asList(
                        changelogRow("+I", 2, "v_2", "creation", "02-27"),
                        changelogRow("+I", 3, "v_3", "creation", "02-27"),
                        changelogRow("+I", 5, "v_5", "creation", "02-28"),
                        changelogRow("+I", 6, "v_6", "creation", "02-28"),
                        changelogRow("+I", 9, "v_9", "creation", "02-28"),
                        changelogRow("+I", 10, "v_10", "creation", "02-28")));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Negative tests
    // ----------------------------------------------------------------------------------------------------------------

    @Test
    public void testNonPkTable() {
        String nonPkTable =
                createTable(
                        Collections.singletonList("k int"),
                        Collections.emptyList(),
                        Collections.emptyList());

        assertThatThrownBy(
                        () -> new MergeIntoActionBuilder(warehouse, database, nonPkTable).build())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "merge-into action doesn't support table with no primary keys defined.");
    }

    @Test
    public void testIncompatibleSchema() {
        // build MergeIntoAction
        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, database, "T");
        action.withSourceTable("S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withNotMatchedInsert(null, "S.k, S.v, 0, S.dt");

        assertThatThrownBy(() -> action.build().run())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The schema of result in action 'not-matched-insert' is invalid.\n"
                                + "Result schema:   [INT NOT NULL, STRING, INT NOT NULL, STRING NOT NULL]\n"
                                + "Expected schema: [INT NOT NULL, STRING, STRING, STRING NOT NULL]");
    }

    @Test
    public void testIllegalSourceName() throws Exception {
        // create source table in a new database
        sEnv.executeSql("DROP TABLE S");
        sEnv.executeSql("CREATE DATABASE test_db");
        sEnv.executeSql("USE test_db");
        prepareSourceTable();

        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, "default", "T");
        // the qualified path of source table is absent
        action.withSourceTable("S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withMatchedDelete("S.v IS NULL");

        assertThatThrownBy(() -> action.build().run())
                .satisfies(anyCauseMatches(ValidationException.class, "Object 'S' not found"));
    }

    @Test
    public void testIllegalSourceNameSqlCase() {
        // drop table S
        sEnv.executeSql("DROP TABLE S");

        MergeIntoActionBuilder action = new MergeIntoActionBuilder(warehouse, "default", "T");
        action.withSourceSqls(
                        "CREATE DATABASE test_db",
                        "CREATE TEMPORARY TABLE test_db.S (k INT, v STRING, dt STRING) WITH ('connector' = 'values', 'bounded' = 'true')")
                // the qualified path of source table  is absent
                .withSourceTable("S")
                .withMergeCondition("T.k = S.k AND T.dt = S.dt")
                .withMatchedDelete("S.v IS NULL");

        assertThatThrownBy(() -> action.build().run())
                .satisfies(anyCauseMatches(ValidationException.class, "Object 'S' not found"));
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

    private void validateProcedureResult(
            String procedureStatement, List<Row> streamingExpected, List<Row> batchExpected)
            throws Exception {
        BlockingIterator<Row, Row> iterator =
                testStreamingRead(buildSimpleQuery("T"), initialRecords);
        callProcedure(procedureStatement, true, true);
        // test batch read first to ensure TABLE_DML_SYNC works
        testBatchRead(buildSimpleQuery("T"), batchExpected);
        // test streaming read
        validateStreamingReadResult(iterator, streamingExpected);
        iterator.close();
    }

    private void prepareTargetTable(CoreOptions.ChangelogProducer producer) throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("k INT", "v STRING", "last_action STRING", "dt STRING"),
                        Arrays.asList("k", "dt"),
                        Collections.singletonList("dt"),
                        new HashMap<String, String>() {
                            {
                                put(CHANGELOG_PRODUCER.key(), producer.toString());
                                // test works with partial update normally
                                if (producer == CoreOptions.ChangelogProducer.LOOKUP) {
                                    put(
                                            CoreOptions.MERGE_ENGINE.key(),
                                            CoreOptions.MergeEngine.PARTIAL_UPDATE.toString());
                                    put(CoreOptions.IGNORE_DELETE.key(), "true");
                                }
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

    private void prepareSourceTable() throws Exception {
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

    private class MergeIntoActionBuilder {

        private final List<String> args;
        private final List<String> mergeActions;

        public MergeIntoActionBuilder(String warehouse, String database, String table) {
            this.args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "merge_into",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    table));
            this.mergeActions = new ArrayList<>();
        }

        public MergeIntoActionBuilder withTargetAlias(String targetAlias) {
            if (targetAlias != null) {
                args.add("--target_as");
                args.add(targetAlias);
            }
            return this;
        }

        public MergeIntoActionBuilder withSourceTable(String sourceTable) {
            args.add("--source_table");
            args.add(sourceTable);
            return this;
        }

        public MergeIntoActionBuilder withSourceSqls(String... sourceSqls) {
            if (sourceSqls != null) {
                for (String sql : sourceSqls) {
                    args.add("--source_sql");
                    args.add(sql);
                }
            }
            return this;
        }

        public MergeIntoActionBuilder withMergeCondition(String mergeCondition) {
            args.add("--on");
            args.add(mergeCondition);
            return this;
        }

        public MergeIntoActionBuilder withMatchedUpsert(
                @Nullable String matchedUpsertCondition, String matchedUpsertSet) {
            mergeActions.add(MATCHED_UPSERT);
            args.add("--matched_upsert_set");
            args.add(matchedUpsertSet);
            if (matchedUpsertCondition != null) {
                args.add("--matched_upsert_condition");
                args.add(matchedUpsertCondition);
            }
            return this;
        }

        public MergeIntoActionBuilder withNotMatchedBySourceUpsert(
                @Nullable String notMatchedBySourceUpsertCondition,
                String notMatchedBySourceUpsertSet) {
            mergeActions.add(NOT_MATCHED_BY_SOURCE_UPSERT);
            args.add("--not_matched_by_source_upsert_set");
            args.add(notMatchedBySourceUpsertSet);
            if (notMatchedBySourceUpsertCondition != null) {
                args.add("--not_matched_by_source_upsert_condition");
                args.add(notMatchedBySourceUpsertCondition);
            }
            return this;
        }

        public MergeIntoActionBuilder withMatchedDelete(@Nullable String matchedDeleteCondition) {
            mergeActions.add(MATCHED_DELETE);
            if (matchedDeleteCondition != null) {
                args.add("--matched_delete_condition");
                args.add(matchedDeleteCondition);
            }
            return this;
        }

        public MergeIntoActionBuilder withNotMatchedBySourceDelete(
                @Nullable String notMatchedBySourceDeleteCondition) {
            mergeActions.add(NOT_MATCHED_BY_SOURCE_DELETE);
            if (notMatchedBySourceDeleteCondition != null) {
                args.add("--not_matched_by_source_delete_condition");
                args.add(notMatchedBySourceDeleteCondition);
            }
            return this;
        }

        public MergeIntoActionBuilder withNotMatchedInsert(
                @Nullable String notMatchedInsertCondition, String notMatchedInsertValues) {
            mergeActions.add(NOT_MATCHED_INSERT);
            args.add("--not_matched_insert_values");
            args.add(notMatchedInsertValues);
            if (notMatchedInsertCondition != null) {
                args.add("--not_matched_insert_condition");
                args.add(notMatchedInsertCondition);
            }
            return this;
        }

        MergeIntoAction build() {
            args.add("--merge_actions");
            args.add(String.join(",", mergeActions));
            return createAction(MergeIntoAction.class, args);
        }
    }
}
