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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.CoreOptions.DATA_EVOLUTION_ENABLED;
import static org.apache.paimon.CoreOptions.ROW_TRACKING_ENABLED;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.bEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.buildDdl;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.insertInto;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.sEnv;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testBatchRead;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** ITCase for {@link DataEvolutionMergeIntoAction}. */
public class DataEvolutionMergeIntoActionITCase extends ActionITCaseBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionMergeIntoActionITCase.class);

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of(true, "action"),
                Arguments.of(false, "action"),
                Arguments.of(true, "procedure"),
                Arguments.of(false, "procedure"));
    }

    @BeforeEach
    public void setup() throws Exception {
        init(warehouse);

        prepareTargetTable();

        prepareSourceTable();
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testUpdateSingleColumn(boolean inDefault, String invoker) throws Exception {
        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "new_name1"),
                        changelogRow("+I", 2, "name2"),
                        changelogRow("+I", 3, "name3"),
                        changelogRow("+I", 7, "new_name7"),
                        changelogRow("+I", 8, "name8"),
                        changelogRow("+I", 11, "new_name11"),
                        changelogRow("+I", 15, null),
                        changelogRow("+I", 18, "new_name18"));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("T.id=S.id")
                            .withMatchedUpdateSet("T.name=S.name")
                            .withSourceTable("S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T.id=S.id', 'name=S.name', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead("SELECT id, name FROM T where id in (1, 2, 3, 7, 8, 11, 15, 18)", expected);
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testUpdateMultipleColumns(boolean inDefault, String invoker) throws Exception {
        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "new_name1", 100.1),
                        changelogRow("+I", 2, "name2", 0.2),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 7, "new_name7", null),
                        changelogRow("+I", 8, "name8", 0.8),
                        changelogRow("+I", 11, "new_name11", 101.1),
                        changelogRow("+I", 15, null, 101.1),
                        changelogRow("+I", 18, "new_name18", 101.8));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("T.id=S.id")
                            .withMatchedUpdateSet("T.name=S.name,T.value=S.`value`")
                            .withSourceTable("S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T.id=S.id', 'name=S.name,value=S.`value`', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T where id in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testSetLiterals(boolean inDefault, String invoker) throws Exception {
        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "testName", 0.0),
                        changelogRow("+I", 2, "name2", 0.2),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 7, "testName", 0.0),
                        changelogRow("+I", 8, "name8", 0.8),
                        changelogRow("+I", 11, "testName", 0.0),
                        changelogRow("+I", 15, "testName", 0.0),
                        changelogRow("+I", 18, "testName", 0.0));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("T.id=S.id")
                            .withMatchedUpdateSet("T.name='testName',T.value=CAST(0.0 as DOUBLE)")
                            .withSourceTable("S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T.id=S.id', 'name=''testName'',value=CAST(0.0 as DOUBLE)', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T where id in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testUpdatePartitionColumnThrowsError(boolean inDefault, String invoker)
            throws Exception {
        Throwable t;
        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, database, "T")
                            .withMergeCondition("T.id=S.id")
                            .withMatchedUpdateSet("T.dt=S.id")
                            .withSourceTable("S")
                            .withSinkParallelism(2);
            t = Assertions.assertThrows(IllegalStateException.class, () -> builder.build().run());
            Assertions.assertTrue(
                    t.getMessage().startsWith("User should not update partition columns:"));
        } else {
            String procedureStatement =
                    "CALL sys.data_evolution_merge_into('default.T', '', '', 'S', 'T.id=S.id', 'dt=S.id', 2)";
            t =
                    Assertions.assertThrows(
                            Exception.class, () -> executeSQL(procedureStatement, false, true));
            org.assertj.core.api.Assertions.assertThat(t)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .message()
                    .contains("User should not update partition columns:");
        }
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testOneToManyUpdate(boolean inDefault, String invoker) throws Exception {
        // A single row in the target table may map to multiple rows in the source table.
        // For example:
        // In target table we have: (id=1, value='val', _ROW_ID=0)
        // In source table we have: (id=1, value='val1'), (id=1, value='val2')
        // If we execute MERGE INTO T SET T.`value`=S.`VALUE` ON T.id=S.id
        // There would be 2 rows with the same _ROW_ID but different new values:
        // (id=1, value='val1', _ROW_ID=0) and (id=1, value='val2', _ROW_ID=0)
        // At that case, we will choose a random row as the final result
        insertInto("S", "(1, 'dup_new_name1', 200.1)");

        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        // First validate results except of id=1 row.
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 2, "name2", 0.2),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 7, "new_name7", null),
                        changelogRow("+I", 8, "name8", 0.8),
                        changelogRow("+I", 11, "new_name11", 101.1),
                        changelogRow("+I", 15, null, 101.1),
                        changelogRow("+I", 18, "new_name18", 101.8));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("T.id=S.id")
                            .withMatchedUpdateSet("T.name=S.name,T.value=S.`value`")
                            .withSourceTable("S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T.id=S.id', 'name=S.name,value=S.`value`', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T where id in (2, 3, 7, 8, 11, 15, 18)", expected);

        // then validate id=1 row
        List<Row> possibleRows =
                Arrays.asList(
                        changelogRow("+I", 1, "dup_new_name1", 200.1),
                        changelogRow("+I", 1, "new_name1", 100.1));
        boolean passed = false;
        for (Row row : possibleRows) {
            try {
                testBatchRead(
                        "SELECT id, name, `value` FROM T where id = 1",
                        Collections.singletonList(row));
                passed = true;
                break;
            } catch (Throwable e) {
                // error happens, just log it.
                LOG.info("Error happens in testing one-to-many merge into.", e);
            }
        }

        Assertions.assertTrue(
                passed,
                "one-to-many merge into test fails, please check log for more information.");
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testAlias(boolean inDefault, String invoker) throws Exception {
        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "new_name1", 100.1),
                        changelogRow("+I", 2, "name2", 0.2),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 7, "new_name7", null),
                        changelogRow("+I", 8, "name8", 0.8),
                        changelogRow("+I", 11, "new_name11", 101.1),
                        changelogRow("+I", 15, null, 101.1),
                        changelogRow("+I", 18, "new_name18", 101.8));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("TempT.id=S.id")
                            .withMatchedUpdateSet("TempT.name=S.name,TempT.value=S.`value`")
                            .withSourceTable("S")
                            .withTargetAlias("TempT")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', 'TempT', '', 'S', 'TempT.id=S.id', 'name=S.name,value=S.`value`', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T where id in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testSqls(boolean inDefault, String invoker) throws Exception {
        String targetDb = inDefault ? database : "test_db";
        if (!inDefault) {
            // create target table in a new database
            sEnv.executeSql("DROP TABLE T");
            sEnv.executeSql("CREATE DATABASE test_db");
            sEnv.executeSql("USE test_db");
            bEnv.executeSql("USE test_db");
            prepareTargetTable();
        }

        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "new_name1", 100.1),
                        changelogRow("+I", 2, "name2", 0.2),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 7, "new_name7", null),
                        changelogRow("+I", 8, "name8", 0.8),
                        changelogRow("+I", 11, "new_name11", 101.1),
                        changelogRow("+I", 15, null, 101.1),
                        changelogRow("+I", 18, "new_name18", 101.8));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("TempT.id=SS.id")
                            .withMatchedUpdateSet("TempT.name=SS.name,TempT.value=SS.`value`")
                            .withSourceTable("SS")
                            .withTargetAlias("TempT")
                            .withSourceSqls(
                                    "CREATE TEMPORARY VIEW SS AS SELECT id, name, `value` FROM S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', 'TempT', 'CREATE TEMPORARY VIEW SS AS SELECT id, name, `value` FROM S',"
                                    + " 'SS', 'TempT.id=SS.id', 'name=SS.name,value=SS.`value`', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T where id in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @Test
    public void testRewriteMergeCondition() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("warehouse", warehouse);
        DataEvolutionMergeIntoAction action =
                new DataEvolutionMergeIntoAction(database, "T", config);

        String mergeCondition = "T.id=S.id";
        assertEquals("`RT`.id=S.id", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "`T`.id=S.id";
        assertEquals("`RT`.id=S.id", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "t.id = s.id AND T.pt = s.pt";
        assertEquals(
                "`RT`.id = s.id AND `RT`.pt = s.pt", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "TT.id = 1 AND T.id = 2";
        assertEquals("TT.id = 1 AND `RT`.id = 2", action.rewriteMergeCondition(mergeCondition));

        mergeCondition = "TT.id = 'T.id' AND T.id = \"T.id\"";
        assertEquals(
                "TT.id = 'T.id' AND `RT`.id = \"T.id\"",
                action.rewriteMergeCondition(mergeCondition));
    }

    private void prepareTargetTable() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "T",
                        Arrays.asList("id INT", "name STRING", "`value` DOUBLE", "dt STRING"),
                        Collections.emptyList(),
                        Collections.singletonList("dt"),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto(
                "T",
                "(1, 'name1', 0.1, '01-22')",
                "(2, 'name2', 0.2, '01-22')",
                "(3, 'name3', 0.3, '01-22')",
                "(4, 'name4', 0.4, '01-22')",
                "(5, 'name5', 0.5, '01-22')",
                "(6, 'name6', 0.6, '01-22')",
                "(7, 'name7', 0.7, '01-22')",
                "(8, 'name8', 0.8, '01-22')",
                "(9, 'name9', 0.9, '01-22')",
                "(10, 'name10', 1.0, '01-22')");

        insertInto(
                "T",
                "(11, 'name11', 1.1, '01-22')",
                "(12, 'name12', 1.2, '01-22')",
                "(13, 'name13', 1.3, '01-22')",
                "(14, 'name14', 1.4, '01-22')",
                "(15, 'name15', 1.5, '01-22')",
                "(16, 'name16', 1.6, '01-22')",
                "(17, 'name17', 1.7, '01-22')",
                "(18, 'name18', 1.8, '01-22')",
                "(19, 'name19', 1.9, '01-22')",
                "(20, 'name20', 2.0, '01-22')");
    }

    private void prepareSourceTable() throws Exception {
        sEnv.executeSql(
                buildDdl(
                        "S",
                        Arrays.asList("id INT", "name STRING", "`value` DOUBLE"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto(
                "S",
                "(1, 'new_name1', 100.1)",
                "(7, 'new_name7', CAST(NULL AS DOUBLE))",
                "(11, 'new_name11', 101.1)",
                "(15, CAST(NULL AS STRING), 101.1)",
                "(18, 'new_name18', 101.8)",
                "(21, 'new_name21', 102.1)");
    }

    private DataEvolutionMergeIntoActionBuilder builder(
            String warehouse, String database, String table) {
        return new DataEvolutionMergeIntoActionBuilder(warehouse, database, table);
    }

    private class DataEvolutionMergeIntoActionBuilder {
        private final List<String> args;

        DataEvolutionMergeIntoActionBuilder(String warehouse, String database, String table) {
            this.args =
                    new ArrayList<>(
                            Arrays.asList(
                                    "data_evolution_merge_into",
                                    "--warehouse",
                                    warehouse,
                                    "--database",
                                    database,
                                    "--table",
                                    table));
        }

        public DataEvolutionMergeIntoActionBuilder withTargetAlias(String targetAlias) {
            if (targetAlias != null) {
                args.add("--target_as");
                args.add(targetAlias);
            }
            return this;
        }

        public DataEvolutionMergeIntoActionBuilder withSourceTable(String sourceTable) {
            args.add("--source_table");
            args.add(sourceTable);
            return this;
        }

        public DataEvolutionMergeIntoActionBuilder withSourceSqls(String... sourceSqls) {
            if (sourceSqls != null) {
                for (String sql : sourceSqls) {
                    args.add("--source_sql");
                    args.add(sql);
                }
            }
            return this;
        }

        public DataEvolutionMergeIntoActionBuilder withMergeCondition(String mergeCondition) {
            args.add("--on");
            args.add(mergeCondition);
            return this;
        }

        public DataEvolutionMergeIntoActionBuilder withMatchedUpdateSet(String matchedUpdateSet) {
            args.add("--matched_update_set");
            args.add(matchedUpdateSet);
            return this;
        }

        public DataEvolutionMergeIntoActionBuilder withSinkParallelism(int sinkParallelism) {
            args.add("--sink_parallelism");
            args.add(String.valueOf(sinkParallelism));
            return this;
        }

        public DataEvolutionMergeIntoAction build() {
            return createAction(DataEvolutionMergeIntoAction.class, args);
        }
    }
}
