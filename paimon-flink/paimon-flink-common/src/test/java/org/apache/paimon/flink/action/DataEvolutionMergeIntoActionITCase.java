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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.functions.ScalarFunction;
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
import java.util.Optional;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                            .withMatchedUpdateSet("T.value=S.`value`,T.name=S.name")
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

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testRowIdColumnContainedInSource(boolean inDefault, String invoker)
            throws Exception {
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
                        changelogRow("+I", 2, "new_name1", 100.1),
                        changelogRow("+I", 3, "name3", 0.3),
                        changelogRow("+I", 4, "name4", 0.4),
                        changelogRow("+I", 8, "new_name7", null),
                        changelogRow("+I", 9, "name9", 0.9),
                        changelogRow("+I", 12, "new_name11", 101.1),
                        changelogRow("+I", 16, null, 101.1),
                        changelogRow("+I", 19, "new_name18", 101.8));

        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("T._ROW_ID=S.id")
                            .withMatchedUpdateSet("T.name=S.name,T.value=S.`value`")
                            .withSourceTable("S")
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T._ROW_ID=S.id', 'name=S.name,value=S.`value`', 2)",
                            targetDb);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name, `value` FROM T$row_tracking where _ROW_ID in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @ParameterizedTest(name = "use default db = {0}, invoker - {1}")
    @MethodSource("testArguments")
    public void testSelfMerge(boolean inDefault, String invoker) throws Exception {
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
                        changelogRow("+I", 2, "name2_test_udf"),
                        changelogRow("+I", 3, "name3_test_udf"),
                        changelogRow("+I", 4, "name4_test_udf"),
                        changelogRow("+I", 8, "name8_test_udf"),
                        changelogRow("+I", 9, "name9_test_udf"),
                        changelogRow("+I", 12, "name12_test_udf"),
                        changelogRow("+I", 16, "name16_test_udf"),
                        changelogRow("+I", 19, "name19_test_udf"));

        String udfName =
                "org.apache.paimon.flink.action.DataEvolutionMergeIntoActionITCase$StringConcatUdf";
        String createFuncSql = "CREATE TEMPORARY FUNCTION concat_string AS";
        String createViewSql =
                String.format(
                        "CREATE TEMPORARY VIEW SS AS SELECT _ROW_ID, concat_string(name) AS name FROM `%s`.`T$row_tracking`",
                        targetDb);
        if (invoker.equals("action")) {
            DataEvolutionMergeIntoActionBuilder builder =
                    builder(warehouse, targetDb, "T")
                            .withMergeCondition("TempT._ROW_ID=SS._ROW_ID")
                            .withMatchedUpdateSet("TempT.name=SS.name")
                            .withSourceTable("SS")
                            .withTargetAlias("TempT")
                            .withSourceSqls(
                                    String.format("%s '%s'", createFuncSql, udfName), createViewSql)
                            .withSinkParallelism(2);

            builder.build().run();
        } else {
            String procedureStatement =
                    String.format(
                            "CALL sys.data_evolution_merge_into('%s.T', 'TempT', '%s',"
                                    + " 'SS', 'TempT._ROW_ID=SS._ROW_ID', 'name=SS.name', 2)",
                            targetDb,
                            String.format("%s ''%s''", createFuncSql, udfName)
                                    + ";"
                                    + createViewSql);

            executeSQL(procedureStatement, false, true);
        }

        testBatchRead(
                "SELECT id, name FROM T$row_tracking where _ROW_ID in (1, 2, 3, 7, 8, 11, 15, 18)",
                expected);
    }

    @Test
    public void testUpdateAction() throws Exception {

        // create index on 01-22 partition
        executeSQL(
                "CALL sys.create_global_index(`table` => 'default.T', index_column => 'id', index_type => 'btree')",
                false,
                true);

        assertTrue(indexFileExists("T"));

        // 1. update indexed columns should throw an error by default
        assertThatThrownBy(
                        () ->
                                executeSQL(
                                        String.format(
                                                "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T._ROW_ID=S.id', 'name=S.name,id=1', 2)",
                                                database),
                                        false,
                                        true))
                .rootCause()
                .hasMessageContaining(
                        "MergeInto: update columns contain globally indexed columns, not supported now.");

        insertInto(
                "T",
                "(31, 'name31', 3.1, '01-23')",
                "(32, 'name32', 3.2, '01-23')",
                "(33, 'name33', 3.3, '01-23')",
                "(34, 'name34', 3.4, '01-23')",
                "(35, 'name35', 3.5, '01-23')",
                "(36, 'name36', 3.6, '01-23')",
                "(37, 'name37', 3.7, '01-23')",
                "(38, 'name38', 3.8, '01-23')",
                "(39, 'name39', 3.9, '01-23')",
                "(40, 'name30', 3.0, '01-23')");

        insertInto("S", "(35, 'new_name25', 125.1)");

        // 2. updating unindexed partitions is not affected
        assertDoesNotThrow(
                () ->
                        executeSQL(
                                String.format(
                                        "CALL sys.data_evolution_merge_into('%s.T', 'TempT', "
                                                + "'CREATE TEMPORARY VIEW SS AS SELECT id, name, `value` FROM S WHERE id > 20',"
                                                + " 'SS', 'TempT.id=SS.id', 'id=SS.id,value=SS.`value`', 2)",
                                        database),
                                false,
                                true));

        // 3. alter table's UpdateAction option to DROP_INDEX
        executeSQL(
                "ALTER TABLE T SET ('global-index.column-update-action' = 'DROP_PARTITION_INDEX')",
                false,
                true);

        assertDoesNotThrow(
                () ->
                        executeSQL(
                                String.format(
                                        "CALL sys.data_evolution_merge_into('%s.T', '', '', 'S', 'T._ROW_ID=S.id', 'name=S.name,id=1', 2)",
                                        database),
                                false,
                                true));

        assertFalse(indexFileExists("T"));
    }

    private boolean indexFileExists(String tableName) throws Exception {
        FileStoreTable table = getFileStoreTable(tableName);

        List<IndexManifestEntry> entries = table.store().newIndexFileHandler().scan("btree");

        return !entries.isEmpty();
    }

    @Test
    public void mergeConditionParserTest() throws Exception {
        // 1. test rewrite table names
        // basic rewrite
        String mergeCondition = "T.id = S.id";
        DataEvolutionMergeIntoAction.MergeConditionParser parser = createParser(mergeCondition);
        assertEquals("`RT`.`id` = `S`.`id`", parser.rewriteSqlNode("T", "RT").toString());

        // should recognize quotes
        mergeCondition = "T.id = s.id AND T.pt = s.pt";
        parser = createParser(mergeCondition);
        assertEquals(
                "`RT`.`id` = `s`.`id` AND `RT`.`pt` = `s`.`pt`",
                parser.rewriteSqlNode("T", "RT").toString());

        // should not rewrite column names
        mergeCondition = "`T`.id = `T1`.`T` and T.`T.T` = S.a";
        parser = createParser(mergeCondition);
        assertEquals(
                "`RT`.`id` = `T1`.`T` AND `RT`.`T.T` = `S`.`a`",
                parser.rewriteSqlNode("T", "RT").toString());

        // should not rewrite literals
        mergeCondition = "T.id = S.id AND S.str_col = 'T.id' AND T.`value` = 1";
        parser = createParser(mergeCondition);
        assertEquals(
                "`RT`.`id` = `S`.`id` AND `S`.`str_col` = 'T.id' AND `RT`.`value` = 1",
                parser.rewriteSqlNode("T", "RT").toString());

        // 2. test extract row id condition
        Optional<String> rowIdColumn;

        mergeCondition = "T._ROW_ID = S.id";
        parser = createParser(mergeCondition);
        rowIdColumn = parser.extractRowIdFieldFromSource("T");
        assertTrue(rowIdColumn.isPresent());
        assertEquals("id", rowIdColumn.get());

        mergeCondition = "S.id = T._ROW_ID";
        parser = createParser(mergeCondition);
        rowIdColumn = parser.extractRowIdFieldFromSource("T");
        assertTrue(rowIdColumn.isPresent());
        assertEquals("id", rowIdColumn.get());

        // target table not matches
        mergeCondition = "S.id = T._ROW_ID";
        parser = createParser(mergeCondition);
        rowIdColumn = parser.extractRowIdFieldFromSource("S");
        assertFalse(rowIdColumn.isPresent());

        // for simplicity, compounded condition is not considered now.
        mergeCondition = "S.id = T._ROW_ID AND T.id = 1";
        parser = createParser(mergeCondition);
        rowIdColumn = parser.extractRowIdFieldFromSource("T");
        assertFalse(rowIdColumn.isPresent());
    }

    private DataEvolutionMergeIntoAction.MergeConditionParser createParser(String mergeCondition)
            throws Exception {
        return new DataEvolutionMergeIntoAction.MergeConditionParser(mergeCondition);
    }

    @Test
    public void testUpdateRawBlobColumnThrowsError() throws Exception {
        // Create a table with raw-data BLOB column
        sEnv.executeSql(
                buildDdl(
                        "RAW_BLOB_T",
                        Arrays.asList("id INT", "name STRING", "picture BYTES"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put("blob-field", "picture");
                            }
                        }));
        insertInto("RAW_BLOB_T", "(1, 'name1', X'48656C6C6F')");

        // Create source table
        sEnv.executeSql(
                buildDdl(
                        "RAW_BLOB_S",
                        Arrays.asList("id INT", "picture BYTES"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put("blob-field", "picture");
                            }
                        }));
        insertInto("RAW_BLOB_S", "(1, X'4E4557')");

        DataEvolutionMergeIntoAction action =
                builder(warehouse, database, "RAW_BLOB_T")
                        .withMergeCondition("RAW_BLOB_T.id=RAW_BLOB_S.id")
                        .withMatchedUpdateSet("RAW_BLOB_T.picture=RAW_BLOB_S.picture")
                        .withSourceTable("RAW_BLOB_S")
                        .withSinkParallelism(1)
                        .build();

        Throwable t = Assertions.assertThrows(IllegalStateException.class, () -> action.run());
        Assertions.assertTrue(
                t.getMessage().contains("raw-data BLOB column"),
                "Expected error about raw-data BLOB column but got: " + t.getMessage());
    }

    @Test
    public void testUpdateNonBlobColumnOnDescriptorBlobTableSucceeds() throws Exception {
        // Create a table with descriptor BLOB column.
        // Previously, MERGE INTO would reject ANY table with BLOB columns.
        // With our change, tables with descriptor-based BLOB columns are accepted
        // as long as the BLOB column itself is descriptor-based.
        sEnv.executeSql(
                buildDdl(
                        "DESC_BLOB_T",
                        Arrays.asList("id INT", "name STRING", "picture BYTES"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put("blob-field", "picture");
                                put("blob-descriptor-field", "picture");
                            }
                        }));

        // Insert data with descriptor-based BLOB
        BlobDescriptor desc1 = new BlobDescriptor("file:///dummy/blob1", 0, 2);
        BlobDescriptor desc2 = new BlobDescriptor("file:///dummy/blob2", 0, 2);
        insertInto(
                "DESC_BLOB_T", String.format("(1, 'name1', X'%s')", bytesToHex(desc1.serialize())));
        insertInto(
                "DESC_BLOB_T", String.format("(2, 'name2', X'%s')", bytesToHex(desc2.serialize())));

        // Create source table for the update (only non-BLOB columns)
        sEnv.executeSql(
                buildDdl(
                        "DESC_BLOB_S",
                        Arrays.asList("id INT", "name STRING"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("DESC_BLOB_S", "(1, 'updated_name1')");

        // Update only the 'name' column via MERGE INTO.
        // This should succeed — the table has BLOB columns but they are descriptor-based.
        builder(warehouse, database, "DESC_BLOB_T")
                .withMergeCondition("DESC_BLOB_T.id=DESC_BLOB_S.id")
                .withMatchedUpdateSet("DESC_BLOB_T.name=DESC_BLOB_S.name")
                .withSourceTable("DESC_BLOB_S")
                .withSinkParallelism(1)
                .build()
                .run();

        // Verify: id=1 name updated, id=2 unchanged
        List<Row> expected =
                Arrays.asList(
                        changelogRow("+I", 1, "updated_name1"), changelogRow("+I", 2, "name2"));
        testBatchRead("SELECT id, name FROM DESC_BLOB_T ORDER BY id", expected);
    }

    @Test
    public void testUpdateExternalStorageBlobColumnSucceeds() throws Exception {
        // Create a temp path for descriptor blobs backed by external storage.
        String externalStoragePath = getTempDirPath("external-storage-path");

        // Create a table with a descriptor BLOB column backed by external storage.
        sEnv.executeSql(
                buildDdl(
                        "COPY_UPD_T",
                        Arrays.asList("id INT", "name STRING", "picture BYTES"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                                put("blob-field", "picture");
                                put("blob-descriptor-field", "picture");
                                put(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key(), "picture");
                                put(
                                        CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key(),
                                        externalStoragePath);
                            }
                        }));

        // Insert initial row with raw bytes; write path stores data in external storage and keeps
        // descriptor bytes.
        insertInto("COPY_UPD_T", "(1, 'name1', X'48656C6C6F')");

        // Create source table with new raw bytes for the BLOB column
        sEnv.executeSql(
                buildDdl(
                        "COPY_UPD_S",
                        Arrays.asList("id INT", "picture BYTES"),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<String, String>() {
                            {
                                put(ROW_TRACKING_ENABLED.key(), "true");
                                put(DATA_EVOLUTION_ENABLED.key(), "true");
                            }
                        }));
        insertInto("COPY_UPD_S", "(1, X'574F524C44')");

        // Update this descriptor BLOB column backed by external storage via MERGE INTO.
        builder(warehouse, database, "COPY_UPD_T")
                .withMergeCondition("COPY_UPD_T.id=COPY_UPD_S.id")
                .withMatchedUpdateSet("COPY_UPD_T.picture=COPY_UPD_S.picture")
                .withSourceTable("COPY_UPD_S")
                .withSinkParallelism(1)
                .build()
                .run();

        // Verify: name stays the same, BLOB column should have new data
        List<Row> expected = Arrays.asList(changelogRow("+I", 1, "name1"));
        testBatchRead("SELECT id, name FROM COPY_UPD_T ORDER BY id", expected);
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

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    /** The test udf to test udf in merge into situation. */
    public static class StringConcatUdf extends ScalarFunction {

        public String eval(String input) {
            return input + "_test_udf";
        }
    }
}
