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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.testutils.assertj.AssertionUtils.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link PostgreSqlSyncTableAction}. */
public class PostgreSqlSyncTableActionITCase extends PostgreSqlActionITCaseBase {

    @BeforeAll
    public static void init() {
        POSTGRE_SQL_CONTAINER.withInitScript("postgresql/sync_table_setup.sql");
        startContainers();
    }

    @Test
    @Timeout(120)
    public void testSyncTable() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "sync_table_schema");
        postgreSqlConfig.put("table-name", "t1|t2");
        postgreSqlConfig.put("decoding.plugin.name", "pgoutput");
        postgreSqlConfig.put("slot.name", "flink_replication_slot_01");

        PostgreSqlSyncTableAction action =
                syncTableActionBuilder(postgreSqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pt", "uid")
                        .build();

        runActionWithDefaultEnv(action);

        try (Statement statement = getStatement()) {
            insertData(statement);
            assertResult();
        }
    }

    private void insertData(Statement statement) throws SQLException {
        statement.executeUpdate("SET search_path TO sync_table_schema;");
        statement.executeUpdate("INSERT INTO t1 VALUES (1,1001,'bob','2023-11-16')");
        statement.executeUpdate("INSERT INTO t1 VALUES (2,1002,'toy','2023-11-17')");
        statement.executeUpdate("INSERT INTO t2 VALUES (1,1001,'beijing','2023-11-16')");
        statement.executeUpdate("INSERT INTO t2 VALUES (2,1002,'shanghai','2023-11-17')");
    }

    private void assertResult() throws Exception {
        FileStoreTable table = getFileStoreTable();

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(100),
                            DataTypes.VARCHAR(100),
                            DataTypes.VARCHAR(100).notNull()
                        },
                        new String[] {"id", "uid", "name", "address", "pt"});
        List<String> primaryKeys = Arrays.asList("pt", "uid");
        List<String> expected =
                Arrays.asList(
                        "+I[1, 1001, bob, beijing, 2023-11-16]",
                        "+I[2, 1002, toy, shanghai, 2023-11-17]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    public void testIncompatiblePostgreSqlTable() {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "sync_table_schema");
        postgreSqlConfig.put("table-name", "incompatible_field_\\d+");

        PostgreSqlSyncTableAction action = syncTableActionBuilder(postgreSqlConfig).build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Column v1 have different types when merging schemas.\n"
                                        + "Current table 'incompatible_field_1' field: `v1` TIMESTAMP(9)\n"
                                        + "To be merged table 'incompatible_field_2' field: `v1` INT"));
    }

    @Test
    public void testIncompatiblePaimonTable() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "sync_table_schema");
        postgreSqlConfig.put("table-name", "incompatible_pk_\\d+");

        createFileStoreTable(
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"a", "b", "c"}),
                Collections.emptyList(),
                Collections.singletonList("a"),
                new HashMap<>());

        PostgreSqlSyncTableAction action =
                syncTableActionBuilder(postgreSqlConfig).withPrimaryKeys("a").build();

        assertThatThrownBy(action::run)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Paimon schema and source table schema are not compatible.\n"));
    }

    @Test
    @Timeout(60)
    public void testOptionsChange() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "sync_table_schema");
        postgreSqlConfig.put("table-name", "test_options_change");

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("bucket", "1");
        tableConfig.put("sink.parallelism", "1");

        PostgreSqlSyncTableAction action =
                syncTableActionBuilder(postgreSqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        JobClient jobClient = runActionWithDefaultEnv(action);

        waitingTables(tableName);
        jobClient.cancel();

        tableConfig.put("sink.savepoint.auto-tag", "true");
        tableConfig.put("tag.num-retained-max", "5");
        tableConfig.put("tag.automatic-creation", "process-time");
        tableConfig.put("tag.creation-period", "hourly");
        tableConfig.put("tag.creation-delay", "600000");
        tableConfig.put("snapshot.time-retained", "1h");
        tableConfig.put("snapshot.num-retained.min", "5");
        tableConfig.put("snapshot.num-retained.max", "10");
        tableConfig.put("changelog-producer", "input");

        PostgreSqlSyncTableAction action1 =
                syncTableActionBuilder(postgreSqlConfig)
                        .withPartitionKeys("pt")
                        .withPrimaryKeys("pk", "pt")
                        .withComputedColumnArgs("pt=substring(_date,5)")
                        .withTableConfig(tableConfig)
                        .build();
        runActionWithDefaultEnv(action1);

        Map<String, String> dynamicOptions = action1.fileStoreTable().options();
        assertThat(dynamicOptions).containsAllEntriesOf(tableConfig);
    }

    @Test
    @Timeout(120)
    public void testComputedColumn() throws Exception {
        // the first round checks for table creation
        // the second round checks for running the action on an existing table
        for (int i = 0; i < 2; i++) {
            innerTestComputedColumn(i == 0);
        }
    }

    private void innerTestComputedColumn(boolean executePostgres) throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "sync_table_schema");
        postgreSqlConfig.put("table-name", "test_computed_column");
        postgreSqlConfig.put("decoding.plugin.name", "pgoutput");
        postgreSqlConfig.put("slot.name", "flink_replication_slot_02");

        List<String> computedColumnDefs =
                Arrays.asList(
                        "_year_date=year(_date)",
                        "_year_datetime=year(_datetime)",
                        "_year_timestamp=year(_timestamp)",
                        "_month_date=month(_date)",
                        "_month_datetime=month(_datetime)",
                        "_month_timestamp=month(_timestamp)",
                        "_day_date=day(_date)",
                        "_day_datetime=day(_datetime)",
                        "_day_timestamp=day(_timestamp)",
                        "_hour_date=hour(_date)",
                        "_hour_datetime=hour(_datetime)",
                        "_hour_timestamp=hour(_timestamp)",
                        "_date_format_date=date_format(_date,yyyy)",
                        "_date_format_datetime=date_format(_datetime,yyyy-MM-dd)",
                        "_date_format_timestamp=date_format(_timestamp,yyyyMMdd)",
                        "_substring_date1=substring(_date,2)",
                        "_substring_date2=substring(_timestamp,5,10)",
                        "_truncate_date=trUNcate(pk,2)");

        PostgreSqlSyncTableAction action =
                syncTableActionBuilder(postgreSqlConfig)
                        .withTableConfig(getBasicTableConfig())
                        .withPartitionKeys("_year_date")
                        .withPrimaryKeys("pk", "_year_date")
                        .withComputedColumnArgs(computedColumnDefs)
                        .build();
        runActionWithDefaultEnv(action);

        if (executePostgres) {
            try (Statement statement = getStatement()) {
                statement.executeUpdate("SET search_path TO sync_table_schema;");
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (1, '2023-03-23', '2022-01-01 14:30', '2021-09-15 15:00:10')");
                statement.executeUpdate(
                        "INSERT INTO test_computed_column VALUES (2, '2023-03-23', null, null)");
            }
        }

        FileStoreTable table = getFileStoreTable();
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.DATE(),
                            DataTypes.TIMESTAMP(0),
                            DataTypes.TIMESTAMP(0),
                            DataTypes.INT().notNull(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.STRING(),
                            DataTypes.INT().notNull()
                        },
                        new String[] {
                            "pk",
                            "_date",
                            "_datetime",
                            "_timestamp",
                            "_year_date",
                            "_year_datetime",
                            "_year_timestamp",
                            "_month_date",
                            "_month_datetime",
                            "_month_timestamp",
                            "_day_date",
                            "_day_datetime",
                            "_day_timestamp",
                            "_hour_date",
                            "_hour_datetime",
                            "_hour_timestamp",
                            "_date_format_date",
                            "_date_format_datetime",
                            "_date_format_timestamp",
                            "_substring_date1",
                            "_substring_date2",
                            "_truncate_date"
                        });
        List<String> expected =
                Arrays.asList(
                        "+I[1, 19439, 2022-01-01T14:30, 2021-09-15T15:00:10, 2023, 2022, 2021, 3, 1, 9, 23, 1, 15, 0, 14, 15, 2023, 2022-01-01, 20210915, 23-03-23, 09-15, 0]",
                        "+I[2, 19439, NULL, NULL, 2023, NULL, NULL, 3, NULL, NULL, 23, NULL, NULL, 0, NULL, NULL, 2023, NULL, NULL, 23-03-23, NULL, 2]");
        waitForResult(expected, table, rowType, Arrays.asList("pk", "_year_date"));
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        return getFileStoreTable(tableName);
    }
}
