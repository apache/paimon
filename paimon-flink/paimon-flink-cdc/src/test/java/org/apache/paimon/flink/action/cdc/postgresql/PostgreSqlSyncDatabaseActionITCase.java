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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link PostgreSqlSyncDatabaseAction}. */
public class PostgreSqlSyncDatabaseActionITCase extends PostgreSqlActionITCaseBase {

    @BeforeAll
    public static void init() {
        POSTGRE_SQL_CONTAINER.withInitScript("postgresql/sync_database_setup.sql");
        startContainers();
    }

    @Test
    @Timeout(60)
    public void testSyncDatabase() throws Exception {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "test_schema");
        postgreSqlConfig.put("decoding.plugin.name", "pgoutput");
        postgreSqlConfig.put("slot.name", "flink_replication_slot");

        PostgreSqlSyncDatabaseAction action = syncDatabaseActionBuilder(postgreSqlConfig).build();

        runActionWithDefaultEnv(action);

        assertExactlyExistTables("test_table_01", "test_table_02");

        try (Statement statement = getStatement()) {
            insertData(statement);
            assertResult();
        }
    }

    private void insertData(Statement statement) throws SQLException {
        statement.executeUpdate("SET search_path TO test_schema;");
        statement.executeUpdate("INSERT INTO test_table_01 VALUES (1, 'a1')");
        statement.executeUpdate("INSERT INTO test_table_02 VALUES (2, 'a2')");
        statement.executeUpdate("INSERT INTO test_table_01 VALUES (3, 'a3')");
        statement.executeUpdate("INSERT INTO test_table_02 VALUES (4, 'a4')");
    }

    private void assertResult() throws Exception {
        FileStoreTable t1 = getFileStoreTable("test_table_01");
        FileStoreTable t2 = getFileStoreTable("test_table_02");

        RowType rowType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k1", "v1"});
        List<String> primaryKeys1 = Collections.singletonList("k1");
        List<String> expected = Arrays.asList("+I[1, a1]", "+I[3, a3]");
        waitForResult(expected, t1, rowType1, primaryKeys1);

        RowType rowType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"k2", "v2"});
        List<String> primaryKeys2 = Collections.singletonList("k2");
        expected = Arrays.asList("+I[2, a2]", "+I[4, a4]");
        waitForResult(expected, t2, rowType2, primaryKeys2);
    }

    @Test
    public void testSpecifiedTable() {
        Map<String, String> postgreSqlConfig = getBasicPostgreSqlConfig();
        postgreSqlConfig.put("database-name", "test_db");
        postgreSqlConfig.put("schema-name", "test_schema");
        postgreSqlConfig.put("slot.name", "flink_replication_slot");
        postgreSqlConfig.put("table-name", "test_table_01");

        PostgreSqlSyncDatabaseAction action = syncDatabaseActionBuilder(postgreSqlConfig).build();

        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "table-name cannot be set for postgresql-sync-database. "
                                + "If you want to sync several PostgreSQL tables into one Paimon table, "
                                + "use postgresql-sync-table instead.");
    }
}
