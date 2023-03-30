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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.ActionITCaseBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link MySqlCtasAction}. */
public class MySqlCtasActionITCase extends ActionITCaseBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlCtasActionITCase.class);

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);
    private static final String USER = "paimonuser";
    private static final String PASSWORD = "paimonpw";
    private static final String DATABASE_NAME = "paimon_test";

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        MYSQL_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return (MySqlContainer)
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/my.cnf")
                        .withSetupSQL("mysql/setup.sql")
                        .withUsername(USER)
                        .withPassword(PASSWORD)
                        .withDatabaseName(DATABASE_NAME)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
    }

    @Test
    @Timeout(60)
    public void testSchemaEvolution() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "schema_evolution_\\d+");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        MySqlCtasAction action =
                new MySqlCtasAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("_id"),
                        new HashMap<>(),
                        ThreadLocalRandom.current().nextInt(3) + 1);
        action.build(env);
        JobClient client = env.executeAsync();

        while (true) {
            JobStatus status = client.getJobStatus().get();
            if (status == JobStatus.RUNNING) {
                break;
            }
            Thread.sleep(1000);
        }

        try (Connection conn =
                DriverManager.getConnection(
                        String.format(
                                "jdbc:mysql://%s:%s/",
                                MYSQL_CONTAINER.getHost(), MYSQL_CONTAINER.getDatabasePort()),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = conn.createStatement()) {
                testSchemaEvolutionImpl(statement);
            }
        }
    }

    private void testSchemaEvolutionImpl(Statement statement) throws Exception {
        FileStoreTable table = getFileStoreTable();
        statement.executeUpdate("USE paimon_test");

        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (1, 'one')");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (2, 'two'), (4, 'four')");
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT().notNull(), DataTypes.VARCHAR(10)},
                        new String[] {"_id", "v1"});
        List<String> primaryKeys = Collections.singletonList("_id");
        List<String> expected = Arrays.asList("+I[1, one]", "+I[2, two]", "+I[4, four]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v2 INT");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (3, 'three', 30), (5, 'five', 50)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v2 INT");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (6, 'six', 60)");
        statement.executeUpdate("UPDATE schema_evolution_2 SET v1 = 'second' WHERE _id = 2");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.INT()
                        },
                        new String[] {"_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[2, second, NULL]",
                        "+I[3, three, 30]",
                        "+I[4, four, NULL]",
                        "+I[5, five, 50]",
                        "+I[6, six, 60]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate("INSERT INTO schema_evolution_1 VALUES (7, 'seven', 70000000000)");
        statement.executeUpdate("UPDATE schema_evolution_1 SET v2 = 30000000000 WHERE _id = 3");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v2 BIGINT");
        statement.executeUpdate("INSERT INTO schema_evolution_2 VALUES (8, 'eight', 80000000000)");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), DataTypes.VARCHAR(10), DataTypes.BIGINT()
                        },
                        new String[] {"_id", "v1", "v2"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL]",
                        "+I[2, second, NULL]",
                        "+I[3, three, 30000000000]",
                        "+I[4, four, NULL]",
                        "+I[5, five, 50]",
                        "+I[6, six, 60]",
                        "+I[7, seven, 70000000000]",
                        "+I[8, eight, 80000000000]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v4 VARBINARY(10)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v1 VARCHAR(20)");
        statement.executeUpdate(
                "INSERT INTO schema_evolution_1 VALUES (9, 'nine', 90000000000, 99999.999, 'nine.bin', 9.9)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v3 NUMERIC(8, 3)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v4 VARBINARY(10)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 ADD COLUMN v5 FLOAT");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v1 VARCHAR(20)");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v1 = 'very long string' WHERE _id = 8");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(10),
                            DataTypes.FLOAT()
                        },
                        new String[] {"_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL, NULL, NULL, NULL]",
                        "+I[2, second, NULL, NULL, NULL, NULL]",
                        "+I[3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[4, four, NULL, NULL, NULL, NULL]",
                        "+I[5, five, 50, NULL, NULL, NULL]",
                        "+I[6, six, 60, NULL, NULL, NULL]",
                        "+I[7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[9, nine, 90000000000, 99999.999, nine.bin, 9.9]");
        waitForResult(expected, table, rowType, primaryKeys);

        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v4 VARBINARY(20)");
        statement.executeUpdate("ALTER TABLE schema_evolution_1 MODIFY COLUMN v5 DOUBLE");
        statement.executeUpdate(
                "UPDATE schema_evolution_1 SET v4 = 'nine.bin.long', v5 = 9.00000000009 WHERE _id = 9");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v4 VARBINARY(20)");
        statement.executeUpdate("ALTER TABLE schema_evolution_2 MODIFY COLUMN v5 DOUBLE");
        statement.executeUpdate(
                "UPDATE schema_evolution_2 SET v4 = 'four.bin.long', v5 = 4.00000000004 WHERE _id = 4");
        rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(20),
                            DataTypes.BIGINT(),
                            DataTypes.DECIMAL(8, 3),
                            DataTypes.VARBINARY(20),
                            DataTypes.DOUBLE()
                        },
                        new String[] {"_id", "v1", "v2", "v3", "v4", "v5"});
        expected =
                Arrays.asList(
                        "+I[1, one, NULL, NULL, NULL, NULL]",
                        "+I[2, second, NULL, NULL, NULL, NULL]",
                        "+I[3, three, 30000000000, NULL, NULL, NULL]",
                        "+I[4, four, NULL, NULL, four.bin.long, 4.00000000004]",
                        "+I[5, five, 50, NULL, NULL, NULL]",
                        "+I[6, six, 60, NULL, NULL, NULL]",
                        "+I[7, seven, 70000000000, NULL, NULL, NULL]",
                        "+I[8, very long string, 80000000000, NULL, NULL, NULL]",
                        "+I[9, nine, 90000000000, 99999.999, nine.bin.long, 9.00000000009]");
        waitForResult(expected, table, rowType, primaryKeys);
    }

    @Test
    @Timeout(30)
    public void testAllTypes() throws Exception {
        Map<String, String> mySqlConfig = getBasicMySqlConfig();
        mySqlConfig.put("database-name", DATABASE_NAME);
        mySqlConfig.put("table-name", "all_types_table");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        MySqlCtasAction action =
                new MySqlCtasAction(
                        mySqlConfig,
                        warehouse,
                        database,
                        tableName,
                        Collections.emptyList(),
                        Collections.singletonList("_id"),
                        new HashMap<>(),
                        1);
        action.build(env);
        env.executeAsync();

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(), // _id
                            DataTypes.BOOLEAN(), // _boolean
                            DataTypes.TINYINT(), // _tinyint
                            DataTypes.SMALLINT(), // _tinyint_unsigned
                            DataTypes.SMALLINT(), // _tinyint_unsigned_zerofill
                            DataTypes.SMALLINT(), // _smallint
                            DataTypes.INT(), // _smallint_unsigned
                            DataTypes.INT(), // _smallint_unsigned_zerofill
                            DataTypes.INT(), // _mediumint
                            DataTypes.BIGINT(), // _mediumint_unsigned
                            DataTypes.BIGINT(), // _mediumint_unsigned_zerofill
                            DataTypes.INT(), // _int
                            DataTypes.BIGINT(), // _int_unsigned
                            DataTypes.BIGINT(), // _int_unsigned_zerofill
                            DataTypes.BIGINT(), // _bigint
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned
                            DataTypes.DECIMAL(20, 0), // _bigint_unsigned_zerofill
                            DataTypes.DECIMAL(20, 0), // _serial
                            DataTypes.FLOAT(), // _float
                            DataTypes.FLOAT(), // _float_unsigned
                            DataTypes.FLOAT(), // _float_unsigned_zerofill
                            DataTypes.DOUBLE(), // _real
                            DataTypes.DOUBLE(), // _real_unsigned
                            DataTypes.DOUBLE(), // _real_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double
                            DataTypes.DOUBLE(), // _double_unsigned
                            DataTypes.DOUBLE(), // _double_unsigned_zerofill
                            DataTypes.DOUBLE(), // _double_precision
                            DataTypes.DOUBLE(), // _double_precision_unsigned
                            DataTypes.DOUBLE(), // _double_precision_unsigned_zerofill
                            DataTypes.DECIMAL(8, 3), // _numeric
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned
                            DataTypes.DECIMAL(8, 3), // _numeric_unsigned_zerofill
                            DataTypes.STRING(), // _fixed
                            DataTypes.STRING(), // _fixed_unsigned
                            DataTypes.STRING(), // _fixed_unsigned_zerofill
                            DataTypes.DECIMAL(8, 0), // _decimal
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned
                            DataTypes.DECIMAL(8, 0), // _decimal_unsigned_zerofill
                            DataTypes.DATE(), // _date
                            DataTypes.TIMESTAMP(0), // _datetime
                            DataTypes.TIMESTAMP(6), // _timestamp
                            DataTypes.CHAR(10), // _char
                            DataTypes.VARCHAR(20), // _varchar
                            DataTypes.STRING(), // _text
                            DataTypes.BINARY(10), // _bin
                            DataTypes.VARBINARY(20), // _varbin
                            DataTypes.BYTES() // _blob
                        },
                        new String[] {
                            "_id",
                            "_boolean",
                            "_tinyint",
                            "_tinyint_unsigned",
                            "_tinyint_unsigned_zerofill",
                            "_smallint",
                            "_smallint_unsigned",
                            "_smallint_unsigned_zerofill",
                            "_mediumint",
                            "_mediumint_unsigned",
                            "_mediumint_unsigned_zerofill",
                            "_int",
                            "_int_unsigned",
                            "_int_unsigned_zerofill",
                            "_bigint",
                            "_bigint_unsigned",
                            "_bigint_unsigned_zerofill",
                            "_serial",
                            "_float",
                            "_float_unsigned",
                            "_float_unsigned_zerofill",
                            "_real",
                            "_real_unsigned",
                            "_real_unsigned_zerofill",
                            "_double",
                            "_double_unsigned",
                            "_double_unsigned_zerofill",
                            "_double_precision",
                            "_double_precision_unsigned",
                            "_double_precision_unsigned_zerofill",
                            "_numeric",
                            "_numeric_unsigned",
                            "_numeric_unsigned_zerofill",
                            "_fixed",
                            "_fixed_unsigned",
                            "_fixed_unsigned_zerofill",
                            "_decimal",
                            "_decimal_unsigned",
                            "_decimal_unsigned_zerofill",
                            "_date",
                            "_datetime",
                            "_timestamp",
                            "_char",
                            "_varchar",
                            "_text",
                            "_bin",
                            "_varbin",
                            "_blob"
                        });
        FileStoreTable table = getFileStoreTable();
        List<String> expected =
                Arrays.asList(
                        "+I["
                                + "1, true, 1, 2, 3, "
                                + "1000, 2000, 3000, "
                                + "100000, 200000, 300000, "
                                + "1000000, 2000000, 3000000, "
                                + "10000000000, 20000000000, 30000000000, 40000000000, "
                                + "1.5, 2.5, 3.5, "
                                + "1.000001, 2.000002, 3.000003, "
                                + "1.000011, 2.000022, 3.000033, "
                                + "1.000111, 2.000222, 3.000333, "
                                + "12345.110, 12345.220, 12345.330, "
                                + "1.2345678987654322E32, 1.2345678987654322E32, 1.2345678987654322E32, "
                                + "11111, 22222, 33333, "
                                + "19439, 2023-03-23T14:30:05, 2023-03-23T15:00:10.123456, "
                                + "Paimon, Apache Paimon, Apache Paimon MySQL Test Data, "
                                + "bytes\u0000\u0000\u0000\u0000\u0000, more bytes, very long bytes test data"
                                + "]",
                        "+I["
                                + "2, NULL, NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, 50000000000, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL, "
                                + "NULL, NULL, NULL"
                                + "]");
        waitForResult(expected, table, rowType, Collections.singletonList("_id"));
    }

    private void waitForResult(
            List<String> expected, FileStoreTable table, RowType rowType, List<String> primaryKeys)
            throws Exception {
        assertThat(table.schema().primaryKeys()).isEqualTo(primaryKeys);

        // wait for table schema to become our expected schema
        while (true) {
            int cnt = 0;
            for (int i = 0; i < table.schema().fields().size(); i++) {
                DataField field = table.schema().fields().get(i);
                boolean sameName = field.name().equals(rowType.getFieldNames().get(i));
                boolean sameType = field.type().equals(rowType.getFieldTypes().get(i));
                if (sameName && sameType) {
                    cnt++;
                }
            }
            if (cnt == rowType.getFieldCount()) {
                break;
            }
            table = table.copyWithLatestSchema();
            Thread.sleep(1000);
        }

        // wait for data to become expected
        List<String> sortedExpected = new ArrayList<>(expected);
        Collections.sort(sortedExpected);
        while (true) {
            TableScan.Plan plan = table.newScan().plan();
            List<String> result =
                    getResult(
                            table.newRead(),
                            plan == null ? Collections.emptyList() : plan.splits(),
                            rowType);
            List<String> sortedActual = new ArrayList<>(result);
            Collections.sort(sortedActual);
            if (sortedExpected.equals(sortedActual)) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    private Map<String, String> getBasicMySqlConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("hostname", MYSQL_CONTAINER.getHost());
        config.put("port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        config.put("username", USER);
        config.put("password", PASSWORD);
        config.put("server-time-zone", ZoneId.of("+00:00").toString());
        return config;
    }

    private FileStoreTable getFileStoreTable() throws Exception {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
        Identifier identifier = Identifier.create(database, tableName);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
