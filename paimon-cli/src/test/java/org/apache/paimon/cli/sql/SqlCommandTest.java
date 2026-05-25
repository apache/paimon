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

package org.apache.paimon.cli.sql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for SQL command execution end-to-end. */
class SqlCommandTest {

    @TempDir Path tempDir;
    private Options options;

    @BeforeEach
    void setUp() throws Exception {
        options = new Options();
        options.set("metastore", "filesystem");
        options.set("warehouse", tempDir.toString());

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("mydb", true);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("city", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("mydb", "users"), schema, true);

        Table table = catalog.getTable(Identifier.create("mydb", "users"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("Alice"),
                        25,
                        BinaryString.fromString("Beijing")));
        writer.write(
                GenericRow.of(
                        2,
                        BinaryString.fromString("Bob"),
                        30,
                        BinaryString.fromString("Shanghai")));
        writer.write(
                GenericRow.of(
                        3,
                        BinaryString.fromString("Charlie"),
                        35,
                        BinaryString.fromString("Beijing")));
        writer.write(
                GenericRow.of(
                        4,
                        BinaryString.fromString("David"),
                        28,
                        BinaryString.fromString("Shanghai")));
        writer.write(
                GenericRow.of(
                        5, BinaryString.fromString("Eve"), 22, BinaryString.fromString("Beijing")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);

        // Create a second table for JOIN tests
        Schema orderSchema =
                Schema.newBuilder()
                        .column("order_id", DataTypes.INT())
                        .column("user_id", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .primaryKey("order_id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("mydb", "orders"), orderSchema, true);

        Table orderTable = catalog.getTable(Identifier.create("mydb", "orders"));
        BatchWriteBuilder owb = orderTable.newBatchWriteBuilder();
        BatchTableWrite ow = owb.newWrite();
        ow.write(GenericRow.of(101, 1, 500));
        ow.write(GenericRow.of(102, 2, 300));
        ow.write(GenericRow.of(103, 1, 200));
        ow.write(GenericRow.of(104, 3, 700));
        List<CommitMessage> orderMessages = ow.prepareCommit();
        ow.close();
        owb.newCommit().commit(orderMessages);

        catalog.close();
    }

    @Test
    void testSelectAll() throws Exception {
        String output = executeSql("SELECT * FROM mydb.users");
        assertThat(output).contains("Alice");
        assertThat(output).contains("Bob");
        assertThat(output).contains("Charlie");
        assertThat(output).contains("David");
        assertThat(output).contains("Eve");
    }

    @Test
    void testSelectWithWhere() throws Exception {
        String output = executeSql("SELECT * FROM mydb.users WHERE id = 1");
        assertThat(output).contains("Alice");
    }

    @Test
    void testSelectWithLimit() throws Exception {
        String output = executeSql("SELECT * FROM mydb.users LIMIT 2");
        // Only 2 data rows (plus header)
        String[] lines = output.trim().split("\n");
        assertThat(lines).hasSize(3);
    }

    @Test
    void testSelectColumns() throws Exception {
        String output = executeSql("SELECT id, name FROM mydb.users LIMIT 1");
        String headerLine = output.split("\n")[0];
        assertThat(headerLine).contains("id");
        assertThat(headerLine).contains("name");
        assertThat(headerLine).doesNotContain("age");
        assertThat(headerLine).doesNotContain("city");
    }

    @Test
    void testGroupByCount() throws Exception {
        String output = executeSql("SELECT city, COUNT(*) FROM mydb.users GROUP BY city");
        assertThat(output).contains("Beijing");
        assertThat(output).contains("Shanghai");
        assertThat(output).contains("3");
        assertThat(output).contains("2");
    }

    @Test
    void testGroupByAvg() throws Exception {
        String output = executeSql("SELECT city, AVG(age) FROM mydb.users GROUP BY city");
        assertThat(output).contains("Beijing");
        assertThat(output).contains("Shanghai");
    }

    @Test
    void testDistinct() throws Exception {
        String output = executeSql("SELECT DISTINCT city FROM mydb.users");
        assertThat(output).contains("Beijing");
        assertThat(output).contains("Shanghai");
        // Only 2 distinct cities + header
        String[] lines = output.trim().split("\n");
        assertThat(lines).hasSize(3);
    }

    @Test
    void testOrderByLimit() throws Exception {
        String output = executeSql("SELECT * FROM mydb.users ORDER BY age DESC LIMIT 2");
        String[] lines = output.trim().split("\n");
        // header + 2 data rows
        assertThat(lines).hasSize(3);
    }

    @Test
    void testJoin() throws Exception {
        String output =
                executeSql(
                        "SELECT u.name, o.amount FROM mydb.users u"
                                + " JOIN mydb.orders o ON u.id = o.user_id"
                                + " ORDER BY o.amount DESC LIMIT 10");
        assertThat(output).contains("Alice");
        assertThat(output).contains("Charlie");
        assertThat(output).contains("700");
    }

    @Test
    void testSubquery() throws Exception {
        String output =
                executeSql(
                        "SELECT name FROM mydb.users"
                                + " WHERE id IN (SELECT user_id FROM mydb.orders WHERE amount > 400)");
        assertThat(output).contains("Alice");
        assertThat(output).contains("Charlie");
    }

    @Test
    void testCaseWhen() throws Exception {
        String output =
                executeSql(
                        "SELECT name, CASE WHEN age >= 30 THEN 'senior' ELSE 'junior' END"
                                + " FROM mydb.users LIMIT 5");
        assertThat(output).contains("senior");
        assertThat(output).contains("junior");
    }

    @Test
    void testUnion() throws Exception {
        String output =
                executeSql(
                        "SELECT name FROM mydb.users WHERE city = 'Beijing'"
                                + " UNION SELECT name FROM mydb.users WHERE age > 29");
        assertThat(output).contains("Alice");
        assertThat(output).contains("Charlie");
        assertThat(output).contains("Bob");
    }

    @Test
    void testHaving() throws Exception {
        String output =
                executeSql(
                        "SELECT city, COUNT(*) FROM mydb.users"
                                + " GROUP BY city HAVING COUNT(*) >= 3");
        assertThat(output).contains("Beijing");
        assertThat(output).doesNotContain("Shanghai");
    }

    @Test
    void testArithmeticExpression() throws Exception {
        String output = executeSql("SELECT name, age * 2 FROM mydb.users WHERE id = 1");
        assertThat(output).contains("Alice");
        assertThat(output).contains("50");
    }

    @Test
    void testWindowFunction() throws Exception {
        String output =
                executeSql(
                        "SELECT name, age, ROW_NUMBER() OVER (ORDER BY age DESC) as rn"
                                + " FROM mydb.users LIMIT 5");
        assertThat(output).contains("rn");
        assertThat(output).contains("1");
        assertThat(output).contains("Charlie");
    }

    @Test
    void testShowDatabases() throws Exception {
        String output = executeSql("SHOW DATABASES");
        assertThat(output).contains("mydb");
    }

    @Test
    void testShowTables() throws Exception {
        String output = executeSql("SHOW TABLES IN mydb");
        assertThat(output).contains("users");
    }

    private String executeSql(String sql) throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            SqlCommand cmd = new SqlCommand();
            cmd.execute(ctx, new String[] {sql});
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
