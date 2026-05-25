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

package org.apache.paimon.cli.commands;

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

class ReadCommandTest {

    @TempDir Path tempDir;
    private Options options;

    @BeforeEach
    void setUp() throws Exception {
        options = new Options();
        options.set("metastore", "filesystem");
        options.set("warehouse", tempDir.toString());

        // Create test database and table
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);

        catalog.createDatabase("testdb", true);

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "users"), schema, true);

        // Insert test data
        Table table = catalog.getTable(Identifier.create("testdb", "users"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();

        writer.write(GenericRow.of(1, BinaryString.fromString("Alice"), 25));
        writer.write(GenericRow.of(2, BinaryString.fromString("Bob"), 30));
        writer.write(GenericRow.of(3, BinaryString.fromString("Charlie"), 35));

        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();
    }

    @Test
    void testReadAll() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            ReadCommand cmd = new ReadCommand();
            cmd.execute(ctx, new String[] {"testdb.users"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("id");
        assertThat(output).contains("name");
        assertThat(output).contains("Alice");
        assertThat(output).contains("Bob");
        assertThat(output).contains("Charlie");
    }

    @Test
    void testReadWithSelect() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            ReadCommand cmd = new ReadCommand();
            cmd.execute(ctx, new String[] {"testdb.users", "--select", "id,name"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("id");
        assertThat(output).contains("name");
        assertThat(output).contains("Alice");
        // age column should not be in output header
        String headerLine = output.split("\n")[0];
        assertThat(headerLine).doesNotContain("age");
    }

    @Test
    void testReadWithLimit() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            ReadCommand cmd = new ReadCommand();
            cmd.execute(ctx, new String[] {"testdb.users", "--limit", "1"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        String[] lines = output.trim().split("\n");
        // 1 header + 1 data row
        assertThat(lines.length).isEqualTo(2);
    }

    @Test
    void testReadJsonFormat() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            ReadCommand cmd = new ReadCommand();
            cmd.execute(ctx, new String[] {"testdb.users", "--format", "json", "--limit", "1"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString().trim();
        assertThat(output).startsWith("{");
        assertThat(output).contains("\"id\":");
        assertThat(output).contains("\"name\":");
    }
}
