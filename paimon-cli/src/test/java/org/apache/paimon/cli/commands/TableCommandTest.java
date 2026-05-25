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
import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TableCommandTest {

    @TempDir Path tempDir;
    private Options options;

    @BeforeEach
    void setUp() throws Exception {
        options = new Options();
        options.set("metastore", "filesystem");
        options.set("warehouse", tempDir.toString());

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        catalog.createDatabase("testdb", true);
        catalog.close();
    }

    @Test
    void testCreateTableFromSchema() throws Exception {
        Path schemaFile = tempDir.resolve("schema.json");
        String schemaJson =
                "{\n"
                        + "  \"fields\": [\n"
                        + "    {\"name\": \"id\", \"type\": \"BIGINT\"},\n"
                        + "    {\"name\": \"name\", \"type\": \"STRING\"},\n"
                        + "    {\"name\": \"amount\", \"type\": \"DECIMAL(10,2)\"}\n"
                        + "  ],\n"
                        + "  \"primaryKeys\": [\"id\"],\n"
                        + "  \"options\": {\"bucket\": \"2\"}\n"
                        + "}";
        Files.write(schemaFile, schemaJson.getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new CreateTableCommand()
                    .execute(
                            ctx, new String[] {"testdb.orders", "--schema", schemaFile.toString()});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("created successfully");

        // Verify table exists via list-tables
        baos.reset();
        System.setOut(new PrintStream(baos));
        try (CommandContext ctx = new CommandContext(options)) {
            new ListTablesCommand().execute(ctx, new String[] {"testdb"});
        } finally {
            System.setOut(originalOut);
        }
        assertThat(baos.toString()).contains("orders");
    }

    @Test
    void testCreateTableIgnoreIfExists() throws Exception {
        Path schemaFile = tempDir.resolve("schema.json");
        String schemaJson =
                "{\"fields\": [{\"name\": \"id\", \"type\": \"INT\"}],"
                        + "\"primaryKeys\": [\"id\"], \"options\": {\"bucket\": \"1\"}}";
        Files.write(schemaFile, schemaJson.getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            CreateTableCommand cmd = new CreateTableCommand();
            cmd.execute(ctx, new String[] {"testdb.t1", "--schema", schemaFile.toString()});
            // Should not throw
            cmd.execute(
                    ctx,
                    new String[] {
                        "testdb.t1", "--schema", schemaFile.toString(), "--ignore-if-exists"
                    });
        } finally {
            System.setOut(originalOut);
        }

        assertThat(baos.toString()).contains("created successfully");
    }

    @Test
    void testDropTable() throws Exception {
        Path schemaFile = tempDir.resolve("schema.json");
        String schemaJson =
                "{\"fields\": [{\"name\": \"id\", \"type\": \"INT\"}],"
                        + "\"primaryKeys\": [\"id\"], \"options\": {\"bucket\": \"1\"}}";
        Files.write(schemaFile, schemaJson.getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new CreateTableCommand()
                    .execute(
                            ctx,
                            new String[] {"testdb.to_drop", "--schema", schemaFile.toString()});

            baos.reset();
            new DropTableCommand().execute(ctx, new String[] {"testdb.to_drop"});
        } finally {
            System.setOut(originalOut);
        }

        assertThat(baos.toString()).contains("dropped successfully");
    }

    @Test
    void testDropTableIgnoreIfNotExists() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new DropTableCommand()
                    .execute(ctx, new String[] {"testdb.nonexistent", "--ignore-if-not-exists"});
        } finally {
            System.setOut(originalOut);
        }

        assertThat(baos.toString()).contains("dropped successfully");
    }

    @Test
    void testListTablesEmpty() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new ListTablesCommand().execute(ctx, new String[] {"testdb"});
        } finally {
            System.setOut(originalOut);
        }

        assertThat(baos.toString()).contains("No tables found");
    }
}
