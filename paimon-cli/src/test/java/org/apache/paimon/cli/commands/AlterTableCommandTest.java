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
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AlterTableCommandTest {

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

        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "t1"), schema, true);
        catalog.close();
    }

    @Test
    void testSetOption() throws Exception {
        String output =
                execute(
                        "testdb.t1",
                        "set-option",
                        "--key",
                        "write-buffer-size",
                        "--value",
                        "128mb");
        assertThat(output).contains("altered successfully");

        // Verify option was set
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        assertThat(table.options()).containsEntry("write-buffer-size", "128mb");
        catalog.close();
    }

    @Test
    void testRemoveOption() throws Exception {
        execute("testdb.t1", "set-option", "--key", "custom-opt", "--value", "v1");
        String output = execute("testdb.t1", "remove-option", "--key", "custom-opt");
        assertThat(output).contains("altered successfully");

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        assertThat(table.options()).doesNotContainKey("custom-opt");
        catalog.close();
    }

    @Test
    void testAddColumn() throws Exception {
        String output = execute("testdb.t1", "add-column", "--name", "email", "--type", "STRING");
        assertThat(output).contains("altered successfully");

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        assertThat(table.rowType().getFieldNames()).contains("email");
        catalog.close();
    }

    @Test
    void testDropColumn() throws Exception {
        String output = execute("testdb.t1", "drop-column", "--name", "age");
        assertThat(output).contains("altered successfully");

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        assertThat(table.rowType().getFieldNames()).doesNotContain("age");
        catalog.close();
    }

    @Test
    void testRenameColumn() throws Exception {
        String output =
                execute("testdb.t1", "rename-column", "--name", "name", "--new-name", "username");
        assertThat(output).contains("altered successfully");

        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        assertThat(table.rowType().getFieldNames()).contains("username");
        assertThat(table.rowType().getFieldNames()).doesNotContain("name");
        catalog.close();
    }

    @Test
    void testRenameTable() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new RenameTableCommand().execute(ctx, new String[] {"testdb.t1", "testdb.t1_renamed"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("renamed");

        CatalogContext cctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(cctx);
        List<String> tables = catalog.listTables("testdb");
        assertThat(tables).contains("t1_renamed");
        assertThat(tables).doesNotContain("t1");
        catalog.close();
    }

    private String execute(String... args) throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new AlterTableCommand().execute(ctx, args);
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
