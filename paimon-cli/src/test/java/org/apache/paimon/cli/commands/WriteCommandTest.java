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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WriteCommandTest {

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
                        .column("score", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "scores"), schema, true);
        catalog.close();
    }

    @Test
    void testWriteCsv() throws Exception {
        Path csvFile = tempDir.resolve("data.csv");
        Files.write(
                csvFile,
                "id,name,score\n1,Alice,95.5\n2,Bob,88.0\n3,Charlie,72.3\n"
                        .getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            WriteCommand cmd = new WriteCommand();
            cmd.execute(ctx, new String[] {csvFile.toString(), "testdb.scores"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("Successfully wrote 3 rows");

        List<InternalRow> rows = readAllRows("testdb", "scores");
        assertThat(rows).hasSize(3);
    }

    @Test
    void testWriteJson() throws Exception {
        Path jsonFile = tempDir.resolve("data.json");
        String json =
                "[{\"id\":1,\"name\":\"Alice\",\"score\":95.5},"
                        + "{\"id\":2,\"name\":\"Bob\",\"score\":88.0}]";
        Files.write(jsonFile, json.getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            WriteCommand cmd = new WriteCommand();
            cmd.execute(
                    ctx, new String[] {jsonFile.toString(), "testdb.scores", "--format", "json"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("Successfully wrote 2 rows");

        List<InternalRow> rows = readAllRows("testdb", "scores");
        assertThat(rows).hasSize(2);
    }

    @Test
    void testWriteCsvWithoutHeader() throws Exception {
        Path csvFile = tempDir.resolve("noheader.csv");
        Files.write(csvFile, "1,Alice,95.5\n2,Bob,88.0\n".getBytes(StandardCharsets.UTF_8));

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            WriteCommand cmd = new WriteCommand();
            cmd.execute(ctx, new String[] {csvFile.toString(), "testdb.scores"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("Successfully wrote 2 rows");
    }

    private List<InternalRow> readAllRows(String db, String table) throws Exception {
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table t = catalog.getTable(Identifier.create(db, table));
        ReadBuilder readBuilder = t.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        List<InternalRow> rows = new ArrayList<>();
        for (Split split : splits) {
            RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split);
            reader.forEachRemaining(rows::add);
        }
        catalog.close();
        return rows;
    }
}
