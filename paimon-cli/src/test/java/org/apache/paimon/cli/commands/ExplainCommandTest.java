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

class ExplainCommandTest {

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
                        .column("city", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "2")
                        .build();
        catalog.createTable(Identifier.create("testdb", "users"), schema, true);

        Table table = catalog.getTable(Identifier.create("testdb", "users"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(
                GenericRow.of(
                        1, BinaryString.fromString("Alice"), BinaryString.fromString("Beijing")));
        writer.write(
                GenericRow.of(
                        2, BinaryString.fromString("Bob"), BinaryString.fromString("Shanghai")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();
    }

    @Test
    void testExplainBasic() throws Exception {
        String output = execute("testdb.users");
        assertThat(output).contains("Scan Plan");
        assertThat(output).contains("Snapshot");
    }

    @Test
    void testExplainWithFilter() throws Exception {
        String output = execute("testdb.users", "--where", "id > 1");
        assertThat(output).contains("Scan Plan");
    }

    @Test
    void testExplainWithProjection() throws Exception {
        String output = execute("testdb.users", "--select", "id,name");
        assertThat(output).contains("Scan Plan");
    }

    @Test
    void testListPartitionsNonPartitioned() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new ListPartitionsCommand().execute(ctx, new String[] {"testdb.users"});
        } finally {
            System.setOut(originalOut);
        }

        assertThat(baos.toString()).contains("not partitioned");
    }

    @Test
    void testListPartitionsWithData() throws Exception {
        // Create partitioned table
        CatalogContext cctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(cctx);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("city", DataTypes.STRING())
                        .column("value", DataTypes.INT())
                        .partitionKeys("city")
                        .primaryKey("id", "city")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "partitioned"), schema, true);

        Table table = catalog.getTable(Identifier.create("testdb", "partitioned"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(GenericRow.of(1, BinaryString.fromString("Beijing"), 100));
        writer.write(GenericRow.of(2, BinaryString.fromString("Shanghai"), 200));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new ListPartitionsCommand().execute(ctx, new String[] {"testdb.partitioned"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("Beijing");
        assertThat(output).contains("Shanghai");
        assertThat(output).contains("RecordCount");
    }

    private String execute(String... args) throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new ExplainCommand().execute(ctx, args);
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
