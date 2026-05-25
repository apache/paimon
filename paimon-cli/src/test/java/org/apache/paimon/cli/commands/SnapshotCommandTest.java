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
import org.apache.paimon.cli.Command;
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

class SnapshotCommandTest {

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
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "t1"), schema, true);

        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(GenericRow.of(1, BinaryString.fromString("Alice")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();
    }

    @Test
    void testSnapshotShowsJson() throws Exception {
        String output = execute(new SnapshotCommand(), "testdb.t1");
        assertThat(output).contains("\"id\"");
        assertThat(output).contains("\"schemaId\"");
    }

    @Test
    void testSnapshotNoData() throws Exception {
        // Create empty table
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "empty"), schema, true);
        catalog.close();

        String output = execute(new SnapshotCommand(), "testdb.empty");
        assertThat(output).contains("No snapshot found");
    }

    @Test
    void testTagCreateAndList() throws Exception {
        execute(new TagCommand(), "create", "testdb.t1", "--tag-name", "v1");
        String output = execute(new TagCommand(), "list", "testdb.t1");
        assertThat(output).contains("v1");
    }

    @Test
    void testTagDelete() throws Exception {
        execute(new TagCommand(), "create", "testdb.t1", "--tag-name", "v2");
        String output = execute(new TagCommand(), "delete", "testdb.t1", "--tag-name", "v2");
        assertThat(output).contains("deleted");

        output = execute(new TagCommand(), "list", "testdb.t1");
        assertThat(output).contains("No tags found");
    }

    @Test
    void testExpireSnapshots() throws Exception {
        // Write a second batch to create more snapshots
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(GenericRow.of(2, BinaryString.fromString("Bob")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();

        String output = execute(new ExpireSnapshotsCommand(), "testdb.t1", "--retain-max", "1");
        assertThat(output).contains("Expired");
        assertThat(output).contains("snapshots");
    }

    @Test
    void testRollbackToTag() throws Exception {
        execute(new TagCommand(), "create", "testdb.t1", "--tag-name", "rollback-point");

        // Write more data
        CatalogContext ctx = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(ctx);
        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(GenericRow.of(2, BinaryString.fromString("Bob")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);
        catalog.close();

        String output = execute(new RollbackCommand(), "testdb.t1", "--tag", "rollback-point");
        assertThat(output).contains("rolled back to tag 'rollback-point'");
    }

    private String execute(Command cmd, String... args) throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            cmd.execute(ctx, args);
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
