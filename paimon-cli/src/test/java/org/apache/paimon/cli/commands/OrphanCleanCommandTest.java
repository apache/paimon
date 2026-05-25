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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OrphanCleanCommandTest {

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
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .build();
        catalog.createTable(Identifier.create("testdb", "t1"), schema, true);

        Table table = catalog.getTable(Identifier.create("testdb", "t1"));
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        writer.write(GenericRow.of(1, org.apache.paimon.data.BinaryString.fromString("a")));
        List<CommitMessage> messages = writer.prepareCommit();
        writer.close();
        writeBuilder.newCommit().commit(messages);

        catalog.close();
    }

    @Test
    void testDryRunNoOrphans() throws Exception {
        String output = execute("testdb.t1", "--dry-run");
        assertThat(output).contains("Dry run");
        assertThat(output).contains("0 orphan files");
    }

    @Test
    void testCleanNoOrphans() throws Exception {
        String output = execute("testdb.t1");
        assertThat(output).contains("Deleted 0 orphan files");
    }

    @Test
    void testCleanWithOrphanFile() throws Exception {
        // Create an orphan file in the data directory
        Path dataDir = tempDir.resolve("testdb.db").resolve("t1").resolve("bucket-0");
        Files.createDirectories(dataDir);
        Path orphan = dataDir.resolve("orphan-file-001.parquet");
        Files.write(orphan, new byte[1024]);
        // Set file modification time to 2 days ago so it's older than default 1d threshold
        Files.setLastModifiedTime(
                orphan,
                java.nio.file.attribute.FileTime.fromMillis(
                        System.currentTimeMillis() - 2 * 24 * 3600 * 1000L));

        String output = execute("testdb.t1");
        assertThat(output).contains("Deleted 1 orphan files");
    }

    @Test
    void testCleanAllTablesInDatabase() throws Exception {
        String output = execute("testdb.*");
        assertThat(output).contains("Deleted 0 orphan files");
    }

    @Test
    void testOlderThanDurationParsing() throws Exception {
        // With --older-than 0d, everything is considered old
        String output = execute("testdb.t1", "--older-than", "0d", "--dry-run");
        assertThat(output).contains("Dry run");
    }

    private String execute(String... args) throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new OrphanCleanCommand().execute(ctx, args);
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
