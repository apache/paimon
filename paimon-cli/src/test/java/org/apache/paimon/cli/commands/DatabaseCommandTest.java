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

import org.apache.paimon.cli.CommandContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseCommandTest {

    @TempDir Path tempDir;
    private Options options;

    @BeforeEach
    void setUp() {
        options = new Options();
        options.set("metastore", "filesystem");
        options.set("warehouse", tempDir.toString());
    }

    @Test
    void testCreateAndListDatabase() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new CreateDatabaseCommand().execute(ctx, new String[] {"mydb"});
            new CreateDatabaseCommand().execute(ctx, new String[] {"analytics"});

            baos.reset();
            new ListDatabasesCommand().execute(ctx, new String[] {});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("mydb");
        assertThat(output).contains("analytics");
    }

    @Test
    void testCreateDatabaseIgnoreIfExists() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new CreateDatabaseCommand().execute(ctx, new String[] {"mydb"});
            // Should not throw
            new CreateDatabaseCommand().execute(ctx, new String[] {"mydb", "--ignore-if-exists"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("created successfully");
    }

    @Test
    void testDropDatabase() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new CreateDatabaseCommand().execute(ctx, new String[] {"tempdb"});

            baos.reset();
            new DropDatabaseCommand().execute(ctx, new String[] {"tempdb"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("dropped successfully");
    }

    @Test
    void testDropDatabaseIgnoreIfNotExists() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            // Should not throw
            new DropDatabaseCommand()
                    .execute(ctx, new String[] {"nonexistent", "--ignore-if-not-exists"});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("dropped successfully");
    }

    @Test
    void testListDatabasesEmpty() throws Exception {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new ListDatabasesCommand().execute(ctx, new String[] {});
        } finally {
            System.setOut(originalOut);
        }

        String output = baos.toString();
        assertThat(output).contains("No databases found");
    }
}
