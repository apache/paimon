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
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BranchCommandTest {

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
        catalog.close();
    }

    @Test
    void testListBranchesEmpty() throws Exception {
        String output = execute("list", "testdb.t1");
        assertThat(output).contains("No branches found");
    }

    @Test
    void testCreateAndListBranch() throws Exception {
        String output = execute("create", "testdb.t1", "--name", "dev");
        assertThat(output).contains("Branch 'dev' created");

        output = execute("list", "testdb.t1");
        assertThat(output.trim()).isEqualTo("dev");
    }

    @Test
    void testRenameBranch() throws Exception {
        execute("create", "testdb.t1", "--name", "old-name");
        execute("rename", "testdb.t1", "--name", "old-name", "--new-name", "new-name");

        String output = execute("list", "testdb.t1");
        assertThat(output.trim()).isEqualTo("new-name");
    }

    @Test
    void testDeleteBranch() throws Exception {
        execute("create", "testdb.t1", "--name", "to-delete");
        String output = execute("delete", "testdb.t1", "--name", "to-delete");
        assertThat(output).contains("deleted");

        output = execute("list", "testdb.t1");
        assertThat(output).contains("No branches found");
    }

    @Test
    void testMultipleBranches() throws Exception {
        execute("create", "testdb.t1", "--name", "branch-a");
        execute("create", "testdb.t1", "--name", "branch-b");

        String output = execute("list", "testdb.t1");
        assertThat(output).contains("branch-a");
        assertThat(output).contains("branch-b");
    }

    private String execute(String... args) throws Exception {
        String[] fullArgs = new String[args.length];
        System.arraycopy(args, 0, fullArgs, 0, args.length);

        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));

        try (CommandContext ctx = new CommandContext(options)) {
            new BranchCommand().execute(ctx, fullArgs);
        } finally {
            System.setOut(originalOut);
        }
        return baos.toString();
    }
}
